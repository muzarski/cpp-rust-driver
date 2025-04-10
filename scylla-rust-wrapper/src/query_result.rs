use crate::argconv::*;
use crate::cass_error::{CassError, ToCassError};
use crate::cass_types::{
    cass_data_type_type, get_column_type, CassColumnSpec, CassDataType, CassDataTypeInner,
    CassValueType, MapDataType,
};
use crate::execution_error::CassErrorResult;
use crate::inet::CassInet;
use crate::types::*;
use crate::uuid::CassUuid;
use cass_raw_value::CassRawValue;
use row_with_self_borrowed_result_data::RowWithSelfBorrowedResultData;
use scylla::cluster::metadata::{ColumnType, NativeType};
use scylla::deserialize::row::{
    BuiltinDeserializationError, BuiltinDeserializationErrorKind, ColumnIterator, DeserializeRow,
};
use scylla::deserialize::value::DeserializeValue;
use scylla::errors::{DeserializationError, IntoRowsResultError, TypeCheckError};
use scylla::frame::response::result::{ColumnSpec, DeserializedMetadataAndRawRows};
use scylla::response::query_result::{ColumnSpecs, QueryResult};
use scylla::response::PagingStateResponse;
use scylla::value::{
    Counter, CqlDate, CqlDecimalBorrowed, CqlDuration, CqlTime, CqlTimestamp, CqlTimeuuid,
};
use std::convert::TryInto;
use std::net::IpAddr;
use std::os::raw::c_char;
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

pub enum CassResultKind {
    NonRows,
    Rows(CassRowsResult),
}

pub struct CassRowsResult {
    // Arc: shared with first_row (yoke).
    pub(crate) shared_data: Arc<CassRowsResultSharedData>,
    pub(crate) first_row: Option<RowWithSelfBorrowedResultData>,
}

pub(crate) struct CassRowsResultSharedData {
    pub(crate) raw_rows: DeserializedMetadataAndRawRows,
    // Arc: shared with CassPrepared
    pub(crate) metadata: Arc<CassResultMetadata>,
}

pub struct CassResult {
    pub tracing_id: Option<Uuid>,
    pub paging_state_response: PagingStateResponse,
    pub kind: CassResultKind,
}

impl CassResult {
    /// It creates CassResult object based on the:
    /// - query result
    /// - paging state response
    /// - optional cached result metadata - it's provided for prepared statements
    pub fn from_result_payload(
        result: QueryResult,
        paging_state_response: PagingStateResponse,
        maybe_result_metadata: Option<Arc<CassResultMetadata>>,
    ) -> Result<Self, CassErrorResult> {
        match result.into_rows_result() {
            Ok(rows_result) => {
                // maybe_result_metadata is:
                // - Some(_) for prepared statements
                // - None for unprepared statements
                let metadata = maybe_result_metadata.unwrap_or_else(|| {
                    Arc::new(CassResultMetadata::from_column_specs(
                        rows_result.column_specs(),
                    ))
                });

                let (raw_rows, tracing_id, _) = rows_result.into_inner();
                let shared_data = Arc::new(CassRowsResultSharedData { raw_rows, metadata });
                let first_row = RowWithSelfBorrowedResultData::first_from_raw_rows_and_metadata(
                    Arc::clone(&shared_data),
                )?;

                let cass_result = CassResult {
                    tracing_id,
                    paging_state_response,
                    kind: CassResultKind::Rows(CassRowsResult {
                        shared_data,
                        first_row,
                    }),
                };

                Ok(cass_result)
            }
            Err(IntoRowsResultError::ResultNotRows(result)) => {
                let cass_result = CassResult {
                    tracing_id: result.tracing_id(),
                    paging_state_response,
                    kind: CassResultKind::NonRows,
                };

                Ok(cass_result)
            }
            Err(IntoRowsResultError::ResultMetadataLazyDeserializationError(err)) => {
                Err(err.into())
            }
        }
    }
}

impl FFI for CassResult {
    type Origin = FromArc;
}

#[derive(Debug)]
pub struct CassResultMetadata {
    pub col_specs: Vec<CassColumnSpec>,
}

impl CassResultMetadata {
    pub fn from_column_specs(col_specs: ColumnSpecs<'_, '_>) -> CassResultMetadata {
        let col_specs = col_specs
            .iter()
            .map(|col_spec| {
                let name = col_spec.name().to_owned();
                let data_type = Arc::new(get_column_type(col_spec.typ()));

                CassColumnSpec { name, data_type }
            })
            .collect();

        CassResultMetadata { col_specs }
    }
}

pub(crate) struct CassRawRow<'frame, 'metadata> {
    pub(crate) columns: Vec<CassRawValue<'frame, 'metadata>>,
}

impl<'frame, 'metadata> DeserializeRow<'frame, 'metadata> for CassRawRow<'frame, 'metadata> {
    fn type_check(_specs: &[ColumnSpec]) -> Result<(), TypeCheckError> {
        Ok(())
    }

    fn deserialize(
        mut row: ColumnIterator<'frame, 'metadata>,
    ) -> Result<Self, DeserializationError> {
        let mut columns = Vec::with_capacity(row.size_hint().0);
        while let Some(column) = row.next().transpose()? {
            columns.push(
                <CassRawValue as DeserializeValue>::deserialize(column.spec.typ(), column.slice)
                    .map_err(|err| {
                        DeserializationError::new(BuiltinDeserializationError {
                            rust_name: std::any::type_name::<CassRawValue>(),
                            kind: BuiltinDeserializationErrorKind::ColumnDeserializationFailed {
                                column_index: column.index,
                                column_name: column.spec.name().to_owned(),
                                err,
                            },
                        })
                    })?,
            );
        }
        Ok(Self { columns })
    }
}

/// The lifetime of CassRow is bound to CassResult.
/// It will be freed, when CassResult is freed.(see #[cass_result_free])
pub struct CassRow<'result> {
    pub columns: Vec<CassValue<'result>>,
    pub result_metadata: &'result CassResultMetadata,
}

impl FFI for CassRow<'_> {
    type Origin = FromRef;
}

impl<'result> CassRow<'result> {
    pub(crate) fn from_row_and_metadata(
        row: Vec<CassRawValue<'result, 'result>>,
        result_metadata: &'result CassResultMetadata,
    ) -> Self {
        Self {
            columns: create_cass_row_columns(row, result_metadata),
            result_metadata,
        }
    }
}

/// Module defining [`RowWithSelfBorrowedResultData`] struct.
/// The purpose of this module is so the `query_result` module does not directly depend on `yoke`.
mod row_with_self_borrowed_result_data {
    use std::sync::Arc;

    use yoke::{Yoke, Yokeable};

    use crate::execution_error::CassErrorResult;
    use crate::query_result::CassRawRow;

    use super::{CassRow, CassRowsResultSharedData};

    /// A simple wrapper over CassRow.
    /// Needed, so we can implement Yokeable for it, instead of implementing it for CassRow.
    #[derive(Yokeable)]
    struct CassRowWrapper<'result>(CassRow<'result>);

    /// A wrapper over struct which self-borrows the metadata allocated using Arc.
    ///
    /// It's needed to safely express the relationship between [`CassRowsResult`][super::CassRowsResult]
    /// and its `first_row` field. The relationship is as follows:
    /// 1. `CassRowsResult` owns `shared_data` field, which is an `Arc<CassRowsResultSharedData>`.
    /// 2. `CassRowsResult` owns the row (`first_row`)
    /// 3. `CassRow` borrows from `shared_data` (serialized values bytes and metadata).
    ///
    /// This struct is a shared owner of the row bytes and metadata, and self-borrows this data
    /// to the `CassRow` it contains.
    pub struct RowWithSelfBorrowedResultData(
        Yoke<CassRowWrapper<'static>, Arc<CassRowsResultSharedData>>,
    );

    impl RowWithSelfBorrowedResultData {
        /// Constructs [`RowWithSelfBorrowedResultData`] based on the first row from `raw_rows_and_metadata`.
        pub(super) fn first_from_raw_rows_and_metadata(
            raw_rows_and_metadata: Arc<CassRowsResultSharedData>,
        ) -> Result<Option<Self>, CassErrorResult> {
            enum AttachError {
                CassErrorResult(CassErrorResult),
                NoRows,
            }
            impl From<CassErrorResult> for AttachError {
                fn from(err: CassErrorResult) -> Self {
                    AttachError::CassErrorResult(err)
                }
            }

            let yoke_result = Yoke::try_attach_to_cart(
                raw_rows_and_metadata,
                |raw_rows_and_metadata_ref| -> Result<_, AttachError> {
                    let CassRowsResultSharedData { raw_rows, metadata } = raw_rows_and_metadata_ref;

                    let row_result = match raw_rows
                        .rows_iter::<CassRawRow>()
                        // unwrap: CassRawRow always passes the typecheck.
                        .unwrap()
                        .next()
                    {
                        Some(Ok(row)) => Ok(row),
                        Some(Err(deser_error)) => {
                            Err(AttachError::CassErrorResult(deser_error.into()))
                        }
                        None => Err(AttachError::NoRows),
                    };
                    let row = row_result?;

                    Ok(CassRowWrapper(CassRow::from_row_and_metadata(
                        row.columns,
                        metadata,
                    )))
                },
            );

            match yoke_result {
                Ok(yoke) => Ok(Some(Self(yoke))),
                Err(AttachError::NoRows) => Ok(None),
                Err(AttachError::CassErrorResult(err)) => Err(err),
            }
        }

        pub(super) fn row(&self) -> &CassRow<'_> {
            &self.0.get().0
        }
    }
}

/// A separate module so there is no way to construct CassRawValue other than using `DeserializeValue` implementation.
/// This is because `CassRawValue` maps the "empty" values to null in this implementation.
pub(crate) mod cass_raw_value {
    use scylla::cluster::metadata::{ColumnType, NativeType};
    use scylla::deserialize::value::DeserializeValue;
    use scylla::deserialize::FrameSlice;
    use scylla::errors::{DeserializationError, TypeCheckError};
    use thiserror::Error;

    pub(crate) struct CassRawValue<'frame, 'metadata> {
        typ: &'metadata ColumnType<'metadata>,
        slice: Option<FrameSlice<'frame>>,
        /// Required to implement [`super::cass_value_item_count`].
        item_count: Option<usize>,
    }

    #[derive(Error, Debug)]
    pub(crate) enum CollectionLengthDeserializationError {
        #[error("Provided slice is too short. Expected at least 4 bytes, got {0}.")]
        TooFewBytes(usize),
        #[error("Deserialized length is negative: {0}.")]
        NegativeLength(i32),
    }

    /// Precomputes the `item_count` from raw value.
    /// It is required to implement [`cass_value_item_count`].
    fn determine_value_item_count(
        typ: &ColumnType<'_>,
        slice: Option<FrameSlice<'_>>,
    ) -> Result<Option<usize>, CollectionLengthDeserializationError> {
        // Pre-compute the item count. Required to implement cass_value_item_count.
        // The cpp-driver semantics:
        // - tuples/UDTs - obtain the value count from type metadata
        // - collections - deserialize the first 4 bytes as collection length
        // - other types - always return 0 when `cass_value_item_count` is called
        let item_count: Option<usize> = slice
            .map(|frame_slice| match typ {
                ColumnType::Collection { .. } => {
                    let slice = frame_slice.as_slice();
                    let length_arr: [u8; 4] = slice
                        .get(0..4)
                        .ok_or(CollectionLengthDeserializationError::TooFewBytes(
                            slice.len(),
                        ))?
                        .try_into()
                        // unwrap: Conversion from slice of length 4 to array of length 4 is safe.
                        .unwrap();

                    let i32_length = i32::from_be_bytes(length_arr);
                    let length = i32_length.try_into().map_err(|_| {
                        CollectionLengthDeserializationError::NegativeLength(i32_length)
                    })?;

                    Ok(Some(length))
                }
                ColumnType::Tuple(types) => Ok(Some(types.len())),
                ColumnType::UserDefinedType { definition, .. } => {
                    Ok(Some(definition.field_types.len()))
                }
                _ => Ok(None),
            })
            .transpose()?
            .flatten();

        Ok(item_count)
    }

    impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for CassRawValue<'frame, 'metadata> {
        fn type_check(_typ: &ColumnType) -> Result<(), TypeCheckError> {
            Ok(())
        }

        fn deserialize(
            typ: &'metadata ColumnType<'metadata>,
            v: Option<FrameSlice<'frame>>,
        ) -> Result<Self, DeserializationError> {
            // Handle the "empty" values. cpp-driver fallbacks to null if:
            // 1. the value is "empty", AND
            // 2. the type is non-string/non-byte and emptiable.
            // TODO: Extend the C API and support "empty" values.
            let slice = match (typ, v) {
                // For non-emptiable/string/byte types just return the slice.
                (
                    // These types do not support "empty values".
                    // See https://github.com/scylladb/scylla-rust-driver/blob/v1.1.0/scylla-cql/src/frame/response/result.rs#L220-L240.
                    ColumnType::Collection { .. }
                    | ColumnType::UserDefinedType { .. }
                    | ColumnType::Native(NativeType::Duration)
                    | ColumnType::Native(NativeType::Counter)

                    // string/byte types
                    | ColumnType::Native(NativeType::Ascii)
                    | ColumnType::Native(NativeType::Text)
                    | ColumnType::Native(NativeType::Blob),
                    slice,
                ) => slice,

                // For the types that support "empty" values, fallback to null.
                (_, Some(slice)) if slice.is_empty() => None,

                // Value is not "empty", just return the slice.
                (_, slice) => slice,
            };

            let item_count =
                determine_value_item_count(typ, slice).map_err(DeserializationError::new)?;

            Ok(Self {
                typ,
                slice,
                item_count,
            })
        }
    }

    impl<'frame, 'metadata> CassRawValue<'frame, 'metadata> {
        pub(crate) fn typ(&self) -> &'metadata ColumnType<'metadata> {
            self.typ
        }

        pub(crate) fn slice(&self) -> Option<FrameSlice<'frame>> {
            self.slice
        }

        pub(crate) fn item_count(&self) -> Option<usize> {
            self.item_count
        }
    }
}

pub struct CassValue<'result> {
    pub(crate) value: CassRawValue<'result, 'result>,
    pub(crate) value_type: &'result Arc<CassDataType>,
}

impl FFI for CassValue<'_> {
    type Origin = FromRef;
}

impl<'result> CassValue<'result> {
    pub fn get_non_null<T>(&'result self) -> Result<T, NonNullDeserializationError>
    where
        T: DeserializeValue<'result, 'result>,
    {
        let (typ, frame_slice) = (self.value.typ(), self.value.slice());
        if frame_slice.is_none() {
            return Err(NonNullDeserializationError::IsNull);
        }

        T::type_check(typ)?;
        let v = T::deserialize(typ, frame_slice)?;
        Ok(v)
    }

    pub fn get_bytes_non_null(&self) -> Result<&'result [u8], NonNullDeserializationError> {
        let Some(slice) = self.value.slice() else {
            return Err(NonNullDeserializationError::IsNull);
        };

        Ok(slice.as_slice())
    }
}

#[derive(Debug, Error)]
pub enum NonNullDeserializationError {
    #[error("Value is null")]
    IsNull,
    #[error("Typecheck failed: {0}")]
    Typecheck(#[from] TypeCheckError),
    #[error("Deserialization failed: {0}")]
    Deserialization(#[from] DeserializationError),
}

impl ToCassError for NonNullDeserializationError {
    fn to_cass_error(&self) -> CassError {
        match self {
            NonNullDeserializationError::IsNull => CassError::CASS_ERROR_LIB_NULL_VALUE,
            NonNullDeserializationError::Typecheck(_) => {
                CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE
            }
            NonNullDeserializationError::Deserialization(_) => {
                CassError::CASS_ERROR_LIB_INVALID_DATA
            }
        }
    }
}

fn create_cass_row_columns<'result>(
    row: Vec<CassRawValue<'result, 'result>>,
    metadata: &'result CassResultMetadata,
) -> Vec<CassValue<'result>> {
    row.into_iter()
        .zip(metadata.col_specs.iter())
        .map(|(value, col_spec)| {
            let value_type = &col_spec.data_type;
            CassValue { value, value_type }
        })
        .collect()
}

#[no_mangle]
pub unsafe extern "C" fn cass_result_free(result_raw: CassOwnedSharedPtr<CassResult, CConst>) {
    ArcFFI::free(result_raw);
}

#[no_mangle]
pub unsafe extern "C" fn cass_result_has_more_pages(
    result: CassBorrowedSharedPtr<CassResult, CConst>,
) -> cass_bool_t {
    unsafe { result_has_more_pages(&result) }
}

unsafe fn result_has_more_pages(result: &CassBorrowedSharedPtr<CassResult, CConst>) -> cass_bool_t {
    let result = ArcFFI::as_ref(result.borrow()).unwrap();
    (!result.paging_state_response.finished()) as cass_bool_t
}

#[no_mangle]
pub unsafe extern "C" fn cass_row_get_column<'result>(
    row_raw: CassBorrowedSharedPtr<'result, CassRow<'result>, CConst>,
    index: size_t,
) -> CassBorrowedSharedPtr<'result, CassValue<'result>, CConst> {
    let row: &CassRow = RefFFI::as_ref(row_raw).unwrap();

    let index_usize: usize = index.try_into().unwrap();
    let column_value = match row.columns.get(index_usize) {
        Some(val) => val,
        None => return RefFFI::null(),
    };

    RefFFI::as_ptr(column_value)
}

#[no_mangle]
pub unsafe extern "C" fn cass_row_get_column_by_name<'result>(
    row: CassBorrowedSharedPtr<'result, CassRow<'result>, CConst>,
    name: *const c_char,
) -> CassBorrowedSharedPtr<'result, CassValue<'result>, CConst> {
    let name_str = unsafe { ptr_to_cstr(name) }.unwrap();
    let name_length = name_str.len();

    unsafe { cass_row_get_column_by_name_n(row, name, name_length as size_t) }
}

#[no_mangle]
pub unsafe extern "C" fn cass_row_get_column_by_name_n<'result>(
    row: CassBorrowedSharedPtr<'result, CassRow<'result>, CConst>,
    name: *const c_char,
    name_length: size_t,
) -> CassBorrowedSharedPtr<'result, CassValue<'result>, CConst> {
    let row_from_raw = RefFFI::as_ref(row).unwrap();
    let mut name_str = unsafe { ptr_to_cstr_n(name, name_length).unwrap() };
    let mut is_case_sensitive = false;

    if name_str.starts_with('\"') && name_str.ends_with('\"') {
        name_str = name_str.strip_prefix('\"').unwrap();
        name_str = name_str.strip_suffix('\"').unwrap();
        is_case_sensitive = true;
    }

    row_from_raw
        .result_metadata
        .col_specs
        .iter()
        .enumerate()
        .find(|(_, col_spec)| {
            is_case_sensitive && col_spec.name == name_str
                || !is_case_sensitive && col_spec.name.eq_ignore_ascii_case(name_str)
        })
        .map(|(index, _)| match row_from_raw.columns.get(index) {
            Some(value) => RefFFI::as_ptr(value),
            None => RefFFI::null(),
        })
        .unwrap_or(RefFFI::null())
}

#[no_mangle]
pub unsafe extern "C" fn cass_result_column_name(
    result: CassBorrowedSharedPtr<CassResult, CConst>,
    index: size_t,
    name: *mut *const c_char,
    name_length: *mut size_t,
) -> CassError {
    let result_from_raw = ArcFFI::as_ref(result).unwrap();
    let index_usize: usize = index.try_into().unwrap();

    let CassResultKind::Rows(CassRowsResult { shared_data, .. }) = &result_from_raw.kind else {
        return CassError::CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS;
    };

    if index_usize >= shared_data.metadata.col_specs.len() {
        return CassError::CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS;
    }

    let column_name = &shared_data
        .metadata
        .col_specs
        .get(index_usize)
        .unwrap()
        .name;

    unsafe { write_str_to_c(column_name, name, name_length) };

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_result_column_type(
    result: CassBorrowedSharedPtr<CassResult, CConst>,
    index: size_t,
) -> CassValueType {
    let data_type_ptr = unsafe { cass_result_column_data_type(result, index) };
    if ArcFFI::is_null(&data_type_ptr) {
        return CassValueType::CASS_VALUE_TYPE_UNKNOWN;
    }
    unsafe { cass_data_type_type(data_type_ptr) }
}

#[no_mangle]
pub unsafe extern "C" fn cass_result_column_data_type(
    result: CassBorrowedSharedPtr<CassResult, CConst>,
    index: size_t,
) -> CassBorrowedSharedPtr<CassDataType, CConst> {
    let result_from_raw: &CassResult = ArcFFI::as_ref(result).unwrap();
    let index_usize: usize = index
        .try_into()
        .expect("Provided index is out of bounds. Max possible value is usize::MAX");

    let CassResultKind::Rows(CassRowsResult { shared_data, .. }) = &result_from_raw.kind else {
        return ArcFFI::null();
    };

    shared_data
        .metadata
        .col_specs
        .get(index_usize)
        .map(|col_spec| ArcFFI::as_ptr(&col_spec.data_type))
        .unwrap_or(ArcFFI::null())
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_type(
    value: CassBorrowedSharedPtr<CassValue, CConst>,
) -> CassValueType {
    let value_from_raw = RefFFI::as_ref(value).unwrap();
    unsafe { cass_data_type_type(ArcFFI::as_ptr(value_from_raw.value_type)) }
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_data_type<'result>(
    value: CassBorrowedSharedPtr<'result, CassValue<'result>, CConst>,
) -> CassBorrowedSharedPtr<'result, CassDataType, CConst> {
    let value_from_raw = RefFFI::as_ref(value).unwrap();

    ArcFFI::as_ptr(value_from_raw.value_type)
}

macro_rules! val_ptr_to_ref_ensure_non_null {
    ($ptr:ident) => {{
        let maybe_ref = RefFFI::as_ref($ptr);
        match maybe_ref {
            Some(r) => r,
            None => return CassError::CASS_ERROR_LIB_NULL_VALUE,
        }
    }};
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_get_float(
    value: CassBorrowedSharedPtr<CassValue, CConst>,
    output: *mut cass_float_t,
) -> CassError {
    let val: &CassValue = val_ptr_to_ref_ensure_non_null!(value);

    let f: f32 = match val.get_non_null() {
        Ok(v) => v,
        Err(e) => return e.to_cass_error(),
    };
    unsafe { std::ptr::write(output, f) };

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_get_double(
    value: CassBorrowedSharedPtr<CassValue, CConst>,
    output: *mut cass_double_t,
) -> CassError {
    let val: &CassValue = val_ptr_to_ref_ensure_non_null!(value);

    let f: f64 = match val.get_non_null() {
        Ok(v) => v,
        Err(e) => return e.to_cass_error(),
    };
    unsafe { std::ptr::write(output, f) };

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_get_bool(
    value: CassBorrowedSharedPtr<CassValue, CConst>,
    output: *mut cass_bool_t,
) -> CassError {
    let val: &CassValue = val_ptr_to_ref_ensure_non_null!(value);

    let b: bool = match val.get_non_null() {
        Ok(v) => v,
        Err(e) => return e.to_cass_error(),
    };
    unsafe { std::ptr::write(output, b as cass_bool_t) };

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_get_int8(
    value: CassBorrowedSharedPtr<CassValue, CConst>,
    output: *mut cass_int8_t,
) -> CassError {
    let val: &CassValue = val_ptr_to_ref_ensure_non_null!(value);

    let i: i8 = match val.get_non_null() {
        Ok(v) => v,
        Err(e) => return e.to_cass_error(),
    };
    unsafe { std::ptr::write(output, i) };

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_get_int16(
    value: CassBorrowedSharedPtr<CassValue, CConst>,
    output: *mut cass_int16_t,
) -> CassError {
    let val: &CassValue = val_ptr_to_ref_ensure_non_null!(value);

    let i: i16 = match val.get_non_null() {
        Ok(v) => v,
        Err(e) => return e.to_cass_error(),
    };
    unsafe { std::ptr::write(output, i) };

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_get_uint32(
    value: CassBorrowedSharedPtr<CassValue, CConst>,
    output: *mut cass_uint32_t,
) -> CassError {
    let val: &CassValue = val_ptr_to_ref_ensure_non_null!(value);

    let date: CqlDate = match val.get_non_null() {
        Ok(v) => v,
        Err(e) => return e.to_cass_error(),
    };
    unsafe { std::ptr::write(output, date.0) };

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_get_int32(
    value: CassBorrowedSharedPtr<CassValue, CConst>,
    output: *mut cass_int32_t,
) -> CassError {
    let val: &CassValue = val_ptr_to_ref_ensure_non_null!(value);

    let i: i32 = match val.get_non_null() {
        Ok(v) => v,
        Err(e) => return e.to_cass_error(),
    };
    unsafe { std::ptr::write(output, i) };

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_get_int64(
    value: CassBorrowedSharedPtr<CassValue, CConst>,
    output: *mut cass_int64_t,
) -> CassError {
    let val: &CassValue = val_ptr_to_ref_ensure_non_null!(value);

    let i: i64 = match val.value.typ() {
        ColumnType::Native(NativeType::BigInt) => match val.get_non_null::<i64>() {
            Ok(v) => v,
            Err(NonNullDeserializationError::Typecheck(_)) => {
                panic!("The typecheck unexpectedly failed!")
            }
            Err(e) => return e.to_cass_error(),
        },
        ColumnType::Native(NativeType::Counter) => match val.get_non_null::<Counter>() {
            Ok(v) => v.0,
            Err(NonNullDeserializationError::Typecheck(_)) => {
                panic!("The typecheck unexpectedly failed!")
            }
            Err(e) => return e.to_cass_error(),
        },
        ColumnType::Native(NativeType::Time) => match val.get_non_null::<CqlTime>() {
            Ok(v) => v.0,
            Err(NonNullDeserializationError::Typecheck(_)) => {
                panic!("The typecheck unexpectedly failed!")
            }
            Err(e) => return e.to_cass_error(),
        },
        ColumnType::Native(NativeType::Timestamp) => match val.get_non_null::<CqlTimestamp>() {
            Ok(v) => v.0,
            Err(NonNullDeserializationError::Typecheck(_)) => {
                panic!("The typecheck unexpectedly failed!")
            }
            Err(e) => return e.to_cass_error(),
        },
        _ => return CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
    };

    unsafe { std::ptr::write(output, i) };

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_get_uuid(
    value: CassBorrowedSharedPtr<CassValue, CConst>,
    output: *mut CassUuid,
) -> CassError {
    let val: &CassValue = val_ptr_to_ref_ensure_non_null!(value);

    let uuid: Uuid = match val.value.typ() {
        ColumnType::Native(NativeType::Uuid) => match val.get_non_null::<Uuid>() {
            Ok(v) => v,
            Err(NonNullDeserializationError::Typecheck(_)) => {
                panic!("The typecheck unexpectedly failed!")
            }
            Err(e) => return e.to_cass_error(),
        },
        ColumnType::Native(NativeType::Timeuuid) => match val.get_non_null::<CqlTimeuuid>() {
            Ok(v) => v.into(),
            Err(NonNullDeserializationError::Typecheck(_)) => {
                panic!("The typecheck unexpectedly failed!")
            }
            Err(e) => return e.to_cass_error(),
        },
        _ => return CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
    };

    unsafe { std::ptr::write(output, uuid.into()) };

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_get_inet(
    value: CassBorrowedSharedPtr<CassValue, CConst>,
    output: *mut CassInet,
) -> CassError {
    let val: &CassValue = val_ptr_to_ref_ensure_non_null!(value);

    let inet: IpAddr = match val.get_non_null() {
        Ok(v) => v,
        Err(e) => return e.to_cass_error(),
    };
    unsafe { std::ptr::write(output, inet.into()) };

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_get_decimal(
    value: CassBorrowedSharedPtr<CassValue, CConst>,
    varint: *mut *const cass_byte_t,
    varint_size: *mut size_t,
    scale: *mut cass_int32_t,
) -> CassError {
    let val: &CassValue = val_ptr_to_ref_ensure_non_null!(value);

    let decimal: CqlDecimalBorrowed = match val.get_non_null() {
        Ok(v) => v,
        Err(e) => return e.to_cass_error(),
    };

    let (varint_value, scale_value) = decimal.as_signed_be_bytes_slice_and_exponent();
    unsafe {
        std::ptr::write(varint_size, varint_value.len() as size_t);
        std::ptr::write(varint, varint_value.as_ptr());
        std::ptr::write(scale, scale_value);
    }

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_get_string(
    value: CassBorrowedSharedPtr<CassValue, CConst>,
    output: *mut *const c_char,
    output_size: *mut size_t,
) -> CassError {
    let val: &CassValue = val_ptr_to_ref_ensure_non_null!(value);

    // It seems that cpp driver doesn't check the type - you can call _get_string
    // on any type and get internal represenation. I don't see how to do it easily in
    // a compatible way in rust, so let's do something sensible - only return result
    // for string values.
    let s = match val.value.typ() {
        ColumnType::Native(NativeType::Ascii) | ColumnType::Native(NativeType::Text) => {
            match val.get_non_null::<&str>() {
                Ok(v) => v,
                Err(NonNullDeserializationError::Typecheck(_)) => {
                    panic!("The typecheck unexpectedly failed!")
                }
                Err(e) => return e.to_cass_error(),
            }
        }
        _ => return CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
    };

    unsafe { write_str_to_c(s, output, output_size) };

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_get_duration(
    value: CassBorrowedSharedPtr<CassValue, CConst>,
    months: *mut cass_int32_t,
    days: *mut cass_int32_t,
    nanos: *mut cass_int64_t,
) -> CassError {
    let val: &CassValue = val_ptr_to_ref_ensure_non_null!(value);

    let duration: CqlDuration = match val.get_non_null() {
        Ok(v) => v,
        Err(e) => return e.to_cass_error(),
    };

    unsafe {
        std::ptr::write(months, duration.months);
        std::ptr::write(days, duration.days);
        std::ptr::write(nanos, duration.nanoseconds);
    }

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_get_bytes(
    value: CassBorrowedSharedPtr<CassValue, CConst>,
    output: *mut *const cass_byte_t,
    output_size: *mut size_t,
) -> CassError {
    let value_from_raw: &CassValue = val_ptr_to_ref_ensure_non_null!(value);

    let bytes = match value_from_raw.get_bytes_non_null() {
        Ok(s) => s,
        Err(e) => return e.to_cass_error(),
    };

    unsafe {
        std::ptr::write(output, bytes.as_ptr());
        std::ptr::write(output_size, bytes.len() as size_t);
    }

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_is_null(
    value: CassBorrowedSharedPtr<CassValue, CConst>,
) -> cass_bool_t {
    let val: &CassValue = RefFFI::as_ref(value).unwrap();
    val.value.slice().is_none() as cass_bool_t
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_is_collection(
    value: CassBorrowedSharedPtr<CassValue, CConst>,
) -> cass_bool_t {
    let val = RefFFI::as_ref(value).unwrap();

    matches!(
        unsafe { val.value_type.get_unchecked() }.get_value_type(),
        CassValueType::CASS_VALUE_TYPE_LIST
            | CassValueType::CASS_VALUE_TYPE_SET
            | CassValueType::CASS_VALUE_TYPE_MAP
    ) as cass_bool_t
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_is_duration(
    value: CassBorrowedSharedPtr<CassValue, CConst>,
) -> cass_bool_t {
    let val = RefFFI::as_ref(value).unwrap();

    (unsafe { val.value_type.get_unchecked() }.get_value_type()
        == CassValueType::CASS_VALUE_TYPE_DURATION) as cass_bool_t
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_item_count(
    collection: CassBorrowedSharedPtr<CassValue, CConst>,
) -> size_t {
    let val = RefFFI::as_ref(collection).unwrap();

    val.value.item_count().unwrap_or(0) as size_t
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_primary_sub_type(
    collection: CassBorrowedSharedPtr<CassValue, CConst>,
) -> CassValueType {
    let val = RefFFI::as_ref(collection).unwrap();

    match unsafe { val.value_type.get_unchecked() } {
        CassDataTypeInner::List {
            typ: Some(list), ..
        } => unsafe { list.get_unchecked() }.get_value_type(),
        CassDataTypeInner::Set { typ: Some(set), .. } => {
            unsafe { set.get_unchecked() }.get_value_type()
        }
        CassDataTypeInner::Map {
            typ: MapDataType::Key(key) | MapDataType::KeyAndValue(key, _),
            ..
        } => unsafe { key.get_unchecked() }.get_value_type(),
        _ => CassValueType::CASS_VALUE_TYPE_UNKNOWN,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_secondary_sub_type(
    collection: CassBorrowedSharedPtr<CassValue, CConst>,
) -> CassValueType {
    let val = RefFFI::as_ref(collection).unwrap();

    match unsafe { val.value_type.get_unchecked() } {
        CassDataTypeInner::Map {
            typ: MapDataType::KeyAndValue(_, value),
            ..
        } => unsafe { value.get_unchecked() }.get_value_type(),
        _ => CassValueType::CASS_VALUE_TYPE_UNKNOWN,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_result_row_count(
    result_raw: CassBorrowedSharedPtr<CassResult, CConst>,
) -> size_t {
    let result = ArcFFI::as_ref(result_raw).unwrap();

    let CassResultKind::Rows(CassRowsResult { shared_data, .. }) = &result.kind else {
        return 0;
    };

    shared_data.raw_rows.rows_count() as size_t
}

#[no_mangle]
pub unsafe extern "C" fn cass_result_column_count(
    result_raw: CassBorrowedSharedPtr<CassResult, CConst>,
) -> size_t {
    let result = ArcFFI::as_ref(result_raw).unwrap();

    let CassResultKind::Rows(CassRowsResult { shared_data, .. }) = &result.kind else {
        return 0;
    };

    shared_data.metadata.col_specs.len() as size_t
}

#[no_mangle]
pub unsafe extern "C" fn cass_result_first_row(
    result_raw: CassBorrowedSharedPtr<CassResult, CConst>,
) -> CassBorrowedSharedPtr<CassRow, CConst> {
    let result = ArcFFI::as_ref(result_raw).unwrap();

    let CassResultKind::Rows(CassRowsResult { first_row, .. }) = &result.kind else {
        return RefFFI::null();
    };

    first_row
        .as_ref()
        .map(RowWithSelfBorrowedResultData::row)
        .map(RefFFI::as_ptr)
        .unwrap_or(RefFFI::null())
}

#[no_mangle]
pub unsafe extern "C" fn cass_result_paging_state_token(
    result: CassBorrowedSharedPtr<CassResult, CConst>,
    paging_state: *mut *const c_char,
    paging_state_size: *mut size_t,
) -> CassError {
    if unsafe { result_has_more_pages(&result) } == cass_false {
        return CassError::CASS_ERROR_LIB_NO_PAGING_STATE;
    }

    let result_from_raw = ArcFFI::as_ref(result).unwrap();

    match &result_from_raw.paging_state_response {
        PagingStateResponse::HasMorePages { state } => match state.as_bytes_slice() {
            Some(result_paging_state) => unsafe {
                *paging_state_size = result_paging_state.len() as u64;
                *paging_state = result_paging_state.as_ptr() as *const c_char;
            },
            None => unsafe {
                *paging_state_size = 0;
                *paging_state = std::ptr::null();
            },
        },
        PagingStateResponse::NoMorePages => unsafe {
            *paging_state_size = 0;
            *paging_state = std::ptr::null();
        },
    }

    CassError::CASS_OK
}

#[cfg(test)]
mod tests {
    use scylla::cluster::metadata::{CollectionType, ColumnType, NativeType};
    use scylla::frame::response::result::{ColumnSpec, DeserializedMetadataAndRawRows, TableSpec};
    use scylla::response::query_result::ColumnSpecs;
    use scylla::response::PagingStateResponse;

    use crate::argconv::{ptr_to_cstr_n, CConst, CassBorrowedSharedPtr};
    use crate::cass_types::{CassDataType, CassDataTypeInner};
    use crate::{
        argconv::{ArcFFI, RefFFI},
        cass_error::CassError,
        cass_types::CassValueType,
        query_result::{
            cass_result_column_data_type, cass_result_column_name, cass_result_first_row, size_t,
        },
    };
    use std::{ffi::c_char, ptr::addr_of_mut, sync::Arc};

    use super::row_with_self_borrowed_result_data::RowWithSelfBorrowedResultData;
    use super::{
        cass_result_column_count, cass_result_column_type, CassResult, CassResultKind,
        CassResultMetadata, CassRowsResult, CassRowsResultSharedData,
    };

    fn col_spec(name: &'static str, typ: ColumnType<'static>) -> ColumnSpec<'static> {
        ColumnSpec::borrowed(name, typ, TableSpec::borrowed("ks", "tbl"))
    }

    const FIRST_COLUMN_NAME: &str = "bigint_col";
    const SECOND_COLUMN_NAME: &str = "varint_col";
    const THIRD_COLUMN_NAME: &str = "list_double_col";
    fn create_cass_rows_result() -> CassResult {
        let metadata = Arc::new(CassResultMetadata::from_column_specs(ColumnSpecs::new(&[
            col_spec(FIRST_COLUMN_NAME, ColumnType::Native(NativeType::BigInt)),
            col_spec(SECOND_COLUMN_NAME, ColumnType::Native(NativeType::Varint)),
            col_spec(
                THIRD_COLUMN_NAME,
                ColumnType::Collection {
                    frozen: false,
                    typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Double))),
                },
            ),
        ])));

        let raw_rows = DeserializedMetadataAndRawRows::mock_empty();
        let shared_data = Arc::new(CassRowsResultSharedData { raw_rows, metadata });
        let first_row = RowWithSelfBorrowedResultData::first_from_raw_rows_and_metadata(
            Arc::clone(&shared_data),
        )
        .unwrap();

        CassResult {
            tracing_id: None,
            paging_state_response: PagingStateResponse::NoMorePages,
            kind: CassResultKind::Rows(CassRowsResult {
                shared_data,
                first_row,
            }),
        }
    }

    unsafe fn cass_result_column_name_rust_str(
        result_ptr: CassBorrowedSharedPtr<CassResult, CConst>,
        column_index: u64,
    ) -> Option<&'static str> {
        let mut name_ptr: *const c_char = std::ptr::null();
        let mut name_length: size_t = 0;
        let cass_err = unsafe {
            cass_result_column_name(
                result_ptr,
                column_index,
                addr_of_mut!(name_ptr),
                addr_of_mut!(name_length),
            )
        };
        assert_eq!(CassError::CASS_OK, cass_err);
        unsafe { ptr_to_cstr_n(name_ptr, name_length) }
    }

    #[test]
    fn rows_cass_result_api_test() {
        let result = Arc::new(create_cass_rows_result());

        unsafe {
            let result_ptr = ArcFFI::as_ptr(&result);

            // cass_result_column_count test
            {
                let column_count = cass_result_column_count(result_ptr.borrow());
                assert_eq!(3, column_count);
            }

            // cass_result_column_name test
            {
                let first_column_name =
                    cass_result_column_name_rust_str(result_ptr.borrow(), 0).unwrap();
                assert_eq!(FIRST_COLUMN_NAME, first_column_name);
                let second_column_name =
                    cass_result_column_name_rust_str(result_ptr.borrow(), 1).unwrap();
                assert_eq!(SECOND_COLUMN_NAME, second_column_name);
                let third_column_name =
                    cass_result_column_name_rust_str(result_ptr.borrow(), 2).unwrap();
                assert_eq!(THIRD_COLUMN_NAME, third_column_name);
            }

            // cass_result_column_type test
            {
                let first_col_type = cass_result_column_type(result_ptr.borrow(), 0);
                assert_eq!(CassValueType::CASS_VALUE_TYPE_BIGINT, first_col_type);
                let second_col_type = cass_result_column_type(result_ptr.borrow(), 1);
                assert_eq!(CassValueType::CASS_VALUE_TYPE_VARINT, second_col_type);
                let third_col_type = cass_result_column_type(result_ptr.borrow(), 2);
                assert_eq!(CassValueType::CASS_VALUE_TYPE_LIST, third_col_type);
                let out_of_bound_col_type = cass_result_column_type(result_ptr.borrow(), 555);
                assert_eq!(
                    CassValueType::CASS_VALUE_TYPE_UNKNOWN,
                    out_of_bound_col_type
                );
            }

            // cass_result_column_data_type test
            {
                let first_col_data_type_ptr = cass_result_column_data_type(result_ptr.borrow(), 0);
                let first_col_data_type = ArcFFI::as_ref(first_col_data_type_ptr).unwrap();
                assert_eq!(
                    &CassDataType::new(CassDataTypeInner::Value(
                        CassValueType::CASS_VALUE_TYPE_BIGINT
                    )),
                    first_col_data_type
                );
                let second_col_data_type_ptr = cass_result_column_data_type(result_ptr.borrow(), 1);
                let second_col_data_type = ArcFFI::as_ref(second_col_data_type_ptr).unwrap();
                assert_eq!(
                    &CassDataType::new(CassDataTypeInner::Value(
                        CassValueType::CASS_VALUE_TYPE_VARINT
                    )),
                    second_col_data_type
                );
                let third_col_data_type_ptr = cass_result_column_data_type(result_ptr.borrow(), 2);
                let third_col_data_type = ArcFFI::as_ref(third_col_data_type_ptr).unwrap();
                assert_eq!(
                    &CassDataType::new(CassDataTypeInner::List {
                        typ: Some(CassDataType::new_arced(CassDataTypeInner::Value(
                            CassValueType::CASS_VALUE_TYPE_DOUBLE
                        ))),
                        frozen: false
                    }),
                    third_col_data_type
                );
                let out_of_bound_col_data_type =
                    cass_result_column_data_type(result_ptr.borrow(), 555);
                assert!(ArcFFI::is_null(&out_of_bound_col_data_type));
            }
        }
    }

    fn create_non_rows_cass_result() -> CassResult {
        CassResult {
            tracing_id: None,
            paging_state_response: PagingStateResponse::NoMorePages,
            kind: CassResultKind::NonRows,
        }
    }

    #[test]
    fn non_rows_cass_result_api_test() {
        let result = Arc::new(create_non_rows_cass_result());

        // Check that API functions do not panic when rows are empty - e.g. for INSERT queries.
        unsafe {
            let result_ptr = ArcFFI::as_ptr(&result);

            assert_eq!(0, cass_result_column_count(result_ptr.borrow()));
            assert_eq!(
                CassValueType::CASS_VALUE_TYPE_UNKNOWN,
                cass_result_column_type(result_ptr.borrow(), 0)
            );
            assert!(ArcFFI::is_null(&cass_result_column_data_type(
                result_ptr.borrow(),
                0
            )));
            assert!(RefFFI::is_null(&cass_result_first_row(result_ptr.borrow())));

            {
                let mut name_ptr: *const c_char = std::ptr::null();
                let mut name_length: size_t = 0;
                let cass_err = cass_result_column_name(
                    result_ptr,
                    0,
                    addr_of_mut!(name_ptr),
                    addr_of_mut!(name_length),
                );
                assert_eq!(CassError::CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS, cass_err);
            }
        }
    }
}
