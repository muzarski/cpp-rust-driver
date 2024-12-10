use crate::argconv::*;
use crate::cass_error::{CassError, ToCassError};
use crate::cass_types::{
    cass_data_type_type, get_column_type, CassColumnSpec, CassDataType, CassDataTypeInner,
    CassValueType, MapDataType,
};
use crate::inet::CassInet;
use crate::metadata::{
    CassColumnMeta, CassKeyspaceMeta, CassMaterializedViewMeta, CassSchemaMeta, CassTableMeta,
};
use crate::query_error::CassErrorResult;
use crate::types::*;
use crate::uuid::CassUuid;
use scylla::deserialize::result::TypedRowIterator;
use scylla::deserialize::row::{
    BuiltinDeserializationError, BuiltinDeserializationErrorKind, ColumnIterator,
};
use scylla::deserialize::value::{ListlikeIterator, MapIterator, UdtIterator};
use scylla::deserialize::{
    DeserializationError, DeserializeRow, DeserializeValue, FrameSlice, TypeCheckError,
};
use scylla::frame::response::result::{ColumnSpec, ColumnType, DeserializedMetadataAndRawRows};
use scylla::frame::value::{
    Counter, CqlDate, CqlDecimalBorrowed, CqlDuration, CqlTime, CqlTimestamp, CqlTimeuuid,
};
use scylla::transport::query_result::{ColumnSpecs, IntoRowsResultError};
use scylla::transport::PagingStateResponse;
use scylla::QueryResult;
use std::borrow::{Borrow, Cow};
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
    pub raw_rows: DeserializedMetadataAndRawRows,
    pub first_row: Option<CassRow<'static>>,
    pub metadata: Arc<CassResultMetadata>,
}

pub struct CassResult {
    pub tracing_id: Option<Uuid>,
    pub paging_state_response: PagingStateResponse,
    pub kind: CassResultKind,
}

impl CassRowsResult {
    unsafe fn create_first_row(
        raw_rows: &DeserializedMetadataAndRawRows,
        metadata: &CassResultMetadata,
    ) -> Result<Option<CassRow<'static>>, CassErrorResult> {
        let first_row = raw_rows
            .rows_iter::<CassRawRow>()
            .unwrap()
            .next()
            .transpose()?
            .map(|row| std::mem::transmute(CassRow::from_row_and_metadata(row.columns, metadata)));

        Ok(first_row)
    }
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
                    Arc::new(CassResultMetadata::from_column_spec_views(
                        rows_result.column_specs(),
                    ))
                });

                let (raw_rows, tracing_id, _) = rows_result.into_inner();
                let first_row = unsafe { CassRowsResult::create_first_row(&raw_rows, &metadata)? };

                let cass_result = CassResult {
                    tracing_id,
                    paging_state_response,
                    kind: CassResultKind::Rows(CassRowsResult {
                        raw_rows,
                        first_row,
                        metadata,
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
    type Ownership = OwnershipShared;
}

#[derive(Debug)]
pub struct CassResultMetadata {
    pub col_specs: Vec<CassColumnSpec>,
}

impl CassResultMetadata {
    pub fn from_column_specs(col_specs: &[ColumnSpec<'_>]) -> CassResultMetadata {
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

    // I don't like introducing this method, but there is a discrepancy
    // between the types representing column specs returned from
    // `QueryRowsResult::column_specs()` (returns ColumnSpecs<'_>) and
    // `PreparedStatement::get_result_set_col_specs()` (returns &[ColumnSpec<'_>).
    //
    // I tried to workaround it with accepting a generic type, such as iterator,
    // but then again, types of items we are iterating over differ as well -
    // ColumnSpecView<'_> vs ColumnSpec<'_>.
    //
    // This should probably be adjusted on rust-driver side.
    pub fn from_column_spec_views(col_specs: ColumnSpecs<'_>) -> CassResultMetadata {
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

pub struct CassRawRow<'result> {
    columns: Vec<CassRawValue<'result>>,
}

impl<'frame, 'metadata, 'result> DeserializeRow<'frame, 'metadata> for CassRawRow<'result>
where
    'frame: 'result,
    'metadata: 'result,
{
    fn type_check(_specs: &[ColumnSpec]) -> Result<(), TypeCheckError> {
        Ok(())
    }

    fn deserialize(
        mut row: ColumnIterator<'frame, 'metadata>,
    ) -> Result<Self, DeserializationError> {
        let mut columns = Vec::with_capacity(row.size_hint().0);
        while let Some(column) = row.next().transpose()? {
            columns.push(
                <CassRawValue>::deserialize(column.spec.typ(), column.slice).map_err(|err| {
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
    type Ownership = OwnershipBorrowed;
}

impl<'result> CassRow<'result> {
    fn from_row_and_metadata(
        row: Vec<CassRawValue<'result>>,
        metadata: &'result CassResultMetadata,
    ) -> CassRow<'result> {
        Self {
            columns: create_cass_row_columns(row, metadata),
            result_metadata: metadata,
        }
    }
}

pub struct CassRawValue<'result> {
    pub typ: &'result ColumnType<'result>,
    pub slice: Option<FrameSlice<'result>>,
}

impl<'frame, 'metadata, 'result> DeserializeValue<'frame, 'metadata> for CassRawValue<'result>
where
    'metadata: 'result,
    'frame: 'result,
{
    fn type_check(_typ: &ColumnType) -> Result<(), TypeCheckError> {
        Ok(())
    }

    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        Ok(Self { typ, slice: v })
    }
}

pub enum Collection<'result> {
    List(Vec<CassValue<'result>>),
    Map(Vec<(CassValue<'result>, CassValue<'result>)>),
    Set(Vec<CassValue<'result>>),
    UserDefinedType {
        keyspace: String,
        type_name: String,
        fields: Vec<(String, Option<CassValue<'result>>)>,
    },
    Tuple(Vec<Option<CassValue<'result>>>),
}

pub struct CassValue<'result> {
    pub value: CassRawValue<'result>,
    pub value_type: Arc<CassDataType>,
}

impl FFI for CassValue<'_> {
    type Ownership = OwnershipBorrowed;
}

impl CassValue<'_> {
    pub fn get_non_null<'result, T>(&'result self) -> Result<T, NonNullDeserializationError>
    where
        T: DeserializeValue<'result, 'result>,
    {
        if self.value.slice.is_none() {
            return Err(NonNullDeserializationError::IsNull);
        }

        T::type_check(self.value.typ)?;
        let v = T::deserialize(self.value.typ, self.value.slice)?;
        Ok(v)
    }

    pub fn get_bytes_non_null(&self) -> Result<&[u8], NonNullDeserializationError> {
        let Some(slice) = self.value.slice else {
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
    row: Vec<CassRawValue<'result>>,
    metadata: &'result CassResultMetadata,
) -> Vec<CassValue<'result>> {
    row.into_iter()
        .zip(metadata.col_specs.iter())
        .map(|(value, col_spec)| {
            let column_type = Arc::clone(&col_spec.data_type);
            CassValue {
                value,
                value_type: column_type,
            }
        })
        .collect()
}

// fn get_column_value<'result>(
//     column: CassRawValue<'result>,
//     column_type: &Arc<CassDataType>,
// ) -> Value<'result> {
//     match (column, unsafe { column_type.get_unchecked() }) {
//         (
//             CqlValue::List(list),
//             CassDataTypeInner::List {
//                 typ: Some(list_type),
//                 ..
//             },
//         ) => CollectionValue(Collection::List(
//             list.into_iter()
//                 .map(|val| CassValue {
//                     value_type: list_type.clone(),
//                     value: Some(get_column_value(val, list_type)),
//                 })
//                 .collect(),
//         )),
//         (
//             CqlValue::Map(map),
//             CassDataTypeInner::Map {
//                 typ: MapDataType::KeyAndValue(key_type, value_type),
//                 ..
//             },
//         ) => CollectionValue(Collection::Map(
//             map.into_iter()
//                 .map(|(key, val)| {
//                     (
//                         CassValue {
//                             value_type: key_type.clone(),
//                             value: Some(get_column_value(key, key_type)),
//                         },
//                         CassValue {
//                             value_type: value_type.clone(),
//                             value: Some(get_column_value(val, value_type)),
//                         },
//                     )
//                 })
//                 .collect(),
//         )),
//         (
//             CqlValue::Set(set),
//             CassDataTypeInner::Set {
//                 typ: Some(set_type),
//                 ..
//             },
//         ) => CollectionValue(Collection::Set(
//             set.into_iter()
//                 .map(|val| CassValue {
//                     value_type: set_type.clone(),
//                     value: Some(get_column_value(val, set_type)),
//                 })
//                 .collect(),
//         )),
//         (
//             CqlValue::UserDefinedType {
//                 keyspace,
//                 type_name,
//                 fields,
//             },
//             CassDataTypeInner::UDT(udt_type),
//         ) => CollectionValue(Collection::UserDefinedType {
//             keyspace,
//             type_name,
//             fields: fields
//                 .into_iter()
//                 .enumerate()
//                 .map(|(index, (name, val_opt))| {
//                     let udt_field_type_opt = udt_type.get_field_by_index(index);
//                     if let (Some(val), Some(udt_field_type)) = (val_opt, udt_field_type_opt) {
//                         return (
//                             name,
//                             Some(CassValue {
//                                 value_type: udt_field_type.clone(),
//                                 value: Some(get_column_value(val, udt_field_type)),
//                             }),
//                         );
//                     }
//                     (name, None)
//                 })
//                 .collect(),
//         }),
//         (CqlValue::Tuple(tuple), CassDataTypeInner::Tuple(tuple_types)) => {
//             CollectionValue(Collection::Tuple(
//                 tuple
//                     .into_iter()
//                     .enumerate()
//                     .map(|(index, val_opt)| {
//                         val_opt
//                             .zip(tuple_types.get(index))
//                             .map(|(val, tuple_field_type)| CassValue {
//                                 value_type: tuple_field_type.clone(),
//                                 value: Some(get_column_value(val, tuple_field_type)),
//                             })
//                     })
//                     .collect(),
//             ))
//         }
//         (regular_value, _) => RegularValue(regular_value),
//     }
// }

pub struct CassRowsResultIterator<'result> {
    iterator: TypedRowIterator<'result, 'result, CassRawRow<'result>>,
    result_metadata: &'result CassResultMetadata,
    current_row: Option<CassRow<'result>>,
}

pub enum CassResultIterator<'result> {
    NonRows,
    Rows(CassRowsResultIterator<'result>),
}

pub struct CassRowIterator<'result> {
    row: &'result CassRow<'result>,
    position: Option<usize>,
}

pub struct CassListLikeIterator<'result> {
    iterator: ListlikeIterator<'result, 'result, CassRawValue<'result>>,
    item_data_type: Arc<CassDataType>,
    current_value: Option<CassValue<'result>>,
}

pub enum CassMapIteratorState {
    Key,
    Value,
}

pub struct CassMapIterator<'result> {
    iterator: MapIterator<'result, 'result, CassRawValue<'result>, CassRawValue<'result>>,
    entry_data_type: (Arc<CassDataType>, Arc<CassDataType>),
    current_entry: Option<(CassValue<'result>, CassValue<'result>)>,
    state: CassMapIteratorState,
}

pub enum CassCollectionIterator<'result> {
    ListOrSet(CassListLikeIterator<'result>),
    Map(CassMapIterator<'result>),
}

pub struct CassUdtField<'result> {
    name: Cow<'result, str>,
    value: CassValue<'result>,
}

pub struct CassUdtIterator<'result> {
    iterator: UdtIterator<'result, 'result>,
    field_types: &'result [(String, Arc<CassDataType>)],
    current: Option<(usize, CassUdtField<'result>)>,
}

pub struct CassSchemaMetaIterator {
    value: &'static CassSchemaMeta,
    count: usize,
    position: Option<usize>,
}

pub struct CassKeyspaceMetaIterator {
    value: &'static CassKeyspaceMeta,
    count: usize,
    position: Option<usize>,
}

pub struct CassTableMetaIterator {
    value: &'static CassTableMeta,
    count: usize,
    position: Option<usize>,
}

pub struct CassViewMetaIterator {
    value: &'static CassMaterializedViewMeta,
    count: usize,
    position: Option<usize>,
}

pub enum CassIterator<'result> {
    CassResultIterator(CassResultIterator<'result>),
    CassRowIterator(CassRowIterator<'result>),
    CassCollectionIterator(CassCollectionIterator<'result>),
    CassMapIterator(CassMapIterator<'result>),
    CassUdtIterator(CassUdtIterator<'result>),
    CassSchemaMetaIterator(CassSchemaMetaIterator),
    CassKeyspaceMetaTableIterator(CassKeyspaceMetaIterator),
    CassKeyspaceMetaUserTypeIterator(CassKeyspaceMetaIterator),
    CassKeyspaceMetaViewIterator(CassKeyspaceMetaIterator),
    CassTableMetaIterator(CassTableMetaIterator),
    CassViewMetaIterator(CassViewMetaIterator),
}

impl FFI for CassIterator<'_> {
    type Ownership = OwnershipExclusive;
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_free(iterator: CassExclusiveMutPtr<CassIterator>) {
    BoxFFI::free(iterator);
}

// After creating an iterator we have to call next() before accessing the value
#[no_mangle]
pub unsafe extern "C" fn cass_iterator_next(
    mut iterator: CassExclusiveMutPtr<CassIterator>,
) -> cass_bool_t {
    let mut iter = BoxFFI::as_mut_ref(&mut iterator).unwrap();

    match &mut iter {
        CassIterator::CassResultIterator(result_iterator) => {
            let CassResultIterator::Rows(rows_result_iterator) = result_iterator else {
                return false as cass_bool_t;
            };

            let new_row = rows_result_iterator
                .iterator
                .next()
                .and_then(Result::ok)
                .map(|row| {
                    CassRow::from_row_and_metadata(
                        row.columns,
                        rows_result_iterator.result_metadata,
                    )
                });

            rows_result_iterator.current_row = new_row;

            rows_result_iterator.current_row.is_some() as cass_bool_t
        }
        CassIterator::CassRowIterator(row_iterator) => {
            let new_pos: usize = row_iterator.position.map_or(0, |prev_pos| prev_pos + 1);

            row_iterator.position = Some(new_pos);

            (new_pos < row_iterator.row.columns.len()) as cass_bool_t
        }
        CassIterator::CassCollectionIterator(collection_iterator) => match collection_iterator {
            CassCollectionIterator::ListOrSet(ll_iter) => {
                let new_value = ll_iter
                    .iterator
                    .next()
                    .and_then(Result::ok)
                    .map(|raw_value| CassValue {
                        value: raw_value,
                        value_type: Arc::clone(&ll_iter.item_data_type),
                    });

                ll_iter.current_value = new_value;

                ll_iter.current_value.is_some() as cass_bool_t
            }
            CassCollectionIterator::Map(map_iter) => {
                match map_iter.state {
                    CassMapIteratorState::Key if map_iter.current_entry.is_some() => {
                        map_iter.state = CassMapIteratorState::Value;
                    }
                    _ => {
                        let new_entry =
                            map_iter
                                .iterator
                                .next()
                                .and_then(Result::ok)
                                .map(|raw_entry| {
                                    (
                                        CassValue {
                                            value: raw_entry.0,
                                            value_type: Arc::clone(&map_iter.entry_data_type.0),
                                        },
                                        CassValue {
                                            value: raw_entry.1,
                                            value_type: Arc::clone(&map_iter.entry_data_type.1),
                                        },
                                    )
                                });

                        map_iter.current_entry = new_entry;
                        map_iter.state = CassMapIteratorState::Key;
                    }
                }
                map_iter.current_entry.is_some() as cass_bool_t
            }
        },
        CassIterator::CassMapIterator(map_iter) => {
            let new_entry = map_iter
                .iterator
                .next()
                .and_then(Result::ok)
                .map(|raw_entry| {
                    (
                        CassValue {
                            value: raw_entry.0,
                            value_type: Arc::clone(&map_iter.entry_data_type.0),
                        },
                        CassValue {
                            value: raw_entry.1,
                            value_type: Arc::clone(&map_iter.entry_data_type.1),
                        },
                    )
                });

            map_iter.current_entry = new_entry;
            map_iter.current_entry.is_some() as cass_bool_t
        }
        CassIterator::CassUdtIterator(udt_iterator) => {
            let next = {
                let pos = udt_iterator.current.as_ref().map_or(0, |(pos, _)| pos + 1);
                let field = udt_iterator
                    .iterator
                    .next()
                    .map(|((col_name, col_type), res)| {
                        res.ok().map(|v| {
                            let value = CassValue {
                                value: CassRawValue {
                                    typ: col_type,
                                    slice: v.flatten(),
                                },
                                value_type: Arc::clone(&udt_iterator.field_types[pos].1),
                            };
                            CassUdtField {
                                name: col_name.clone(),
                                value,
                            }
                        })
                    })
                    .flatten();

                field.map(|f| (pos, f))
            };

            udt_iterator.current = next;

            udt_iterator.current.is_some() as cass_bool_t
        }
        CassIterator::CassSchemaMetaIterator(schema_meta_iterator) => {
            let new_pos: usize = schema_meta_iterator
                .position
                .map_or(0, |prev_pos| prev_pos + 1);

            schema_meta_iterator.position = Some(new_pos);

            (new_pos < schema_meta_iterator.count) as cass_bool_t
        }
        CassIterator::CassKeyspaceMetaTableIterator(keyspace_meta_iterator) => {
            let new_pos: usize = keyspace_meta_iterator
                .position
                .map_or(0, |prev_pos| prev_pos + 1);

            keyspace_meta_iterator.position = Some(new_pos);

            (new_pos < keyspace_meta_iterator.count) as cass_bool_t
        }
        CassIterator::CassKeyspaceMetaUserTypeIterator(keyspace_meta_iterator) => {
            let new_pos: usize = keyspace_meta_iterator
                .position
                .map_or(0, |prev_pos| prev_pos + 1);

            keyspace_meta_iterator.position = Some(new_pos);

            (new_pos < keyspace_meta_iterator.count) as cass_bool_t
        }
        CassIterator::CassKeyspaceMetaViewIterator(keyspace_meta_iterator) => {
            let new_pos: usize = keyspace_meta_iterator
                .position
                .map_or(0, |prev_pos| prev_pos + 1);

            keyspace_meta_iterator.position = Some(new_pos);

            (new_pos < keyspace_meta_iterator.count) as cass_bool_t
        }
        CassIterator::CassTableMetaIterator(table_iterator) => {
            let new_pos: usize = table_iterator.position.map_or(0, |prev_pos| prev_pos + 1);

            table_iterator.position = Some(new_pos);

            (new_pos < table_iterator.count) as cass_bool_t
        }
        CassIterator::CassViewMetaIterator(view_iterator) => {
            let new_pos: usize = view_iterator.position.map_or(0, |prev_pos| prev_pos + 1);

            view_iterator.position = Some(new_pos);

            (new_pos < view_iterator.count) as cass_bool_t
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_get_row(
    iterator: CassExclusiveConstPtr<CassIterator>,
) -> CassBorrowedPtr<CassRow> {
    let iter = BoxFFI::as_ref(&iterator).unwrap();

    // Defined only for result iterator, for other types should return null
    if let CassIterator::CassResultIterator(result_iterator) = iter {
        let CassResultIterator::Rows(rows_result_iterator) = result_iterator else {
            return RefFFI::null();
        };

        return rows_result_iterator
            .current_row
            .as_ref()
            .map(RefFFI::as_ptr)
            .unwrap_or(RefFFI::null());
    }

    RefFFI::null()
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_get_column(
    iterator: CassExclusiveConstPtr<CassIterator>,
) -> CassBorrowedPtr<CassValue> {
    let iter = BoxFFI::as_ref(&iterator).unwrap();

    // Defined only for row iterator, for other types should return null
    if let CassIterator::CassRowIterator(row_iterator) = iter {
        let iter_position = match row_iterator.position {
            Some(pos) => pos,
            None => return RefFFI::null(),
        };

        let value = match row_iterator.row.columns.get(iter_position) {
            Some(col) => col,
            None => return RefFFI::null(),
        };

        return RefFFI::as_ptr(value);
    }

    RefFFI::null()
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_get_value(
    iterator: CassExclusiveConstPtr<CassIterator>,
) -> CassBorrowedPtr<CassValue> {
    let iter = BoxFFI::as_ref(&iterator).unwrap();

    // Defined only for collections(list, set and map) or tuple iterator, for other types should return null
    match iter {
        CassIterator::CassCollectionIterator(collection_iterator) => match collection_iterator {
            CassCollectionIterator::ListOrSet(ll_iter) => ll_iter
                .current_value
                .as_ref()
                .map(RefFFI::as_ptr)
                .unwrap_or(RefFFI::null()),
            CassCollectionIterator::Map(map_iter) => {
                match (&map_iter.current_entry, &map_iter.state) {
                    (Some(entry), CassMapIteratorState::Key) => RefFFI::as_ptr(&entry.0),
                    (Some(entry), CassMapIteratorState::Value) => RefFFI::as_ptr(&entry.1),
                    (None, _) => RefFFI::null(),
                }
            }
        },
        _ => RefFFI::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_get_map_key<'result>(
    iterator: CassExclusiveConstPtr<CassIterator<'result>>,
) -> CassBorrowedPtr<CassValue<'result>> {
    let iter = BoxFFI::as_ref(&iterator).unwrap();

    let CassIterator::CassMapIterator(map_iter) = iter else {
        return RefFFI::null();
    };

    map_iter
        .current_entry
        .as_ref()
        .map(|entry| RefFFI::as_ptr(&entry.0))
        .unwrap_or(RefFFI::null())
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_get_map_value<'result>(
    iterator: CassExclusiveConstPtr<CassIterator<'result>>,
) -> CassBorrowedPtr<CassValue<'result>> {
    let iter = BoxFFI::as_ref(&iterator).unwrap();

    let CassIterator::CassMapIterator(map_iter) = iter else {
        return RefFFI::null();
    };

    map_iter
        .current_entry
        .as_ref()
        .map(|entry| RefFFI::as_ptr(&entry.1))
        .unwrap_or(RefFFI::null())
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_get_user_type_field_name(
    iterator: CassExclusiveConstPtr<CassIterator>,
    name: *mut *const c_char,
    name_length: *mut size_t,
) -> CassError {
    let iter = BoxFFI::as_ref(&iterator).unwrap();

    let CassIterator::CassUdtIterator(udt_iter) = iter else {
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    let (_, field) = match udt_iter.current.as_ref() {
        Some(current) => current,
        None => return CassError::CASS_ERROR_LIB_BAD_PARAMS,
    };
    let field_name = &field.name;

    write_str_to_c(field_name.as_ref(), name, name_length);

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_get_user_type_field_value(
    iterator: CassExclusiveConstPtr<CassIterator>,
) -> CassBorrowedPtr<CassValue> {
    let iter = BoxFFI::as_ref(&iterator).unwrap();

    let CassIterator::CassUdtIterator(udt_iter) = iter else {
        return RefFFI::null();
    };

    udt_iter
        .current
        .as_ref()
        .map(|(_, field)| RefFFI::as_ptr(&field.value))
        .unwrap_or(RefFFI::null())
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_get_keyspace_meta(
    iterator: CassExclusiveConstPtr<CassIterator>,
) -> CassBorrowedPtr<CassKeyspaceMeta> {
    let iter = BoxFFI::as_ref(&iterator).unwrap();

    if let CassIterator::CassSchemaMetaIterator(schema_meta_iterator) = iter {
        let iter_position = match schema_meta_iterator.position {
            Some(pos) => pos,
            None => return RefFFI::null(),
        };

        let schema_meta_entry_opt = &schema_meta_iterator
            .value
            .keyspaces
            .iter()
            .nth(iter_position);

        return match schema_meta_entry_opt {
            Some(schema_meta_entry) => RefFFI::as_ptr(schema_meta_entry.1),
            None => RefFFI::null(),
        };
    }

    RefFFI::null()
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_get_table_meta(
    iterator: CassExclusiveConstPtr<CassIterator>,
) -> CassBorrowedPtr<CassTableMeta> {
    let iter = BoxFFI::as_ref(&iterator).unwrap();

    if let CassIterator::CassKeyspaceMetaTableIterator(keyspace_meta_iterator) = iter {
        let iter_position = match keyspace_meta_iterator.position {
            Some(pos) => pos,
            None => return RefFFI::null(),
        };

        let table_meta_entry_opt = keyspace_meta_iterator
            .value
            .tables
            .iter()
            .nth(iter_position);

        return match table_meta_entry_opt {
            Some(table_meta_entry) => RefFFI::as_ptr(table_meta_entry.1.as_ref()),
            None => RefFFI::null(),
        };
    }

    RefFFI::null()
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_get_user_type(
    iterator: CassExclusiveConstPtr<CassIterator>,
) -> CassSharedPtr<CassDataType> {
    let iter = BoxFFI::as_ref(&iterator).unwrap();

    if let CassIterator::CassKeyspaceMetaUserTypeIterator(keyspace_meta_iterator) = iter {
        let iter_position = match keyspace_meta_iterator.position {
            Some(pos) => pos,
            None => return ArcFFI::null(),
        };

        let udt_to_type_entry_opt = keyspace_meta_iterator
            .value
            .user_defined_type_data_type
            .iter()
            .nth(iter_position);

        return match udt_to_type_entry_opt {
            Some(udt_to_type_entry) => ArcFFI::as_ptr(udt_to_type_entry.1),
            None => ArcFFI::null(),
        };
    }

    ArcFFI::null()
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_get_column_meta(
    iterator: CassExclusiveConstPtr<CassIterator>,
) -> CassBorrowedPtr<CassColumnMeta> {
    let iter = BoxFFI::as_ref(&iterator).unwrap();

    match iter {
        CassIterator::CassTableMetaIterator(table_meta_iterator) => {
            let iter_position = match table_meta_iterator.position {
                Some(pos) => pos,
                None => return RefFFI::null(),
            };

            let column_meta_entry_opt = table_meta_iterator
                .value
                .columns_metadata
                .iter()
                .nth(iter_position);

            match column_meta_entry_opt {
                Some(column_meta_entry) => RefFFI::as_ptr(column_meta_entry.1),
                None => RefFFI::null(),
            }
        }
        CassIterator::CassViewMetaIterator(view_meta_iterator) => {
            let iter_position = match view_meta_iterator.position {
                Some(pos) => pos,
                None => return RefFFI::null(),
            };

            let column_meta_entry_opt = view_meta_iterator
                .value
                .view_metadata
                .columns_metadata
                .iter()
                .nth(iter_position);

            match column_meta_entry_opt {
                Some(column_meta_entry) => RefFFI::as_ptr(column_meta_entry.1),
                None => RefFFI::null(),
            }
        }
        _ => RefFFI::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_get_materialized_view_meta(
    iterator: CassExclusiveConstPtr<CassIterator>,
) -> CassBorrowedPtr<CassMaterializedViewMeta> {
    let iter = BoxFFI::as_ref(&iterator).unwrap();

    match iter {
        CassIterator::CassKeyspaceMetaViewIterator(keyspace_meta_iterator) => {
            let iter_position = match keyspace_meta_iterator.position {
                Some(pos) => pos,
                None => return RefFFI::null(),
            };

            let view_meta_entry_opt = keyspace_meta_iterator.value.views.iter().nth(iter_position);

            match view_meta_entry_opt {
                Some(view_meta_entry) => RefFFI::as_ptr(view_meta_entry.1.as_ref()),
                None => RefFFI::null(),
            }
        }
        CassIterator::CassTableMetaIterator(table_meta_iterator) => {
            let iter_position = match table_meta_iterator.position {
                Some(pos) => pos,
                None => return RefFFI::null(),
            };

            let view_meta_entry_opt = table_meta_iterator.value.views.iter().nth(iter_position);

            match view_meta_entry_opt {
                Some(view_meta_entry) => RefFFI::as_ptr(view_meta_entry.1.as_ref()),
                None => RefFFI::null(),
            }
        }
        _ => RefFFI::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_from_result<'result>(
    result: CassSharedPtr<CassResult>,
) -> CassExclusiveMutPtr<CassIterator<'result>> {
    let result_from_raw = ArcFFI::into_ref(result).unwrap();

    let iterator = match &result_from_raw.kind {
        CassResultKind::NonRows => CassResultIterator::NonRows,
        CassResultKind::Rows(cass_rows_result) => {
            CassResultIterator::Rows(CassRowsResultIterator {
                iterator: cass_rows_result.raw_rows.rows_iter().unwrap(),
                result_metadata: &cass_rows_result.metadata,
                current_row: None,
            })
        }
    };

    BoxFFI::into_ptr(Box::new(CassIterator::CassResultIterator(iterator)))
}

#[no_mangle]
#[allow(clippy::needless_lifetimes)]
pub unsafe extern "C" fn cass_iterator_from_row<'result>(
    row: CassBorrowedPtr<CassRow<'result>>,
) -> CassExclusiveMutPtr<CassIterator<'result>> {
    let row_from_raw = RefFFI::into_ref(row).unwrap();

    let iterator = CassRowIterator {
        row: row_from_raw,
        position: None,
    };

    BoxFFI::into_ptr(Box::new(CassIterator::CassRowIterator(iterator)))
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_from_collection<'result>(
    value: CassBorrowedPtr<CassValue<'result>>,
) -> CassExclusiveMutPtr<CassIterator<'result>> {
    if RefFFI::is_null(&value) {
        return BoxFFI::null_mut();
    }

    let val = RefFFI::into_ref(value).unwrap();

    match val.value.typ {
        ColumnType::List(_) | ColumnType::Set(_) => {
            let iterator: ListlikeIterator<CassRawValue> = match val.get_non_null() {
                Ok(i) => i,
                Err(NonNullDeserializationError::Typecheck(_)) => {
                    panic!("The typecheck unexpectedly failed!")
                }
                Err(_) => return BoxFFI::null_mut(),
            };

            let item_data_type = match val.value_type.get_unchecked() {
                CassDataTypeInner::List {
                    typ: Some(list_type),
                    ..
                } => Arc::clone(list_type),
                CassDataTypeInner::Set {
                    typ: Some(set_type),
                    ..
                } => Arc::clone(set_type),
                _ => panic!("Inconsistent CassDataType and ColumnType!!!"),
            };

            let collection_iter = CassIterator::CassCollectionIterator(
                CassCollectionIterator::ListOrSet(CassListLikeIterator {
                    iterator,
                    item_data_type,
                    current_value: None,
                }),
            );

            BoxFFI::into_ptr(Box::new(collection_iter))
        }
        ColumnType::Map(_, _) => {
            let iterator: MapIterator<CassRawValue, CassRawValue> = match val.get_non_null() {
                Ok(i) => i,
                Err(NonNullDeserializationError::Typecheck(_)) => {
                    panic!("The typecheck unexpectedly failed!")
                }
                Err(_) => return BoxFFI::null_mut(),
            };

            let entry_data_type = match val.value_type.get_unchecked() {
                CassDataTypeInner::Map {
                    typ: MapDataType::KeyAndValue(key_type, value_type),
                    ..
                } => (Arc::clone(key_type), Arc::clone(value_type)),
                _ => panic!("Inconsistent CassDataType and ColumnType!!!"),
            };

            let collection_iter = CassIterator::CassCollectionIterator(
                CassCollectionIterator::Map(CassMapIterator {
                    iterator,
                    entry_data_type,
                    current_entry: None,
                    state: CassMapIteratorState::Key,
                }),
            );

            BoxFFI::into_ptr(Box::new(collection_iter))
        }
        _ => BoxFFI::null_mut(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_from_tuple(
    value: CassBorrowedPtr<CassValue>,
) -> CassExclusiveMutPtr<CassIterator<'static>> {
    let tuple = RefFFI::into_ref(value).unwrap();

    if let Some(Value::CollectionValue(Collection::Tuple(val))) = &tuple.value {
        let item_count = val.len();
        let iterator = CassCollectionIterator {
            value: tuple,
            count: item_count as u64,
            position: None,
        };

        return BoxFFI::into_ptr(Box::new(CassIterator::CassCollectionIterator(iterator)));
    }

    BoxFFI::null_mut()
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_from_map<'result>(
    value: CassBorrowedPtr<CassValue<'result>>,
) -> CassExclusiveMutPtr<CassIterator<'result>> {
    if RefFFI::is_null(&value) {
        return BoxFFI::null_mut();
    }

    let val = RefFFI::into_ref(value).unwrap();

    match val.value.typ {
        ColumnType::Map(_, _) => {
            let iterator: MapIterator<CassRawValue, CassRawValue> = match val.get_non_null() {
                Ok(i) => i,
                Err(NonNullDeserializationError::Typecheck(_)) => {
                    panic!("The typecheck unexpectedly failed!")
                }
                Err(_) => return BoxFFI::null_mut(),
            };

            let entry_data_type = match val.value_type.get_unchecked() {
                CassDataTypeInner::Map {
                    typ: MapDataType::KeyAndValue(key_type, value_type),
                    ..
                } => (Arc::clone(key_type), Arc::clone(value_type)),
                _ => panic!("Inconsistent CassDataType and ColumnType!!!"),
            };

            let collection_iter = CassIterator::CassCollectionIterator(
                CassCollectionIterator::Map(CassMapIterator {
                    iterator,
                    entry_data_type,
                    current_entry: None,
                    state: CassMapIteratorState::Key,
                }),
            );

            BoxFFI::into_ptr(Box::new(collection_iter))
        }
        _ => BoxFFI::null_mut(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_fields_from_user_type<'result>(
    value: CassBorrowedPtr<CassValue<'result>>,
) -> CassExclusiveMutPtr<CassIterator<'result>> {
    if RefFFI::is_null(&value) {
        return BoxFFI::null_mut();
    }

    let val = RefFFI::into_ref(value).unwrap();

    match val.value.typ {
        ColumnType::UserDefinedType { .. } => {
            let iterator: UdtIterator = match val.get_non_null() {
                Ok(i) => i,
                Err(NonNullDeserializationError::Typecheck(_)) => {
                    panic!("The typecheck unexpectedly failed!")
                }
                Err(_) => return BoxFFI::null_mut(),
            };

            let field_types = match val.value_type.get_unchecked() {
                CassDataTypeInner::UDT(udt) => udt.field_types.as_slice(),
                _ => panic!("Inconsistent CassDataType and ColumnType!!!"),
            };

            let udt_iter = CassIterator::CassUdtIterator(CassUdtIterator {
                iterator,
                field_types,
                current: None,
            });

            BoxFFI::into_ptr(Box::new(udt_iter))
        }
        _ => BoxFFI::null_mut(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_keyspaces_from_schema_meta(
    schema_meta: CassExclusiveConstPtr<CassSchemaMeta>,
) -> CassExclusiveMutPtr<CassIterator<'static>> {
    let metadata = BoxFFI::into_ref(schema_meta).unwrap();

    let iterator = CassSchemaMetaIterator {
        value: metadata,
        count: metadata.keyspaces.len(),
        position: None,
    };

    BoxFFI::into_ptr(Box::new(CassIterator::CassSchemaMetaIterator(iterator)))
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_tables_from_keyspace_meta(
    keyspace_meta: CassBorrowedPtr<CassKeyspaceMeta>,
) -> CassExclusiveMutPtr<CassIterator<'static>> {
    let metadata = RefFFI::into_ref(keyspace_meta).unwrap();

    let iterator = CassKeyspaceMetaIterator {
        value: metadata,
        count: metadata.tables.len(),
        position: None,
    };

    BoxFFI::into_ptr(Box::new(CassIterator::CassKeyspaceMetaTableIterator(
        iterator,
    )))
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_materialized_views_from_keyspace_meta(
    keyspace_meta: CassBorrowedPtr<CassKeyspaceMeta>,
) -> CassExclusiveMutPtr<CassIterator<'static>> {
    let metadata = RefFFI::into_ref(keyspace_meta).unwrap();

    let iterator = CassKeyspaceMetaIterator {
        value: metadata,
        count: metadata.views.len(),
        position: None,
    };

    BoxFFI::into_ptr(Box::new(CassIterator::CassKeyspaceMetaViewIterator(
        iterator,
    )))
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_user_types_from_keyspace_meta(
    keyspace_meta: CassBorrowedPtr<CassKeyspaceMeta>,
) -> CassExclusiveMutPtr<CassIterator<'static>> {
    let metadata = RefFFI::into_ref(keyspace_meta).unwrap();

    let iterator = CassKeyspaceMetaIterator {
        value: metadata,
        count: metadata.user_defined_type_data_type.len(),
        position: None,
    };

    BoxFFI::into_ptr(Box::new(CassIterator::CassKeyspaceMetaUserTypeIterator(
        iterator,
    )))
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_columns_from_table_meta(
    table_meta: CassBorrowedPtr<CassTableMeta>,
) -> CassExclusiveMutPtr<CassIterator<'static>> {
    let metadata = RefFFI::into_ref(table_meta).unwrap();

    let iterator = CassTableMetaIterator {
        value: metadata,
        count: metadata.columns_metadata.len(),
        position: None,
    };

    BoxFFI::into_ptr(Box::new(CassIterator::CassTableMetaIterator(iterator)))
}

pub unsafe extern "C" fn cass_iterator_materialized_views_from_table_meta(
    table_meta: CassBorrowedPtr<CassTableMeta>,
) -> CassExclusiveMutPtr<CassIterator<'static>> {
    let metadata = RefFFI::into_ref(table_meta).unwrap();

    let iterator = CassTableMetaIterator {
        value: metadata,
        count: metadata.views.len(),
        position: None,
    };

    BoxFFI::into_ptr(Box::new(CassIterator::CassTableMetaIterator(iterator)))
}

pub unsafe extern "C" fn cass_iterator_columns_from_materialized_view_meta(
    view_meta: CassBorrowedPtr<CassMaterializedViewMeta>,
) -> CassExclusiveMutPtr<CassIterator<'static>> {
    let metadata = RefFFI::into_ref(view_meta).unwrap();

    let iterator = CassViewMetaIterator {
        value: metadata,
        count: metadata.view_metadata.columns_metadata.len(),
        position: None,
    };

    BoxFFI::into_ptr(Box::new(CassIterator::CassViewMetaIterator(iterator)))
}

#[no_mangle]
pub unsafe extern "C" fn cass_result_free(result_raw: CassSharedPtr<CassResult>) {
    ArcFFI::free(result_raw);
}

#[no_mangle]
pub unsafe extern "C" fn cass_result_has_more_pages(
    result: CassSharedPtr<CassResult>,
) -> cass_bool_t {
    result_has_more_pages(&result)
}

unsafe fn result_has_more_pages(result: &CassSharedPtr<CassResult>) -> cass_bool_t {
    let result = ArcFFI::as_ref(result).unwrap();
    (!result.paging_state_response.finished()) as cass_bool_t
}

#[no_mangle]
pub unsafe extern "C" fn cass_row_get_column(
    row_raw: CassBorrowedPtr<CassRow>,
    index: size_t,
) -> CassBorrowedPtr<CassValue> {
    let row: &CassRow = RefFFI::as_ref(&row_raw).unwrap();

    let index_usize: usize = index.try_into().unwrap();
    let column_value = match row.columns.get(index_usize) {
        Some(val) => val,
        None => return RefFFI::null(),
    };

    RefFFI::as_ptr(column_value)
}

#[no_mangle]
pub unsafe extern "C" fn cass_row_get_column_by_name(
    row: CassBorrowedPtr<CassRow>,
    name: *const c_char,
) -> CassBorrowedPtr<CassValue> {
    let name_str = ptr_to_cstr(name).unwrap();
    let name_length = name_str.len();

    cass_row_get_column_by_name_n(row, name, name_length as size_t)
}

#[no_mangle]
pub unsafe extern "C" fn cass_row_get_column_by_name_n(
    row: CassBorrowedPtr<CassRow>,
    name: *const c_char,
    name_length: size_t,
) -> CassBorrowedPtr<CassValue> {
    let row_from_raw = RefFFI::as_ref(&row).unwrap();
    let mut name_str = ptr_to_cstr_n(name, name_length).unwrap();
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
    result: CassSharedPtr<CassResult>,
    index: size_t,
    name: *mut *const c_char,
    name_length: *mut size_t,
) -> CassError {
    let result_from_raw = ArcFFI::as_ref(&result).unwrap();
    let index_usize: usize = index.try_into().unwrap();

    let CassResultKind::Rows(CassRowsResult { metadata, .. }) = &result_from_raw.kind else {
        return CassError::CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS;
    };

    if index_usize >= metadata.col_specs.len() {
        return CassError::CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS;
    }

    let column_name = &metadata.col_specs.get(index_usize).unwrap().name;

    write_str_to_c(column_name, name, name_length);

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_result_column_type(
    result: CassSharedPtr<CassResult>,
    index: size_t,
) -> CassValueType {
    let data_type_ptr = cass_result_column_data_type(result, index);
    if ArcFFI::is_null(&data_type_ptr) {
        return CassValueType::CASS_VALUE_TYPE_UNKNOWN;
    }
    cass_data_type_type(data_type_ptr)
}

#[no_mangle]
pub unsafe extern "C" fn cass_result_column_data_type(
    result: CassSharedPtr<CassResult>,
    index: size_t,
) -> CassSharedPtr<CassDataType> {
    let result_from_raw: &CassResult = ArcFFI::as_ref(&result).unwrap();
    let index_usize: usize = index
        .try_into()
        .expect("Provided index is out of bounds. Max possible value is usize::MAX");

    let CassResultKind::Rows(CassRowsResult { metadata, .. }) = &result_from_raw.kind else {
        return ArcFFI::null();
    };

    metadata
        .col_specs
        .get(index_usize)
        .map(|col_spec| ArcFFI::as_ptr(&col_spec.data_type))
        .unwrap_or(ArcFFI::null())
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_type(value: CassBorrowedPtr<CassValue>) -> CassValueType {
    value_type(&value)
}

unsafe fn value_type(value: &CassBorrowedPtr<CassValue>) -> CassValueType {
    let value_from_raw = RefFFI::as_ref(value).unwrap();

    cass_data_type_type(ArcFFI::as_ptr(&value_from_raw.value_type))
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_data_type(
    value: CassBorrowedPtr<CassValue>,
) -> CassSharedPtr<CassDataType> {
    let value_from_raw = RefFFI::as_ref(&value).unwrap();

    ArcFFI::as_ptr(&value_from_raw.value_type)
}

macro_rules! val_ptr_to_ref_ensure_non_null {
    ($ptr:ident) => {{
        let maybe_ref = RefFFI::as_ref(&$ptr);
        match maybe_ref {
            Some(r) => r,
            None => return CassError::CASS_ERROR_LIB_NULL_VALUE,
        }
    }};
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_get_float(
    value: CassBorrowedPtr<CassValue>,
    output: *mut cass_float_t,
) -> CassError {
    let val: &CassValue = val_ptr_to_ref_ensure_non_null!(value);

    let f: f32 = match val.get_non_null() {
        Ok(v) => v,
        Err(e) => return e.to_cass_error(),
    };
    std::ptr::write(output, f);

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_get_double(
    value: CassBorrowedPtr<CassValue>,
    output: *mut cass_double_t,
) -> CassError {
    let val: &CassValue = val_ptr_to_ref_ensure_non_null!(value);

    let f: f64 = match val.get_non_null() {
        Ok(v) => v,
        Err(e) => return e.to_cass_error(),
    };
    std::ptr::write(output, f);

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_get_bool(
    value: CassBorrowedPtr<CassValue>,
    output: *mut cass_bool_t,
) -> CassError {
    let val: &CassValue = val_ptr_to_ref_ensure_non_null!(value);

    let b: bool = match val.get_non_null() {
        Ok(v) => v,
        Err(e) => return e.to_cass_error(),
    };
    std::ptr::write(output, b as cass_bool_t);

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_get_int8(
    value: CassBorrowedPtr<CassValue>,
    output: *mut cass_int8_t,
) -> CassError {
    let val: &CassValue = val_ptr_to_ref_ensure_non_null!(value);

    let i: i8 = match val.get_non_null() {
        Ok(v) => v,
        Err(e) => return e.to_cass_error(),
    };
    std::ptr::write(output, i);

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_get_int16(
    value: CassBorrowedPtr<CassValue>,
    output: *mut cass_int16_t,
) -> CassError {
    let val: &CassValue = val_ptr_to_ref_ensure_non_null!(value);

    let i: i16 = match val.get_non_null() {
        Ok(v) => v,
        Err(e) => return e.to_cass_error(),
    };
    std::ptr::write(output, i);

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_get_uint32(
    value: CassBorrowedPtr<CassValue>,
    output: *mut cass_uint32_t,
) -> CassError {
    let val: &CassValue = val_ptr_to_ref_ensure_non_null!(value);

    let date: CqlDate = match val.get_non_null() {
        Ok(v) => v,
        Err(e) => return e.to_cass_error(),
    };
    std::ptr::write(output, date.0);

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_get_int32(
    value: CassBorrowedPtr<CassValue>,
    output: *mut cass_int32_t,
) -> CassError {
    let val: &CassValue = val_ptr_to_ref_ensure_non_null!(value);

    let i: i32 = match val.get_non_null() {
        Ok(v) => v,
        Err(e) => return e.to_cass_error(),
    };
    std::ptr::write(output, i);

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_get_int64(
    value: CassBorrowedPtr<CassValue>,
    output: *mut cass_int64_t,
) -> CassError {
    let val: &CassValue = val_ptr_to_ref_ensure_non_null!(value);

    let i: i64 = match val.value.typ {
        ColumnType::BigInt => match val.get_non_null::<i64>() {
            Ok(v) => v,
            Err(NonNullDeserializationError::Typecheck(_)) => {
                panic!("The typecheck unexpectedly failed!")
            }
            Err(e) => return e.to_cass_error(),
        },
        ColumnType::Counter => match val.get_non_null::<Counter>() {
            Ok(v) => v.0,
            Err(NonNullDeserializationError::Typecheck(_)) => {
                panic!("The typecheck unexpectedly failed!")
            }
            Err(e) => return e.to_cass_error(),
        },
        ColumnType::Time => match val.get_non_null::<CqlTime>() {
            Ok(v) => v.0,
            Err(NonNullDeserializationError::Typecheck(_)) => {
                panic!("The typecheck unexpectedly failed!")
            }
            Err(e) => return e.to_cass_error(),
        },
        ColumnType::Timestamp => match val.get_non_null::<CqlTimestamp>() {
            Ok(v) => v.0,
            Err(NonNullDeserializationError::Typecheck(_)) => {
                panic!("The typecheck unexpectedly failed!")
            }
            Err(e) => return e.to_cass_error(),
        },
        _ => return CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
    };

    std::ptr::write(output, i);

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_get_uuid(
    value: CassBorrowedPtr<CassValue>,
    output: *mut CassUuid,
) -> CassError {
    let val: &CassValue = val_ptr_to_ref_ensure_non_null!(value);

    let uuid: Uuid = match val.value.typ {
        ColumnType::Uuid => match val.get_non_null::<Uuid>() {
            Ok(v) => v,
            Err(NonNullDeserializationError::Typecheck(_)) => {
                panic!("The typecheck unexpectedly failed!")
            }
            Err(e) => return e.to_cass_error(),
        },
        ColumnType::Timeuuid => match val.get_non_null::<CqlTimeuuid>() {
            Ok(v) => v.into(),
            Err(NonNullDeserializationError::Typecheck(_)) => {
                panic!("The typecheck unexpectedly failed!")
            }
            Err(e) => return e.to_cass_error(),
        },
        _ => return CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
    };

    std::ptr::write(output, uuid.into());

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_get_inet(
    value: CassBorrowedPtr<CassValue>,
    output: *mut CassInet,
) -> CassError {
    let val: &CassValue = val_ptr_to_ref_ensure_non_null!(value);

    let inet: IpAddr = match val.get_non_null() {
        Ok(v) => v,
        Err(e) => return e.to_cass_error(),
    };
    std::ptr::write(output, inet.into());

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_get_decimal(
    value: CassBorrowedPtr<CassValue>,
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
    std::ptr::write(varint_size, varint_value.len() as size_t);
    std::ptr::write(varint, varint_value.as_ptr());
    std::ptr::write(scale, scale_value);

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_get_string(
    value: CassBorrowedPtr<CassValue>,
    output: *mut *const c_char,
    output_size: *mut size_t,
) -> CassError {
    let val: &CassValue = val_ptr_to_ref_ensure_non_null!(value);

    // It seems that cpp driver doesn't check the type - you can call _get_string
    // on any type and get internal represenation. I don't see how to do it easily in
    // a compatible way in rust, so let's do something sensible - only return result
    // for string values.
    let s = match val.value.typ {
        ColumnType::Ascii | ColumnType::Text => match val.get_non_null::<&str>() {
            Ok(v) => v,
            Err(NonNullDeserializationError::Typecheck(_)) => {
                panic!("The typecheck unexpectedly failed!")
            }
            Err(e) => return e.to_cass_error(),
        },
        _ => return CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
    };

    write_str_to_c(s, output, output_size);

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_get_duration(
    value: CassBorrowedPtr<CassValue>,
    months: *mut cass_int32_t,
    days: *mut cass_int32_t,
    nanos: *mut cass_int64_t,
) -> CassError {
    let val: &CassValue = val_ptr_to_ref_ensure_non_null!(value);

    let duration: CqlDuration = match val.get_non_null() {
        Ok(v) => v,
        Err(e) => return e.to_cass_error(),
    };
    std::ptr::write(months, duration.months);
    std::ptr::write(days, duration.days);
    std::ptr::write(nanos, duration.nanoseconds);

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_get_bytes(
    value: CassBorrowedPtr<CassValue>,
    output: *mut *const cass_byte_t,
    output_size: *mut size_t,
) -> CassError {
    let value_from_raw: &CassValue = val_ptr_to_ref_ensure_non_null!(value);

    let bytes = match value_from_raw.get_bytes_non_null() {
        Ok(s) => s,
        Err(e) => return e.to_cass_error(),
    };

    std::ptr::write(output, bytes.as_ptr());
    std::ptr::write(output_size, bytes.len() as size_t);

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_is_null(value: CassBorrowedPtr<CassValue>) -> cass_bool_t {
    let val: &CassValue = RefFFI::as_ref(&value).unwrap();
    val.value.slice.is_none() as cass_bool_t
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_is_collection(
    value: CassBorrowedPtr<CassValue>,
) -> cass_bool_t {
    value_is_collection(&value)
}

unsafe fn value_is_collection(value: &CassBorrowedPtr<CassValue>) -> cass_bool_t {
    let val = RefFFI::as_ref(value).unwrap();

    matches!(
        val.value_type.get_unchecked().get_value_type(),
        CassValueType::CASS_VALUE_TYPE_LIST
            | CassValueType::CASS_VALUE_TYPE_SET
            | CassValueType::CASS_VALUE_TYPE_MAP
    ) as cass_bool_t
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_is_duration(value: CassBorrowedPtr<CassValue>) -> cass_bool_t {
    let val = RefFFI::as_ref(&value).unwrap();

    (val.value_type.get_unchecked().get_value_type() == CassValueType::CASS_VALUE_TYPE_DURATION)
        as cass_bool_t
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_item_count(collection: CassBorrowedPtr<CassValue>) -> size_t {
    value_item_count(&collection)
}

unsafe fn value_item_count(collection: &CassBorrowedPtr<CassValue>) -> size_t {
    let val = RefFFI::as_ref(collection).unwrap();

    match &val.value {
        Some(Value::CollectionValue(Collection::List(list))) => list.len() as size_t,
        Some(Value::CollectionValue(Collection::Map(map))) => map.len() as size_t,
        Some(Value::CollectionValue(Collection::Set(set))) => set.len() as size_t,
        Some(Value::CollectionValue(Collection::Tuple(tuple))) => tuple.len() as size_t,
        Some(Value::CollectionValue(Collection::UserDefinedType { fields, .. })) => {
            fields.len() as size_t
        }
        _ => 0 as size_t,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_primary_sub_type(
    collection: CassBorrowedPtr<CassValue>,
) -> CassValueType {
    let val = RefFFI::as_ref(&collection).unwrap();

    match val.value_type.get_unchecked() {
        CassDataTypeInner::List {
            typ: Some(list), ..
        } => list.get_unchecked().get_value_type(),
        CassDataTypeInner::Set { typ: Some(set), .. } => set.get_unchecked().get_value_type(),
        CassDataTypeInner::Map {
            typ: MapDataType::Key(key) | MapDataType::KeyAndValue(key, _),
            ..
        } => key.get_unchecked().get_value_type(),
        _ => CassValueType::CASS_VALUE_TYPE_UNKNOWN,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_secondary_sub_type(
    collection: CassBorrowedPtr<CassValue>,
) -> CassValueType {
    let val = RefFFI::as_ref(&collection).unwrap();

    match val.value_type.get_unchecked() {
        CassDataTypeInner::Map {
            typ: MapDataType::KeyAndValue(_, value),
            ..
        } => value.get_unchecked().get_value_type(),
        _ => CassValueType::CASS_VALUE_TYPE_UNKNOWN,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_result_row_count(result_raw: CassSharedPtr<CassResult>) -> size_t {
    let result = ArcFFI::as_ref(&result_raw).unwrap();

    let CassResultKind::Rows(CassRowsResult { raw_rows, .. }) = &result.kind else {
        return 0;
    };

    raw_rows.rows_count() as size_t
}

#[no_mangle]
pub unsafe extern "C" fn cass_result_column_count(result_raw: CassSharedPtr<CassResult>) -> size_t {
    let result = ArcFFI::as_ref(&result_raw).unwrap();

    let CassResultKind::Rows(CassRowsResult { metadata, .. }) = &result.kind else {
        return 0;
    };

    metadata.col_specs.len() as size_t
}

#[no_mangle]
pub unsafe extern "C" fn cass_result_first_row(
    result_raw: CassSharedPtr<CassResult>,
) -> CassBorrowedPtr<CassRow<'static>> {
    let result = ArcFFI::as_ref(&result_raw).unwrap();

    let CassResultKind::Rows(CassRowsResult { first_row, .. }) = &result.kind else {
        return RefFFI::null();
    };

    first_row
        .as_ref()
        .map(RefFFI::as_ptr)
        .unwrap_or(RefFFI::null())
}

#[no_mangle]
pub unsafe extern "C" fn cass_result_paging_state_token(
    result: CassSharedPtr<CassResult>,
    paging_state: *mut *const c_char,
    paging_state_size: *mut size_t,
) -> CassError {
    if result_has_more_pages(&result) == cass_false {
        return CassError::CASS_ERROR_LIB_NO_PAGING_STATE;
    }

    let result_from_raw = ArcFFI::as_ref(&result).unwrap();

    match &result_from_raw.paging_state_response {
        PagingStateResponse::HasMorePages { state } => match state.as_bytes_slice() {
            Some(result_paging_state) => {
                *paging_state_size = result_paging_state.len() as u64;
                *paging_state = result_paging_state.as_ptr() as *const c_char;
            }
            None => {
                *paging_state_size = 0;
                *paging_state = std::ptr::null();
            }
        },
        PagingStateResponse::NoMorePages => {
            *paging_state_size = 0;
            *paging_state = std::ptr::null();
        }
    }

    CassError::CASS_OK
}

#[cfg(test)]
mod tests {
    use std::{ffi::c_char, ptr::addr_of_mut, sync::Arc};

    use scylla::{
        frame::response::result::{
            ColumnSpec, ColumnType, CqlValue, DeserializedMetadataAndRawRows, Row, TableSpec,
        },
        transport::PagingStateResponse,
    };

    use crate::{
        argconv::{ArcFFI, RefFFI},
        cass_error::CassError,
        cass_types::{CassDataType, CassDataTypeInner, CassValueType},
        query_result::{
            cass_result_column_data_type, cass_result_column_name, cass_result_first_row,
            ptr_to_cstr_n, size_t,
        },
    };

    use super::{
        cass_result_column_count, cass_result_column_type, CassResult, CassResultKind,
        CassResultMetadata, CassRow, CassRowsResult, CassSharedPtr,
    };

    fn col_spec(name: &'static str, typ: ColumnType<'static>) -> ColumnSpec<'static> {
        ColumnSpec::borrowed(name, typ, TableSpec::borrowed("ks", "tbl"))
    }

    const FIRST_COLUMN_NAME: &str = "bigint_col";
    const SECOND_COLUMN_NAME: &str = "varint_col";
    const THIRD_COLUMN_NAME: &str = "list_double_col";
    fn create_cass_rows_result() -> CassResult {
        let metadata = Arc::new(CassResultMetadata::from_column_specs(&[
            col_spec(FIRST_COLUMN_NAME, ColumnType::BigInt),
            col_spec(SECOND_COLUMN_NAME, ColumnType::Varint),
            col_spec(
                THIRD_COLUMN_NAME,
                ColumnType::List(Box::new(ColumnType::Double)),
            ),
        ]));

        let first_row = unsafe {
            Some(std::mem::transmute::<CassRow<'_>, CassRow<'static>>(
                CassRow::from_row_and_metadata(
                    Row {
                        columns: vec![
                            Some(CqlValue::BigInt(42)),
                            None,
                            Some(CqlValue::List(vec![
                                CqlValue::Float(0.5),
                                CqlValue::Float(42.42),
                                CqlValue::Float(9999.9999),
                            ])),
                        ],
                    },
                    &metadata,
                ),
            ))
        };

        CassResult {
            tracing_id: None,
            paging_state_response: PagingStateResponse::NoMorePages,
            kind: CassResultKind::Rows(CassRowsResult {
                raw_rows: DeserializedMetadataAndRawRows::mock_empty(),
                first_row,
                metadata,
            }),
        }
    }

    unsafe fn cass_result_column_name_rust_str(
        result_ptr: CassSharedPtr<CassResult>,
        column_index: u64,
    ) -> Option<&'static str> {
        let mut name_ptr: *const c_char = std::ptr::null();
        let mut name_length: size_t = 0;
        let cass_err = cass_result_column_name(
            result_ptr,
            column_index,
            addr_of_mut!(name_ptr),
            addr_of_mut!(name_length),
        );
        assert_eq!(CassError::CASS_OK, cass_err);
        ptr_to_cstr_n(name_ptr, name_length)
    }

    #[test]
    fn rows_cass_result_api_test() {
        let result = Arc::new(create_cass_rows_result());

        unsafe {
            let result_ptr = ArcFFI::as_ptr(&result);

            // cass_result_column_count test
            {
                let column_count = cass_result_column_count(result_ptr.clone());
                assert_eq!(3, column_count);
            }

            // cass_result_column_name test
            {
                let first_column_name =
                    cass_result_column_name_rust_str(result_ptr.clone(), 0).unwrap();
                assert_eq!(FIRST_COLUMN_NAME, first_column_name);
                let second_column_name =
                    cass_result_column_name_rust_str(result_ptr.clone(), 1).unwrap();
                assert_eq!(SECOND_COLUMN_NAME, second_column_name);
                let third_column_name =
                    cass_result_column_name_rust_str(result_ptr.clone(), 2).unwrap();
                assert_eq!(THIRD_COLUMN_NAME, third_column_name);
            }

            // cass_result_column_type test
            {
                let first_col_type = cass_result_column_type(result_ptr.clone(), 0);
                assert_eq!(CassValueType::CASS_VALUE_TYPE_BIGINT, first_col_type);
                let second_col_type = cass_result_column_type(result_ptr.clone(), 1);
                assert_eq!(CassValueType::CASS_VALUE_TYPE_VARINT, second_col_type);
                let third_col_type = cass_result_column_type(result_ptr.clone(), 2);
                assert_eq!(CassValueType::CASS_VALUE_TYPE_LIST, third_col_type);
                let out_of_bound_col_type = cass_result_column_type(result_ptr.clone(), 555);
                assert_eq!(
                    CassValueType::CASS_VALUE_TYPE_UNKNOWN,
                    out_of_bound_col_type
                );
            }

            // cass_result_column_data_type test
            {
                let first_col_data_type_ptr = cass_result_column_data_type(result_ptr.clone(), 0);
                let first_col_data_type = ArcFFI::as_ref(&first_col_data_type_ptr).unwrap();
                assert_eq!(
                    &CassDataType::new(CassDataTypeInner::Value(
                        CassValueType::CASS_VALUE_TYPE_BIGINT
                    )),
                    first_col_data_type
                );
                let second_col_data_type_ptr = cass_result_column_data_type(result_ptr.clone(), 1);
                let second_col_data_type = ArcFFI::as_ref(&second_col_data_type_ptr).unwrap();
                assert_eq!(
                    &CassDataType::new(CassDataTypeInner::Value(
                        CassValueType::CASS_VALUE_TYPE_VARINT
                    )),
                    second_col_data_type
                );
                let third_col_data_type_ptr = cass_result_column_data_type(result_ptr.clone(), 2);
                let third_col_data_type = ArcFFI::as_ref(&third_col_data_type_ptr).unwrap();
                assert_eq!(
                    &CassDataType::new(CassDataTypeInner::List {
                        typ: Some(CassDataType::new_arced(CassDataTypeInner::Value(
                            CassValueType::CASS_VALUE_TYPE_DOUBLE
                        ))),
                        frozen: false
                    }),
                    third_col_data_type
                );
                let out_of_bound_col_data_type = cass_result_column_data_type(result_ptr, 555);
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

            assert_eq!(0, cass_result_column_count(result_ptr.clone()));
            assert_eq!(
                CassValueType::CASS_VALUE_TYPE_UNKNOWN,
                cass_result_column_type(result_ptr.clone(), 0)
            );
            assert!(ArcFFI::is_null(&cass_result_column_data_type(
                result_ptr.clone(),
                0
            )));
            assert!(RefFFI::is_null(&cass_result_first_row(result_ptr.clone())));

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

// CassResult functions:
/*
extern "C" {
    pub fn cass_statement_set_paging_state(
        statement: *mut CassStatement,
        result: CassSharedPtr<CassResult>,
    ) -> CassError;
}
extern "C" {
    pub fn cass_result_row_count(result: CassSharedPtr<CassResult>) -> size_t;
}
extern "C" {
    pub fn cass_result_column_count(result: CassSharedPtr<CassResult>) -> size_t;
}
extern "C" {
    pub fn cass_result_column_name(
        result: CassSharedPtr<CassResult>,
        index: size_t,
        name: *mut *const ::std::os::raw::c_char,
        name_length: *mut size_t,
    ) -> CassError;
}
extern "C" {
    pub fn cass_result_column_type(result: CassSharedPtr<CassResult>, index: size_t) -> CassValueType;
}
extern "C" {
    pub fn cass_result_column_data_type(
        result: CassSharedPtr<CassResult>,
        index: size_t,
    ) -> *const CassDataType;
}
extern "C" {
    pub fn cass_result_first_row(result: CassSharedPtr<CassResult>) -> CassBorrowedPtr<CassRow>;
}
extern "C" {
    pub fn cass_result_has_more_pages(result: CassSharedPtr<CassResult>) -> cass_bool_t;
}
extern "C" {
    pub fn cass_result_paging_state_token(
        result: CassSharedPtr<CassResult>,
        paging_state: *mut *const ::std::os::raw::c_char,
        paging_state_size: *mut size_t,
    ) -> CassError;
}
*/

// CassIterator functions:
/*
extern "C" {
    pub fn cass_iterator_type(iterator: CassExclusiveMutPtr<CassIterator>) -> CassIteratorType;
}

extern "C" {
    pub fn cass_iterator_from_row(row: CassBorrowedPtr<CassRow>) -> CassExclusiveMutPtr<CassIterator>;
}
extern "C" {
    pub fn cass_iterator_from_collection(value: CassBorrowedPtr<CassValue>) -> CassExclusiveMutPtr<CassIterator>;
}
extern "C" {
    pub fn cass_iterator_from_map(value: CassBorrowedPtr<CassValue>) -> CassExclusiveMutPtr<CassIterator>;
}
extern "C" {
    pub fn cass_iterator_from_tuple(value: CassBorrowedPtr<CassValue>) -> CassExclusiveMutPtr<CassIterator>;
}
extern "C" {
    pub fn cass_iterator_fields_from_user_type(value: CassBorrowedPtr<CassValue>) -> CassExclusiveMutPtr<CassIterator>;
}
extern "C" {
    pub fn cass_iterator_keyspaces_from_schema_meta(
        schema_meta: *const CassSchemaMeta,
    ) -> CassExclusiveMutPtr<CassIterator>;
}
extern "C" {
    pub fn cass_iterator_tables_from_keyspace_meta(
        keyspace_meta: *const CassKeyspaceMeta,
    ) -> CassExclusiveMutPtr<CassIterator>;
}
extern "C" {
    pub fn cass_iterator_materialized_views_from_keyspace_meta(
        keyspace_meta: *const CassKeyspaceMeta,
    ) -> CassExclusiveMutPtr<CassIterator>;
}
extern "C" {
    pub fn cass_iterator_user_types_from_keyspace_meta(
        keyspace_meta: *const CassKeyspaceMeta,
    ) -> CassExclusiveMutPtr<CassIterator>;
}
extern "C" {
    pub fn cass_iterator_functions_from_keyspace_meta(
        keyspace_meta: *const CassKeyspaceMeta,
    ) -> CassExclusiveMutPtr<CassIterator>;
}
extern "C" {
    pub fn cass_iterator_aggregates_from_keyspace_meta(
        keyspace_meta: *const CassKeyspaceMeta,
    ) -> CassExclusiveMutPtr<CassIterator>;
}
extern "C" {
    pub fn cass_iterator_fields_from_keyspace_meta(
        keyspace_meta: *const CassKeyspaceMeta,
    ) -> CassExclusiveMutPtr<CassIterator>;
}
extern "C" {
    pub fn cass_iterator_columns_from_table_meta(
        table_meta: *const CassTableMeta,
    ) -> CassExclusiveMutPtr<CassIterator>;
}
extern "C" {
    pub fn cass_iterator_indexes_from_table_meta(
        table_meta: *const CassTableMeta,
    ) -> CassExclusiveMutPtr<CassIterator>;
}
extern "C" {
    pub fn cass_iterator_materialized_views_from_table_meta(
        table_meta: *const CassTableMeta,
    ) -> CassExclusiveMutPtr<CassIterator>;
}
extern "C" {
    pub fn cass_iterator_fields_from_table_meta(
        table_meta: *const CassTableMeta,
    ) -> CassExclusiveMutPtr<CassIterator>;
}
extern "C" {
    pub fn cass_iterator_columns_from_materialized_view_meta(
        view_meta: *const CassMaterializedViewMeta,
    ) -> CassExclusiveMutPtr<CassIterator>;
}
extern "C" {
    pub fn cass_iterator_fields_from_materialized_view_meta(
        view_meta: *const CassMaterializedViewMeta,
    ) -> CassExclusiveMutPtr<CassIterator>;
}
extern "C" {
    pub fn cass_iterator_fields_from_column_meta(
        column_meta: *const CassColumnMeta,
    ) -> CassExclusiveMutPtr<CassIterator>;
}
extern "C" {
    pub fn cass_iterator_fields_from_index_meta(
        index_meta: *const CassIndexMeta,
    ) -> CassExclusiveMutPtr<CassIterator>;
}
extern "C" {
    pub fn cass_iterator_fields_from_function_meta(
        function_meta: *const CassFunctionMeta,
    ) -> CassExclusiveMutPtr<CassIterator>;
}
extern "C" {
    pub fn cass_iterator_fields_from_aggregate_meta(
        aggregate_meta: *const CassAggregateMeta,
    ) -> CassExclusiveMutPtr<CassIterator>;
}
extern "C" {
    pub fn cass_iterator_get_column(iterator: *const CassIterator) -> CassBorrowedPtr<CassValue>;
}
extern "C" {
    pub fn cass_iterator_get_value(iterator: *const CassIterator) -> CassBorrowedPtr<CassValue>;
}
extern "C" {
    pub fn cass_iterator_get_map_key(iterator: *const CassIterator) -> CassBorrowedPtr<CassValue>;
}
extern "C" {
    pub fn cass_iterator_get_map_value(iterator: *const CassIterator) -> CassBorrowedPtr<CassValue>;
}
extern "C" {
    pub fn cass_iterator_get_user_type_field_name(
        iterator: *const CassIterator,
        name: *mut *const ::std::os::raw::c_char,
        name_length: *mut size_t,
    ) -> CassError;
}
extern "C" {
    pub fn cass_iterator_get_user_type_field_value(
        iterator: *const CassIterator,
    ) -> CassBorrowedPtr<CassValue>;
}
extern "C" {
    pub fn cass_iterator_get_keyspace_meta(
        iterator: *const CassIterator,
    ) -> *const CassKeyspaceMeta;
}
extern "C" {
    pub fn cass_iterator_get_table_meta(iterator: *const CassIterator) -> *const CassTableMeta;
}
extern "C" {
    pub fn cass_iterator_get_materialized_view_meta(
        iterator: *const CassIterator,
    ) -> *const CassMaterializedViewMeta;
}
extern "C" {
    pub fn cass_iterator_get_user_type(iterator: *const CassIterator) -> *const CassDataType;
}
extern "C" {
    pub fn cass_iterator_get_function_meta(
        iterator: *const CassIterator,
    ) -> *const CassFunctionMeta;
}
extern "C" {
    pub fn cass_iterator_get_aggregate_meta(
        iterator: *const CassIterator,
    ) -> *const CassAggregateMeta;
}
extern "C" {
    pub fn cass_iterator_get_column_meta(iterator: *const CassIterator) -> *const CassColumnMeta;
}
extern "C" {
    pub fn cass_iterator_get_index_meta(iterator: *const CassIterator) -> *const CassIndexMeta;
}
extern "C" {
    pub fn cass_iterator_get_meta_field_name(
        iterator: *const CassIterator,
        name: *mut *const ::std::os::raw::c_char,
        name_length: *mut size_t,
    ) -> CassError;
}
extern "C" {
    pub fn cass_iterator_get_meta_field_value(iterator: *const CassIterator) -> CassBorrowedPtr<CassValue>;
}
*/

// CassRow functions:
/*
extern "C" {
    pub fn cass_row_get_column_by_name(
        row: CassBorrowedPtr<CassRow>,
        name: *const ::std::os::raw::c_char,
    ) -> CassBorrowedPtr<CassValue>;
}
extern "C" {
    pub fn cass_row_get_column_by_name_n(
        row: CassBorrowedPtr<CassRow>,
        name: *const ::std::os::raw::c_char,
        name_length: size_t,
    ) -> CassBorrowedPtr<CassValue>;
}
*/

// CassValue functions:
/*
#[no_mangle]
pub unsafe extern "C" fn cass_value_get_bytes(
    value: CassBorrowedPtr<CassValue>,
    output: *mut *const cass_byte_t,
    output_size: *mut size_t,
) -> CassError {
}
extern "C" {
    pub fn cass_value_data_type(value: CassBorrowedPtr<CassValue>) -> *const CassDataType;
}
extern "C" {
    pub fn cass_value_type(value: CassBorrowedPtr<CassValue>) -> CassValueType;
}
extern "C" {
    pub fn cass_value_item_count(collection: CassBorrowedPtr<CassValue>) -> size_t;
}
extern "C" {
    pub fn cass_value_primary_sub_type(collection: CassBorrowedPtr<CassValue>) -> CassValueType;
}
extern "C" {
    pub fn cass_value_secondary_sub_type(collection: CassBorrowedPtr<CassValue>) -> CassValueType;
}
*/
