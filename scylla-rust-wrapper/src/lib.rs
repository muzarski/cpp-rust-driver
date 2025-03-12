#![allow(clippy::missing_safety_doc)]

use crate::logging::stderr_log_callback;
use crate::logging::Logger;
use std::sync::LazyLock;
use std::sync::RwLock;
use tokio::runtime::Runtime;

#[macro_use]
mod binding;
mod argconv;
pub mod batch;
pub mod cass_error;
pub mod cass_types;
pub mod cluster;
pub mod collection;
pub mod date_time;
pub mod exec_profile;
mod external;
pub mod future;
pub mod inet;
pub mod integration_testing;
pub mod iterator;
mod logging;
pub mod metadata;
pub mod misc;
pub mod prepared;
pub mod query_error;
pub mod query_result;
pub mod retry_policy;
pub mod session;
pub mod ssl;
pub mod statement;
#[cfg(test)]
pub mod testing;
pub mod tuple;
pub mod user_type;
pub mod uuid;
pub mod value;

/// Includes a file generated by bindgen called `filename`.
macro_rules! include_bindgen_generated {
    ($filename:expr) => {
        include!(concat!(env!("OUT_DIR"), '/', $filename));
    };
}

/// All numeric types are defined here.
pub mod types {
    #![allow(non_camel_case_types)]
    // for `cass_false` and `cass_true` globals.
    #![allow(non_upper_case_globals)]

    // Definition for size_t (and possibly other types in the future)
    include_bindgen_generated!("basic_types.rs");
}

/// CassError, CassErrorSource, CassWriteType
pub mod cass_error_types {
    include_bindgen_generated!("cppdriver_error_types.rs");
}

/// CassValueType
pub mod cass_data_types {
    include_bindgen_generated!("cppdriver_data_types.rs");
}

/// CassConsistency
pub mod cass_consistency_types {
    include_bindgen_generated!("cppdriver_consistency_types.rs");
}

/// CassBatchType
pub mod cass_batch_types {
    include_bindgen_generated!("cppdriver_batch_types.rs");
}

/// CassCompressionType
pub mod cass_compression_types {
    include_bindgen_generated!("cppdriver_compression_types.rs");
}

/// CassCollectionType
pub mod cass_collection_types {
    include_bindgen_generated!("cppdriver_collection_types.rs");
}

/// CassInet
pub mod cass_inet_types {
    #![allow(non_camel_case_types, non_snake_case)]

    include_bindgen_generated!("cppdriver_inet_types.rs");
}

/// CassLogLevel, CassLogMessage
pub mod cass_log_types {
    #![allow(non_camel_case_types, non_snake_case)]

    include_bindgen_generated!("cppdriver_log_types.rs");
}

/// CassColumnType
pub mod cass_column_types {
    include_bindgen_generated!("cppdriver_column_type.rs");
}

/// CassUuid
pub mod cass_uuid_types {
    #![allow(non_camel_case_types, non_snake_case)]

    include_bindgen_generated!("cppdriver_uuid_types.rs");
}

/// CassIteratorType
pub mod cass_iterator_types {
    include_bindgen_generated!("cppdriver_iterator_types.rs");
}

pub static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| Runtime::new().unwrap());
pub static LOGGER: LazyLock<RwLock<Logger>> = LazyLock::new(|| {
    RwLock::new(Logger {
        cb: Some(stderr_log_callback),
        data: std::ptr::null_mut(),
    })
});

// To send a Rust object to C:

// #[no_mangle]
// pub extern "C" fn create_foo() -> *mut Foo {
//     BoxFFI::into_raw(Box::new(Foo))
// }

// To borrow (and not free) from C:

// #[no_mangle]
// pub unsafe extern "C" fn do(foo: *mut Foo) -> *mut Foo {
//     let foo = argconv::ptr_to_ref(foo);
// }

// To take over/destroy Rust object previously given to C:

// #[no_mangle]
// pub unsafe extern "C" fn free_foo(foo: *mut Foo) {
//     // Take the ownership of the value and it will be automatically dropped
//     argconv::ptr_to_opt_box(foo);
// }
