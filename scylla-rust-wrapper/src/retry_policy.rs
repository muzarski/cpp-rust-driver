use scylla::policies::retry::{
    DefaultRetryPolicy, DowngradingConsistencyRetryPolicy, FallthroughRetryPolicy,
};
use std::sync::Arc;

use crate::argconv::{ArcFFI, CMut, CassOwnedConstPtr, FromArc, FFI};

pub enum RetryPolicy {
    DefaultRetryPolicy(Arc<DefaultRetryPolicy>),
    FallthroughRetryPolicy(Arc<FallthroughRetryPolicy>),
    DowngradingConsistencyRetryPolicy(Arc<DowngradingConsistencyRetryPolicy>),
}

pub type CassRetryPolicy = RetryPolicy;

impl FFI for CassRetryPolicy {
    type Origin = FromArc;
}

#[no_mangle]
pub extern "C" fn cass_retry_policy_default_new() -> CassOwnedConstPtr<CassRetryPolicy, CMut> {
    ArcFFI::into_ptr(Arc::new(RetryPolicy::DefaultRetryPolicy(Arc::new(
        DefaultRetryPolicy,
    ))))
}

#[no_mangle]
pub extern "C" fn cass_retry_policy_downgrading_consistency_new(
) -> CassOwnedConstPtr<CassRetryPolicy, CMut> {
    ArcFFI::into_ptr(Arc::new(RetryPolicy::DowngradingConsistencyRetryPolicy(
        Arc::new(DowngradingConsistencyRetryPolicy),
    )))
}

#[no_mangle]
pub extern "C" fn cass_retry_policy_fallthrough_new() -> CassOwnedConstPtr<CassRetryPolicy, CMut> {
    ArcFFI::into_ptr(Arc::new(RetryPolicy::FallthroughRetryPolicy(Arc::new(
        FallthroughRetryPolicy,
    ))))
}

#[no_mangle]
pub unsafe extern "C" fn cass_retry_policy_free(
    retry_policy: CassOwnedConstPtr<CassRetryPolicy, CMut>,
) {
    ArcFFI::free(retry_policy);
}
