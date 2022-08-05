use crate::util::metadata::side_metadata;
use crate::util::metadata::side_metadata::SideMetadataSpec;
use crate::util::ObjectReference;
use atomic::Ordering;

use super::Address;

/// An mark-bit is required per min-object-size aligned address , rather than per object, and can only exist as side metadata.
pub(crate) const GLOBAL_MARK_SIDE_METADATA_SPEC: SideMetadataSpec =
    crate::util::metadata::side_metadata::spec_defs::GLOBAL_MARK_BIT;

pub fn set_global_mark_bit(object: ObjectReference) {
    // debug_assert!(!is_global_mark_set(object), "{:x}: mark bit already set", object);
    side_metadata::store_atomic(
        &GLOBAL_MARK_SIDE_METADATA_SPEC,
        object.to_address(),
        1,
        Ordering::SeqCst,
    );
}

pub fn unset_global_mark_bit(object: ObjectReference) {
    // debug_assert!(is_global_mark_set(object), "{:x}: mark bit not set", object);
    side_metadata::store_atomic(
        &GLOBAL_MARK_SIDE_METADATA_SPEC,
        object.to_address(),
        0,
        Ordering::SeqCst,
    );
}

pub fn is_global_mark_set(object: ObjectReference) -> bool {
    side_metadata::load_atomic(
        &GLOBAL_MARK_SIDE_METADATA_SPEC,
        object.to_address(),
        Ordering::SeqCst,
    ) == 1
}

pub fn bzero_mark_bit(start: Address, size: usize) {
    side_metadata::bzero_metadata(&GLOBAL_MARK_SIDE_METADATA_SPEC, start, size);
}
