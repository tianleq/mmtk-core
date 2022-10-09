use crate::util::metadata::side_metadata::SideMetadataSpec;
use crate::util::Address;
use crate::util::ObjectReference;
use atomic::Ordering;

/// An alloc-bit is required per min-object-size aligned address , rather than per object, and can only exist as side metadata.
pub(crate) const CRITICAL_SIDE_METADATA_SPEC: SideMetadataSpec =
    crate::util::metadata::side_metadata::spec_defs::CRITICAL_BIT;

pub fn set_critical_bit(object: ObjectReference) {
    debug_assert!(
        !is_alloced_in_critical_section(object),
        "{:x}: alloc bit already set",
        object
    );
    CRITICAL_SIDE_METADATA_SPEC.store_atomic::<u8>(object.to_address(), 1, Ordering::SeqCst);
}

pub fn unset_addr_critical_bit(address: Address) {
    debug_assert!(
        is_alloced_in_critical_section_object(address),
        "{:x}: alloc bit not set",
        address
    );
    CRITICAL_SIDE_METADATA_SPEC.store_atomic::<u8>(address, 0, Ordering::SeqCst);
}

pub fn unset_critical_bit(object: ObjectReference) {
    debug_assert!(
        is_alloced_in_critical_section(object),
        "{:x}: alloc bit not set",
        object
    );
    CRITICAL_SIDE_METADATA_SPEC.store_atomic::<u8>(object.to_address(), 0, Ordering::SeqCst);
}

/// # Safety
///
/// This is unsafe: check the comment on `side_metadata::store`
///
pub unsafe fn unset_critical_bit_unsafe(object: ObjectReference) {
    debug_assert!(
        is_alloced_in_critical_section(object),
        "{:x}: alloc bit not set",
        object
    );
    CRITICAL_SIDE_METADATA_SPEC.store::<u8>(object.to_address(), 0);
}

pub fn is_alloced_in_critical_section(object: ObjectReference) -> bool {
    is_alloced_in_critical_section_object(object.to_address())
}

pub fn is_alloced_in_critical_section_object(address: Address) -> bool {
    CRITICAL_SIDE_METADATA_SPEC.load_atomic::<u8>(address, Ordering::SeqCst) == 1
}

/// # Safety
///
/// This is unsafe: check the comment on `side_metadata::load`
///
pub unsafe fn is_alloced_object_unsafe(address: Address) -> bool {
    CRITICAL_SIDE_METADATA_SPEC.load::<u8>(address) == 1
}

pub fn bzero_critical_bit(start: Address, size: usize) {
    CRITICAL_SIDE_METADATA_SPEC.bzero_metadata(start, size);
}
