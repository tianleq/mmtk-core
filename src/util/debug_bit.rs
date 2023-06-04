use crate::util::metadata::side_metadata::SideMetadataSpec;
use crate::util::Address;
use crate::util::ObjectReference;
use crate::vm::VMBinding;
use atomic::Ordering;

/// An debug-bit is required per min-object-size aligned address , rather than per object, and can only exist as side metadata.
pub(crate) const DEBUG_SIDE_METADATA_SPEC: SideMetadataSpec =
    crate::util::metadata::side_metadata::spec_defs::DEBUG_BIT;

pub const DEBUG_SIDE_METADATA_ADDR: Address = DEBUG_SIDE_METADATA_SPEC.get_absolute_offset();

pub fn set_debug_bit<VM: VMBinding>(object: ObjectReference) {
    // debug_assert!(
    //     !is_debug::<VM>(object),
    //     "{:x}: debug bit already set",
    //     object,
    // );
    DEBUG_SIDE_METADATA_SPEC.store_atomic::<u8>(object.to_address::<VM>(), 1, Ordering::SeqCst);
}

pub fn unset_addr_debug_bit<VM: VMBinding>(address: Address) {
    DEBUG_SIDE_METADATA_SPEC.store_atomic::<u8>(address, 0, Ordering::SeqCst);
}

pub fn unset_debug_bit<VM: VMBinding>(object: ObjectReference) {
    DEBUG_SIDE_METADATA_SPEC.store_atomic::<u8>(object.to_address::<VM>(), 0, Ordering::SeqCst);
}

pub fn is_debug<VM: VMBinding>(object: ObjectReference) -> bool {
    is_debug_object(object.to_address::<VM>())
}

pub fn is_debug_object(address: Address) -> bool {
    DEBUG_SIDE_METADATA_SPEC.load_atomic::<u8>(address, Ordering::SeqCst) == 1
}

/// # Safety
///
/// This is unsafe: check the comment on `side_metadata::load`
///
pub unsafe fn is_debug_object_unsafe(address: Address) -> bool {
    DEBUG_SIDE_METADATA_SPEC.load::<u8>(address) == 1
}

pub fn bzero_debug_bit(start: Address, size: usize) {
    DEBUG_SIDE_METADATA_SPEC.bzero_metadata(start, size);
}
