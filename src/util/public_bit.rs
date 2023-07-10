use crate::util::metadata::side_metadata::SideMetadataSpec;
use crate::util::Address;
use crate::util::ObjectReference;
use crate::vm::VMBinding;
use atomic::Ordering;

/// An public-bit is required per min-object-size aligned address , rather than per object, and can only exist as side metadata.
pub(crate) const PUBLIC_SIDE_METADATA_SPEC: SideMetadataSpec =
    crate::util::metadata::side_metadata::spec_defs::PUBLIC_BIT;

pub const PUBLIC_SIDE_METADATA_ADDR: Address = PUBLIC_SIDE_METADATA_SPEC.get_absolute_offset();

pub fn set_public_bit<VM: VMBinding>(object: ObjectReference) {
    debug_assert!(
        !is_public::<VM>(object),
        "{:x}: public bit already set",
        object,
    );
    PUBLIC_SIDE_METADATA_SPEC.store_atomic::<u8>(object.to_address::<VM>(), 1, Ordering::SeqCst);
}

pub fn unset_addr_public_bit<VM: VMBinding>(address: Address) {
    PUBLIC_SIDE_METADATA_SPEC.store_atomic::<u8>(address, 0, Ordering::SeqCst);
}

pub fn unset_public_bit<VM: VMBinding>(object: ObjectReference) {
    PUBLIC_SIDE_METADATA_SPEC.store_atomic::<u8>(object.to_address::<VM>(), 0, Ordering::SeqCst);
}

pub fn is_public<VM: VMBinding>(object: ObjectReference) -> bool {
    is_public_object(object.to_address::<VM>())
}

pub fn is_public_object(address: Address) -> bool {
    PUBLIC_SIDE_METADATA_SPEC.load_atomic::<u8>(address, Ordering::SeqCst) == 1
}

/// # Safety
///
/// This is unsafe: check the comment on `side_metadata::load`
///
pub unsafe fn is_public_object_unsafe(address: Address) -> bool {
    PUBLIC_SIDE_METADATA_SPEC.load::<u8>(address) == 1
}

pub fn bzero_public_bit(start: Address, size: usize) {
    PUBLIC_SIDE_METADATA_SPEC.bzero_metadata(start, size);
}
