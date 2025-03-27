use crate::util::metadata::side_metadata::SideMetadataSpec;
use crate::util::Address;
use crate::util::ObjectReference;
use atomic::Ordering;

/// An public-bit is required per min-object-size aligned address , rather than per object, and can only exist as side metadata.
pub(crate) const PUBLIC_SIDE_METADATA_SPEC: SideMetadataSpec =
    crate::util::metadata::side_metadata::spec_defs::PUBLIC_BIT;

pub const PUBLIC_SIDE_METADATA_ADDR: Address = PUBLIC_SIDE_METADATA_SPEC.get_absolute_offset();

#[cfg(not(feature = "debug_publish_object"))]
pub fn set_public_bit(object: ObjectReference) {
    debug_assert!(!is_public(object), "{:x}: public bit already set", object,);
    PUBLIC_SIDE_METADATA_SPEC.store_atomic::<u8>(object.to_raw_address(), 1, Ordering::SeqCst);
}

#[cfg(feature = "debug_publish_object")]
pub fn set_public_bit(object: ObjectReference, _mutator_id: Option<u32>) {
    debug_assert!(!is_public(object), "{:x}: public bit already set", object,);
    // info!("mutator: {:?} publish object: {:?}", _mutator_id, object);
    PUBLIC_SIDE_METADATA_SPEC.store_atomic::<u8>(object.to_raw_address(), 1, Ordering::SeqCst);
}

pub fn unset_addr_public_bit(address: Address) {
    PUBLIC_SIDE_METADATA_SPEC.store_atomic::<u8>(address, 0, Ordering::SeqCst);
}

#[cfg(not(feature = "debug_publish_object"))]
pub fn unset_public_bit(object: ObjectReference) {
    PUBLIC_SIDE_METADATA_SPEC.store_atomic::<u8>(object.to_raw_address(), 0, Ordering::SeqCst);
}

#[cfg(feature = "debug_publish_object")]
pub fn unset_public_bit(object: ObjectReference) -> Address {
    let metadata_addr = crate::util::metadata::side_metadata::address_to_meta_address(
        &PUBLIC_SIDE_METADATA_SPEC,
        object.to_raw_address(),
    );

    PUBLIC_SIDE_METADATA_SPEC.store_atomic::<u8>(object.to_raw_address(), 0, Ordering::SeqCst);

    metadata_addr
}
#[cfg(feature = "debug_publish_object")]
pub fn public_bit_metadata_address<VM: VMBinding>(object: ObjectReference) -> Address {
    crate::util::metadata::side_metadata::address_to_meta_address(
        &PUBLIC_SIDE_METADATA_SPEC,
        object.to_raw_address(),
    )
}

pub fn is_public(object: ObjectReference) -> bool {
    is_public_object(object.to_raw_address())
}

pub fn is_public_object(address: Address) -> bool {
    PUBLIC_SIDE_METADATA_SPEC.load_atomic::<u8>(address, Ordering::SeqCst) == 1
}

pub fn bzero_public_bit(start: Address, size: usize) {
    PUBLIC_SIDE_METADATA_SPEC.bzero_metadata(start, size);
}
