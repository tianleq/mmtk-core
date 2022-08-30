use crate::util::metadata::side_metadata;
use crate::util::metadata::side_metadata::SideMetadataSpec;
use crate::util::Address;
use crate::util::ObjectReference;
use atomic::Ordering;

/// An public-bit is required per min-object-size aligned address , rather than per object, and can only exist as side metadata.
pub(crate) const PUBLIC_SIDE_METADATA_SPEC: SideMetadataSpec =
    crate::util::metadata::side_metadata::spec_defs::PUBLIC_BIT;

pub fn set_public_bit(object: ObjectReference, mutator_id: usize, owner: usize, force: bool) {
    if !force {
        assert!(
            !is_public(object),
            "{:x}: public bit already set (mutator_id:{}, owner: {})",
            object,
            mutator_id,
            owner
        );
    }

    side_metadata::store_atomic(
        &PUBLIC_SIDE_METADATA_SPEC,
        object.to_address(),
        1,
        Ordering::SeqCst,
    );
}

// pub fn unset_addr_public_bit(address: Address) {
//     debug_assert!(
//         is_public_object(address),
//         "{:x}: public bit not set",
//         address
//     );
//     side_metadata::store_atomic(&PUBLIC_SIDE_METADATA_SPEC, address, 0, Ordering::SeqCst);
// }

// pub fn unset_public_bit(object: ObjectReference) {
//     debug_assert!(is_public(object), "{:x}: public bit not set", object);
//     side_metadata::store_atomic(
//         &PUBLIC_SIDE_METADATA_SPEC,
//         object.to_address(),
//         0,
//         Ordering::SeqCst,
//     );
// }

/// # Safety
///
/// This is unsafe: check the comment on `side_metadata::store`
///
// pub unsafe fn unset_public_bit_unsafe(object: ObjectReference) {
//     debug_assert!(is_public(object), "{:x}: public bit not set", object);
//     side_metadata::store(&PUBLIC_SIDE_METADATA_SPEC, object.to_address(), 0);
// }

pub fn is_public(object: ObjectReference) -> bool {
    is_public_object(object.to_address())
}

pub fn is_public_object(address: Address) -> bool {
    side_metadata::load_atomic(&PUBLIC_SIDE_METADATA_SPEC, address, Ordering::SeqCst) == 1
}

/// # Safety
///
/// This is unsafe: check the comment on `side_metadata::load`
///
pub unsafe fn is_public_object_unsafe(address: Address) -> bool {
    side_metadata::load(&PUBLIC_SIDE_METADATA_SPEC, address) == 1
}

pub fn bzero_public_bit(start: Address, size: usize) {
    side_metadata::bzero_metadata(&PUBLIC_SIDE_METADATA_SPEC, start, size);
}
