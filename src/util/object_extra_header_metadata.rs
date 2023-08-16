#[cfg(feature = "extra_header")]
use super::{metadata::MetadataValue, Address, ObjectReference};
#[cfg(feature = "extra_header")]
use crate::vm::VMBinding;

#[cfg(feature = "extra_header")]
pub const BOTTOM_HALF_MASK: usize = 0x00000000FFFFFFFF;
#[cfg(feature = "extra_header")]
pub const TOP_HALF_MASK: usize = 0xFFFFFFFF00000000;

// The following are a few functions for manipulating extra header metadata.
// Basically for each allocation request, we allocate extra bytes of [`HEADER_RESERVED_IN_BYTES`].
// From the allocation result we get (e.g. `alloc_res`), `alloc_res + HEADER_RESERVED_IN_BYTES` is the cell
// address we return to the binding. It ensures we have at least one word (`GC_EXTRA_HEADER_WORD`) before
// the cell address, and ensures the cell address is properly aligned.
// From the cell address, `cell - GC_EXTRA_HEADER_WORD` is where we store the header forwarding pointer.

/// Get the address for header metadata
#[cfg(feature = "extra_header")]
fn extra_header_metadata_address<VM: VMBinding>(object: ObjectReference) -> Address {
    object.to_object_start::<VM>() - VM::EXTRA_HEADER_BYTES
}

/// Get header forwarding pointer for an object

#[cfg(feature = "extra_header")]
pub fn get_extra_header_metadata<VM: VMBinding, T: MetadataValue>(object: ObjectReference) -> T {
    unsafe { extra_header_metadata_address::<VM>(object).load::<T>() }
}

/// Store header forwarding pointer for an object
#[cfg(feature = "extra_header")]
pub fn store_extra_header_metadata<VM: VMBinding, T: MetadataValue>(
    object: ObjectReference,
    metadata: T,
) {
    unsafe {
        extra_header_metadata_address::<VM>(object).store::<T>(metadata);
    }
}

// Clear header forwarding pointer for an object
#[cfg(feature = "extra_header")]
pub fn clear_extra_header_metadata<VM: VMBinding>(object: ObjectReference) {
    crate::util::memory::zero(
        extra_header_metadata_address::<VM>(object),
        VM::EXTRA_HEADER_BYTES,
    );
}
