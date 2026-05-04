use crate::util::metadata::side_metadata::SideMetadataSpec;
use crate::util::Address;
use atomic::Ordering;

/// An public-bit is required per min-object-size aligned address , rather than per object, and can only exist as side metadata.
pub(crate) const SLOT_SIDE_METADATA_SPEC: SideMetadataSpec =
    crate::util::metadata::side_metadata::spec_defs::SLOT_LOG_BIT;

pub const SLOT_SIDE_METADATA_ADDR: Address = SLOT_SIDE_METADATA_SPEC.get_absolute_offset();

pub fn set_slot_log_bit(address: Address) {
    SLOT_SIDE_METADATA_SPEC.store_atomic::<u8>(address, 1, Ordering::SeqCst);
}

pub fn unset_slot_log_bit(address: Address) {
    SLOT_SIDE_METADATA_SPEC.store_atomic::<u8>(address, 0, Ordering::SeqCst);
}

pub fn is_slot_logged(address: Address) -> bool {
    SLOT_SIDE_METADATA_SPEC.load_atomic::<u8>(address, Ordering::SeqCst) == 1
}

pub fn bzero_slot_log_bit(start: Address, size: usize) {
    SLOT_SIDE_METADATA_SPEC.bzero_metadata(start, size);
}
