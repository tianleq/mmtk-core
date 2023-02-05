use super::{Address, ObjectReference};
use crate::vm::ObjectModel;
use crate::vm::VMBinding;

#[inline(always)]
fn header_object_owner_address<VM: VMBinding>(object: ObjectReference) -> Address {
    const EXTRA_HEADER_BYTES: usize = 8;
    <VM as crate::vm::VMBinding>::VMObjectModel::object_start_ref(object) - EXTRA_HEADER_BYTES
}

/// Get header forwarding pointer for an object
#[inline(always)]
pub fn get_header_object_owner<VM: VMBinding>(object: ObjectReference) -> usize {
    unsafe { header_object_owner_address::<VM>(object).load::<usize>() }
}

pub fn set_header_object_owner<VM: VMBinding>(object: ObjectReference, owner: usize) {
    unsafe {
        header_object_owner_address::<VM>(object).store::<usize>(owner);
    }
}
