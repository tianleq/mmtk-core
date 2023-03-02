use super::{Address, ObjectReference};
use crate::vm::VMBinding;

const OBJECT_OWNER_OFFSET: usize = 4;
const REQUEST_ID_OFFSET: usize = 8;
const METADATA_OFFSET: usize = 8;
pub const OBJECT_OWNER_MASK: usize = 0x00000000FFFFFFFF;
pub const REQUEST_ID_MASK: usize = 0xFFFFFFFF00000000;

fn header_object_owner_address<VM: VMBinding>(object: ObjectReference) -> Address {
    object.to_object_start::<VM>() - OBJECT_OWNER_OFFSET
}

fn header_request_id_address<VM: VMBinding>(object: ObjectReference) -> Address {
    object.to_object_start::<VM>() - REQUEST_ID_OFFSET
}

fn header_metadata_address<VM: VMBinding>(object: ObjectReference) -> Address {
    object.to_object_start::<VM>() - METADATA_OFFSET
}

/// Get header forwarding pointer for an object
pub fn get_header_object_owner<VM: VMBinding>(object: ObjectReference) -> u32 {
    unsafe { header_object_owner_address::<VM>(object).load::<u32>() }
}

pub fn get_header_request_id<VM: VMBinding>(object: ObjectReference) -> u32 {
    unsafe { header_request_id_address::<VM>(object).load::<u32>() }
}

pub fn set_header_object_owner<VM: VMBinding>(object: ObjectReference, owner: u32) {
    unsafe {
        header_object_owner_address::<VM>(object).store::<u32>(owner);
    }
}

pub fn set_header_request_id<VM: VMBinding>(object: ObjectReference, request_id: u32) {
    unsafe {
        header_request_id_address::<VM>(object).store::<u32>(request_id);
    }
}

pub fn get_header_metadata<VM: VMBinding>(object: ObjectReference) -> usize {
    unsafe { header_metadata_address::<VM>(object).load::<usize>() }
}

pub fn set_header_metadata<VM: VMBinding>(object: ObjectReference, metadata: usize) {
    unsafe {
        header_metadata_address::<VM>(object).store::<usize>(metadata);
    }
}
