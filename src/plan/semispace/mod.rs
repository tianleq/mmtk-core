//! Plan: semispace

pub(super) mod gc_work;
pub(super) mod global;
pub(super) mod mutator;

// use crate::util::ObjectReference;

pub use self::global::SemiSpace;
pub use self::global::SS_CONSTRAINTS;

// lazy_static! {
//     pub static ref NON_LOCAL_OBJECTS: std::sync::Mutex<std::collections::HashSet<ObjectReference>> =
//         std::sync::Mutex::new(std::collections::HashSet::<ObjectReference>::new());
// }
