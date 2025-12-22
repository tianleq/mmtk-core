//! Memory policies that can be used for spaces.

/// This class defines and manages spaces.  Each policy is an instance
/// of a space.  A space is a region of virtual memory (contiguous or
/// discontigous) which is subject to the same memory management
/// regime.  Multiple spaces (instances of this class or its
/// descendants) may have the same policy (eg there could be numerous
/// instances of CopySpace, each with different roles). Spaces are
/// defined in terms of a unique region of virtual memory, so no two
/// space instances ever share any virtual memory.
///
/// In addition to tracking virtual memory use and the mapping to
/// policy, spaces also manage memory consumption (*used* virtual
/// memory).
pub mod space;

/// Copy context defines the thread local copy allocator for copying policies.
pub mod copy_context;
/// Policy specific GC work
pub mod gc_work;
pub mod sft;
pub mod sft_map;

pub mod compressor;
pub mod copyspace;
pub mod immix;
pub mod immortalspace;
pub mod largeobjectspace;
pub mod lockfreeimmortalspace;
pub mod markcompactspace;
pub mod marksweepspace;
#[cfg(feature = "vm_space")]
pub mod vmspace;

// type Mutator = u32;

// #[cfg(feature = "thread_local_gc_copying")]
// lazy_static! {
//     pub(crate) static ref THREAD_LOCAL_HEAP_IN_PAGES: std::sync::Mutex<std::collections::HashMap<Mutator, usize>> =
//         std::sync::Mutex::new(std::collections::HashMap::new());
// }

#[cfg(debug_assertions)]
use std::sync::atomic::AtomicUsize;

#[cfg(debug_assertions)]
use crate::util::{Address, ObjectReference};

#[cfg(debug_assertions)]
lazy_static! {
    // pub(crate) static ref GLOBAL_OBJECT_REMSET: std::sync::Mutex<std::collections::HashSet<ObjectReference>> =
    //     std::sync::Mutex::new(std::collections::HashSet::new());
    // pub(crate) static ref PRIVATE_OBJECTS_IN_PREV_GC: std::sync::Mutex<std::collections::HashSet<ObjectReference>> =
    //     std::sync::Mutex::new(std::collections::HashSet::new());
    // pub(crate) static ref PRIVATE_OBJECTS_IN_CURRENT_GC: std::sync::Mutex<std::collections::HashSet<ObjectReference>> =
    //     std::sync::Mutex::new(std::collections::HashSet::new());
    // pub(crate) static ref RUNTIME_OBJECT: std::sync::Mutex<std::collections::HashSet<ObjectReference>> =
    //     std::sync::Mutex::new(std::collections::HashSet::new());
    pub(crate) static ref GLOBAL_OBJECTS_CONSERVATIVE: std::sync::Mutex<std::collections::HashMap<ObjectReference, ObjectReference>> =
        std::sync::Mutex::new(std::collections::HashMap::new());

    pub(crate) static ref GLOBAL_OBJECTS: std::sync::Mutex<std::collections::HashMap<ObjectReference, ObjectReference>> =
        std::sync::Mutex::new(std::collections::HashMap::new());

    // pub(crate) static ref REMSET_OBJECTS: std::sync::Mutex<std::collections::HashSet<ObjectReference>> =
    //     std::sync::Mutex::new(std::collections::HashSet::new());

    pub(crate) static ref PRIVATE_OBJECTS: std::sync::Mutex<std::collections::HashSet<ObjectReference>> =
        std::sync::Mutex::new(std::collections::HashSet::new());

    pub(crate) static ref STACK_ROOTS: std::sync::Mutex<std::collections::HashSet<Address>> =
        std::sync::Mutex::new(std::collections::HashSet::new());
    pub(crate) static ref STACK_ROOTS_SANITY: std::sync::Mutex<std::collections::HashSet<Address>> =
        std::sync::Mutex::new(std::collections::HashSet::new());

    pub(crate) static ref GLOBAL_ROOTS_COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    pub(crate) static ref GLOBAL_ROOTS_COUNTER_CONSERVATIVE: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

    pub(crate) static ref PAGES_FREED_IN_LOCAL_GC: AtomicUsize = AtomicUsize::new(0);
}
