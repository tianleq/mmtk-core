//! Utilities used by other modules, including allocators, heap implementation, etc.

// Allow unused code in the util mod. We may have some functions that are not in use,
// but will be useful in future implementations.
#![allow(dead_code)]

// The following modules are public. MMTk bindings can use them to help implementation.

/// An abstract of memory address and object reference.
pub mod address;
/// Allocators
// This module is made public so the binding could implement allocator slowpaths if they would like to.
pub mod alloc;
/// Constants used in MMTk
pub mod constants;
/// Calculation, conversion and rounding for memory related numbers.
pub mod conversions;
/// The copy allocators for a GC worker.
pub mod copy;
/// Linear scan through a heap range
pub mod linear_scan;
/// Wrapper functions for memory syscalls such as mmap, mprotect, etc.
pub mod memory;
/// Opaque pointers used in MMTk, e.g. VMThread.
pub mod opaque_pointer;
/// MMTk command line options.
pub mod options;
/// Reference processing implementation.
pub mod reference_processor;

// The following modules are only public in the mmtk crate. They should only be used in MMTk core.
/// An analysis framework for collecting data and profiling in GC.
#[cfg(feature = "analysis")]
pub(crate) mod analysis;
/// Logging edges to check duplicated edges in GC.
#[cfg(feature = "extreme_assertions")]
pub(crate) mod edge_logger;
/// Non-generic refs to generic types of <VM>.
pub(crate) mod erase_vm;
/// Finalization implementation.
pub(crate) mod finalizable_processor;
/// Heap implementation, including page resource, mmapper, etc.
pub(crate) mod heap;
#[cfg(feature = "is_mmtk_object")]
pub mod is_mmtk_object;
/// Logger initialization
pub(crate) mod logger;
/// Various malloc implementations (conditionally compiled by features)
pub mod malloc;
/// Metadata (OnSide or InHeader) implementation.
pub mod metadata;
/// Forwarding word in object copying.
pub(crate) mod object_forwarding;
/// Utilities funcitons for Rust
pub(crate) mod rust_util;
/// Sanity checker for GC.
#[cfg(feature = "sanity")]
pub(crate) mod sanity;
/// Utils for collecting statistics.
pub(crate) mod statistics;
/// Test utilities.
#[cfg(test)]
pub(crate) mod test_util;
/// A treadmill implementation.
pub(crate) mod treadmill;

#[cfg(feature = "public_bit")]
pub(crate) mod public_bit;

#[cfg(feature = "extra_header")]
pub(crate) mod object_extra_header_metadata;

// These modules are private. They are only used by other util modules.

/// A very simple, generic malloc-free allocator
mod freelist;
/// Implementation of GenericFreeList by an int vector.
mod int_array_freelist;
/// Implementation of GenericFreeList backed by raw memory, allocated
/// on demand direct from the OS (via mmap).
mod raw_memory_freelist;

use std::sync::atomic::AtomicU32;
// use std::sync::atomic::AtomicUsize;

pub use self::address::Address;
pub use self::address::ObjectReference;
pub use self::opaque_pointer::*;
pub use self::reference_processor::ReferenceProcessor;

pub(crate) static MUTATOR_ID_GENERATOR: AtomicU32 = AtomicU32::new(1);

#[cfg(feature = "public_object_analysis")]
#[derive(Default)]
pub(crate) struct Statistics {
    pub per_request_scope_allocation_count: usize,
    pub per_request_scope_allocation_bytes: usize,
    pub per_request_scope_public_count: usize,
    pub per_request_scope_public_bytes: usize,

    pub request_scope_allocation_count: usize,
    pub request_scope_allocation_bytes: usize,
    pub request_scope_public_count: usize,
    pub request_scope_public_bytes: usize,
    pub harness_scope_allocation_count: usize,
    pub harness_scope_allocation_bytes: usize,
    pub harness_scope_public_count: usize,
    pub harness_scope_public_bytes: usize,
    pub all_scope_allocation_count: usize,
    pub all_scope_allocation_bytes: usize,
    pub all_scope_public_count: usize,
    pub all_scope_public_bytes: usize,
}

#[cfg(feature = "public_object_analysis")]
impl Statistics {
    pub fn clear_harness_scope(&mut self) {
        self.harness_scope_allocation_bytes = 0;
        self.harness_scope_allocation_count = 0;
        self.harness_scope_public_bytes = 0;
        self.harness_scope_public_count = 0;
    }

    pub fn clear_request_scope(&mut self) {
        self.request_scope_allocation_bytes = 0;
        self.request_scope_allocation_count = 0;
        self.request_scope_public_bytes = 0;
        self.request_scope_public_count = 0;
    }
}

#[cfg(feature = "public_object_analysis")]
lazy_static! {
    pub(crate) static ref STATISTICS: std::sync::Mutex<Statistics> =
        std::sync::Mutex::new(Statistics::default());
}

#[cfg(all(feature = "thread_local_gc", feature = "debug_publish_object"))]
pub(crate) static LOCAL_GC_ID_GENERATOR: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(1);
