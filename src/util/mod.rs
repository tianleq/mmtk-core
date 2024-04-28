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
/// Test utilities. We need this module for `MockVM` in criterion benches, which does not include code with `cfg(test)`.
#[cfg(any(test, feature = "mock_test"))]
pub mod test_util;

// The following modules are only public in the mmtk crate. They should only be used in MMTk core.
/// An analysis framework for collecting data and profiling in GC.
#[cfg(feature = "analysis")]
pub(crate) mod analysis;
/// Logging edges to check duplicated edges in GC.
#[cfg(feature = "extreme_assertions")]
pub(crate) mod edge_logger;
/// Non-generic refs to generic types of `<VM>`.
pub(crate) mod erase_vm;
/// Finalization implementation.
pub(crate) mod finalizable_processor;
/// Heap implementation, including page resource, mmapper, etc.
pub mod heap;
/// Checking if an address is an valid MMTk object.
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
/// Reference processing implementation.
pub(crate) mod reference_processor;
/// Utilities funcitons for Rust
pub(crate) mod rust_util;
/// Sanity checker for GC.
#[cfg(feature = "sanity")]
pub(crate) mod sanity;
/// Utils for collecting statistics.
pub(crate) mod statistics;
/// A treadmill implementation.
pub(crate) mod treadmill;

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

pub use self::address::Address;
pub use self::address::ObjectReference;
pub use self::opaque_pointer::*;

pub(crate) static MUTATOR_ID_GENERATOR: AtomicU32 = AtomicU32::new(1);
#[cfg(feature = "public_object_analysis")]
#[derive(Default)]
pub(crate) struct Statistics {
    pub allocation_count: usize,
    pub allocation_bytes: usize,
    pub public_count: usize,
    pub public_bytes: usize,
}

#[cfg(feature = "public_object_analysis")]
lazy_static! {
    pub(crate) static ref REQUEST_SCOPE_OBJECTS_STATS: std::sync::Mutex<Statistics> =
        std::sync::Mutex::new(Statistics::default());
    pub(crate) static ref HARNESS_SCOPE_OBJECTS_STATS: std::sync::Mutex<Statistics> =
        std::sync::Mutex::new(Statistics::default());
    pub(crate) static ref ALL_SCOPE_OBJECTS_STATS: std::sync::Mutex<Statistics> =
        std::sync::Mutex::new(Statistics::default());
}

#[cfg(feature = "debug_thread_local_gc_copying")]
#[derive(Default)]
pub(crate) struct GCStatistics {
    pub bytes_allocated: usize,
    pub bytes_published: usize,
    pub number_of_published_blocks: usize,
    pub number_of_live_blocks: usize,
    pub number_of_live_public_blocks: usize,
    pub number_of_clean_blocks_acquired: usize,
    // The following are local gc related
    pub bytes_copied: usize,
    pub number_of_blocks_freed: usize,
    pub number_of_blocks_acquired_for_evacuation: usize,
    pub number_of_local_reusable_blocks: usize,
    pub number_of_global_reusable_blocks: usize,
    pub number_of_los_pages: usize,
}

#[cfg(feature = "debug_thread_local_gc_copying")]
pub(crate) static TOTAL_ALLOCATION_BYTES: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

#[cfg(feature = "debug_thread_local_gc_copying")]
pub(crate) static GLOBAL_GC_STATISTICS: std::sync::Mutex<GCStatistics> =
    std::sync::Mutex::new(GCStatistics {
        bytes_allocated: 0,
        bytes_published: 0,
        number_of_published_blocks: 0,
        number_of_live_blocks: 0,
        number_of_live_public_blocks: 0,
        number_of_clean_blocks_acquired: 0,
        bytes_copied: 0,
        number_of_blocks_freed: 0,
        number_of_blocks_acquired_for_evacuation: 0,
        number_of_local_reusable_blocks: 0,
        number_of_global_reusable_blocks: 0,
        number_of_los_pages: 0,
    });

#[cfg(feature = "debug_thread_local_gc_copying")]
pub(crate) static GLOBAL_GC_ID: AtomicU32 = AtomicU32::new(1);
