pub mod block;
pub mod defrag;
pub mod immixspace;
pub mod line;

pub use immixspace::*;

use crate::policy::immix::block::Block;
use crate::util::linear_scan::Region;

/// The max object size for immix: half of a block
pub const MAX_IMMIX_OBJECT_SIZE: usize = Block::BYTES >> 1;

/// Mark/sweep memory for block-level only
pub const BLOCK_ONLY: bool = false;

/// Mark lines when scanning objects.
/// Otherwise, do it at mark time.
pub const MARK_LINE_AT_SCAN_TIME: bool = true;

// #[cfg(feature = "thread_local_gc_copying")]
// pub(crate) static LOCAL_GC_COPY_RESERVE_PAGES: std::sync::atomic::AtomicUsize =
//     std::sync::atomic::AtomicUsize::new(0);

#[cfg(all(feature = "thread_local_gc_copying", debug_assertions))]
lazy_static! {
    pub(crate) static ref GLOBAL_BLOCK_SET: std::sync::Mutex<std::collections::HashSet<crate::util::Address>> =
        std::sync::Mutex::new(std::collections::HashSet::new());
}
