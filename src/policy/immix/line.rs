use super::block::Block;
use crate::util::linear_scan::{Region, RegionIterator};
use crate::util::metadata::side_metadata::SideMetadataSpec;
use crate::{
    util::{Address, ObjectReference},
    vm::*,
};

/// Data structure to reference a line within an immix block.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialOrd, PartialEq, Eq, Hash)]
pub struct Line(Address);

impl Region for Line {
    const LOG_BYTES: usize = 8;

    #[allow(clippy::assertions_on_constants)] // make sure line is not used when BLOCK_ONLY is turned on.
    fn from_aligned_address(address: Address) -> Self {
        debug_assert!(!super::BLOCK_ONLY);
        debug_assert!(address.is_aligned_to(Self::BYTES));
        Self(address)
    }

    fn start(&self) -> Address {
        self.0
    }
}

#[allow(clippy::assertions_on_constants)]
impl Line {
    pub const RESET_MARK_STATE: u8 = 1;
    pub const MAX_MARK_STATE: u8 = 127;

    /// Line mark table (side)
    pub const MARK_TABLE: SideMetadataSpec =
        crate::util::metadata::side_metadata::spec_defs::IX_LINE_MARK;

    /// Get the block containing the line.
    pub fn block(&self) -> Block {
        debug_assert!(!super::BLOCK_ONLY);
        Block::from_unaligned_address(self.0)
    }

    /// Get line index within its containing block.
    pub fn get_index_within_block(&self) -> usize {
        let addr = self.start();
        addr.get_extent(Block::align(addr)) >> Line::LOG_BYTES
    }

    /// Mark the line. This will update the side line mark table.
    pub fn mark(&self, state: u8) {
        debug_assert!(!super::BLOCK_ONLY);
        unsafe {
            Self::MARK_TABLE.store::<u8>(self.start(), state);
        }
    }

    /// Test line mark state.
    pub fn is_marked(&self, state: u8) -> bool {
        debug_assert!(!super::BLOCK_ONLY);
        unsafe { Self::MARK_TABLE.load::<u8>(self.start()) == state }
    }

    /// Mark all lines the object is spanned to.
    pub fn mark_lines_for_object<VM: VMBinding>(object: ObjectReference, state: u8) -> usize {
        debug_assert!(!super::BLOCK_ONLY);
        let start = object.to_object_start::<VM>();
        let end = start + VM::VMObjectModel::get_current_size(object);
        let start_line = Line::from_unaligned_address(start);
        let mut end_line = Line::from_unaligned_address(end);
        if !Line::is_aligned(end) {
            end_line = end_line.next();
        }
        let mut marked_lines = 0;
        let iter = RegionIterator::<Line>::new(start_line, end_line);
        for line in iter {
            if !line.is_marked(state) {
                marked_lines += 1;
            }
            line.mark(state)
        }
        marked_lines
    }

    #[cfg(feature = "immix_allocation_policy")]
    fn collector_try_acquire_hole(&self, overflow: bool) -> (bool, bool) {
        use std::sync::atomic::Ordering;

        const HOLE_STATUS_MASK: u8 = !Block::LARGE_HOLE_INDEX_MASK;
        let mut large_hole = overflow;
        let block = self.block();
        let idx = self.get_index_within_block() as u8;
        let hole_status = block.get_large_hole_status();
        let size = block.get_large_hole_size();
        #[cfg(debug_assertions)]
        {
            if overflow {
                debug_assert!(
                    (hole_status & Block::LARGE_HOLE_INDEX_MASK) == idx,
                    "line: {:?}, index: {}, metadata: {}",
                    self,
                    idx,
                    hole_status & Block::LARGE_HOLE_INDEX_MASK
                );
                debug_assert_ne!(size, 0);
            }
        }
        // Always succeed if the block does not have large hole
        if size == 0 {
            debug_assert!(!overflow);
            debug_assert_eq!(hole_status, 0);
            debug_assert!(!large_hole);
            return (true, large_hole);
        }
        // Now we know this block has a large hole but during GC phase, normal allocation
        // might found a hole that is part of a large hole. So need to check if the large
        // hole was available in the previous mutator phase

        let large_hole_idx = hole_status & Block::LARGE_HOLE_INDEX_MASK;
        if large_hole_idx <= idx && idx < large_hole_idx + size {
            large_hole = true;
            if (hole_status & HOLE_STATUS_MASK) != 0 {
                // the hole has already been used
                return (false, large_hole);
            }
            // large hole, needs to check
            let old = Block::BLOCK_LARGE_HOLE_STATUS.fetch_or_atomic(
                block.start(),
                HOLE_STATUS_MASK,
                Ordering::SeqCst,
            );

            return ((old & HOLE_STATUS_MASK) == 0, large_hole);
        } else {
            debug_assert!(!large_hole);
            return (true, large_hole);
        }
    }

    #[cfg(feature = "immix_allocation_policy")]
    fn mutator_try_acquire_hole(&self, overflow: bool) -> (bool, bool) {
        use std::sync::atomic::Ordering;

        const HOLE_STATUS_MASK: u8 = !Block::LARGE_HOLE_INDEX_MASK;

        let mut large_hole = overflow;
        let block = self.block();
        let size = block.get_large_hole_size();
        let hole_status = block.get_large_hole_status();
        let idx = hole_status & Block::LARGE_HOLE_INDEX_MASK;
        let hole_idx = self.get_index_within_block() as u8;

        // normal allocation in small holes always succeed
        if size == 0 {
            debug_assert!(!overflow);
            debug_assert_eq!(hole_status, 0);
            return (true, large_hole);
        } else if idx != hole_idx {
            debug_assert!(!overflow);
            return (true, large_hole);
        }
        // Now we know the hole is a large one
        debug_assert!(size >= Block::HUGE_HOLE_SIZE && size < Block::LINES as u8);
        debug_assert_eq!(idx, hole_idx);

        large_hole = true;
        let old = Block::BLOCK_LARGE_HOLE_STATUS.fetch_or_atomic(
            block.start(),
            HOLE_STATUS_MASK,
            Ordering::SeqCst,
        );
        debug_assert!(
            (old & Block::LARGE_HOLE_INDEX_MASK) == hole_idx as u8,
            "line: {:?}, index: {}, metadata: {}",
            self,
            hole_idx,
            old & Block::LARGE_HOLE_INDEX_MASK
        );

        ((old & HOLE_STATUS_MASK) == 0, large_hole)
    }

    #[cfg(feature = "immix_allocation_policy")]
    pub fn try_acquire_hole(&self, overflow: bool, copy: bool) -> (bool, bool) {
        if copy {
            self.collector_try_acquire_hole(overflow)
        } else {
            self.mutator_try_acquire_hole(overflow)
        }
    }
}
