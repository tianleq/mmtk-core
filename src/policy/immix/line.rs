use super::block::Block;
use crate::util::linear_scan::{Region, RegionIterator};
use crate::util::metadata::side_metadata::SideMetadataSpec;
use crate::{
    util::{Address, ObjectReference},
    vm::*,
};

/// Data structure to reference a line within an immix block.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialOrd, PartialEq, Eq)]
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
    #[cfg(not(feature = "thread_local_gc"))]
    pub const MAX_MARK_STATE: u8 = 127;
    #[cfg(feature = "thread_local_gc")]
    const MARK_STATE_BITS: u8 = 3;
    #[cfg(feature = "thread_local_gc")]
    pub const LOCAL_MARK_STATE_BITS: u8 = 4;
    #[cfg(feature = "thread_local_gc")]
    pub const GLOBAL_RESET_MARK_STATE: u8 = Self::RESET_MARK_STATE << Self::LOCAL_MARK_STATE_BITS;
    #[cfg(feature = "thread_local_gc")]
    pub const MAX_MARK_STATE: u8 =
        (Self::GLOBAL_RESET_MARK_STATE << Self::MARK_STATE_BITS) - Self::GLOBAL_RESET_MARK_STATE;
    #[cfg(feature = "thread_local_gc")]
    pub const LOCAL_MAX_MARK_STATE: u8 = (1 << Self::LOCAL_MARK_STATE_BITS) - 1;
    #[cfg(feature = "thread_local_gc")]
    pub const GLOBAL_LINE_MARK_STATE_MASK: u8 = 0xF0;
    #[cfg(feature = "thread_local_gc")]
    pub const PUBLIC_LINE_MARK_STATE_BIT_MASK: u8 = 0x80;

    /// Line mark table (side)
    pub const MARK_TABLE: SideMetadataSpec =
        crate::util::metadata::side_metadata::spec_defs::IX_LINE_MARK;

    /// Block level public table (side)
    pub const LINE_PUBLICATION_TABLE: SideMetadataSpec =
        crate::util::metadata::side_metadata::spec_defs::IX_LINE_PUBLICATION;

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

    #[cfg(feature = "thread_local_gc")]
    /// Test line publication state.
    pub fn is_published(&self, publish_state: u8) -> bool {
        self.is_marked(publish_state)
    }

    #[cfg(debug_assertions)]
    /// Test line publication state.
    pub fn debug_line_mark_state(&self) -> u8 {
        unsafe { Self::MARK_TABLE.load::<u8>(self.start()) }
    }

    #[cfg(debug_assertions)]
    /// Test line publication state.
    pub fn reset_line_mark_state(&self) {
        unsafe { Self::MARK_TABLE.store::<u8>(self.start(), 0) }
    }

    #[cfg(feature = "thread_local_gc")]
    pub fn public_line_mark_state(state: u8) -> u8 {
        (state & Self::GLOBAL_LINE_MARK_STATE_MASK) | Self::PUBLIC_LINE_MARK_STATE_BIT_MASK
    }

    #[cfg(not(feature = "thread_local_gc"))]
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

    #[cfg(feature = "thread_local_gc")]
    pub fn mark_lines_for_object<VM: VMBinding>(object: ObjectReference, state: u8) -> usize {
        // debug_assert!(!super::BLOCK_ONLY);
        // let start = object.to_object_start::<VM>();
        // let end = start + VM::VMObjectModel::get_current_size(object);
        // let start_line = Line::from_unaligned_address(start);
        // let mut end_line = Line::from_unaligned_address(end);
        // if !Line::is_aligned(end) {
        //     end_line = end_line.next();
        // }
        // let mut marked_lines = 0;
        // let iter = RegionIterator::<Line>::new(start_line, end_line);
        // for line in iter {
        //     if !line.is_marked(state) {
        //         marked_lines += 1;
        //     }
        //     line.mark(state)
        // }
        // marked_lines
        Self::mark_lines_for_object_impl::<VM>(object, state, false)
    }

    #[cfg(feature = "thread_local_gc")]
    pub fn verify_line_mark_state_of_object<VM: VMBinding>(
        object: ObjectReference,
        state: u8,
    ) -> bool {
        debug_assert!(!super::BLOCK_ONLY);
        let start = object.to_object_start::<VM>();
        let end = start + VM::VMObjectModel::get_current_size(object);
        let start_line = Line::from_unaligned_address(start);
        let mut end_line = Line::from_unaligned_address(end);
        if !Line::is_aligned(end) {
            end_line = end_line.next();
        }

        let iter = RegionIterator::<Line>::new(start_line, end_line);
        for line in iter {
            if line.is_marked(state) {
                return false;
            }
        }
        true
    }

    #[cfg(feature = "thread_local_gc")]
    pub fn publish_lines_for_object<VM: VMBinding>(object: ObjectReference) {
        let start = object.to_object_start::<VM>();
        let end = start + VM::VMObjectModel::get_current_size(object);
        let start_line = Line::from_unaligned_address(start);
        let mut end_line = Line::from_unaligned_address(end);
        if !Line::is_aligned(end) {
            end_line = end_line.next();
        }
        // The following is safe because an object can only be published by its owner(exactly one thread will do the publication)
        let size = end_line.end() - start_line.start();
        debug_assert!(
            size % Line::BYTES == 0,
            "size is not a multiply of line size"
        );
        Line::LINE_PUBLICATION_TABLE.bset_metadata(start_line.start(), size);
    }

    #[cfg(feature = "thread_local_gc")]
    pub fn is_public_line(&self) -> bool {
        unsafe { Line::LINE_PUBLICATION_TABLE.load::<u8>(self.start()) != 0 }
    }

    #[cfg(feature = "thread_local_gc")]
    /// Mark all lines the object is spanned to, but keep public lines untouched
    pub fn thread_local_mark_lines_for_object<VM: VMBinding>(
        object: ObjectReference,
        state: u8,
    ) -> usize {
        // It is safe/sound to retain public state because the objects visited
        // here in the local gc are private, other mutator cannot see them. And since
        // the current mutator is stopped, no more private objects will be published
        Self::mark_lines_for_object_impl::<VM>(object, state, true)
    }

    #[cfg(feature = "thread_local_gc")]
    pub fn mark_lines_for_object_impl<VM: VMBinding>(
        object: ObjectReference,
        state: u8,
        retain_public_state: bool,
    ) -> usize {
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
            } else if retain_public_state && line.is_published(Self::public_line_mark_state(state))
            {
                continue;
            }
            line.mark(state)
        }
        marked_lines
    }
}
