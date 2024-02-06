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

    #[cfg(all(feature = "thread_local_gc", debug_assertions))]
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
    pub fn get_line_mark_state(&self) -> u8 {
        unsafe { Self::MARK_TABLE.load::<u8>(self.start()) }
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
    /// Mark all lines the object is spanned to.
    pub fn mark_lines_for_object<VM: VMBinding>(object: ObjectReference, state: u8) {
        // This function is only called in a global gc
        #[cfg(not(feature = "immix_non_moving"))]
        // In a moving setting, a global gc will evacuate all public objects, which means
        // private objects and public objects will not share the same line. Therefore, no
        // need to retain the public line state
        Self::mark_lines_for_object_impl::<VM>(object, state, false);
        #[cfg(feature = "immix_non_moving")]
        // A non-moving immix has to make sure that public line state is not overwritten
        // Otherwise, subsequent local gc may reclaim a line that still has live public
        // objects on it. The reason behind this is that now global gc no longer evacuate
        // public objects,
        Self::mark_lines_for_object_impl::<VM>(object, state, true);
    }

    #[cfg(all(feature = "thread_local_gc", debug_assertions))]
    pub fn verify_line_mark_state_of_object<VM: VMBinding>(
        object: ObjectReference,
        forbidden_state: u8,
        desired_state: Option<u8>,
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
            if line.is_marked(forbidden_state) {
                return false;
            }
            if let Some(state) = desired_state {
                if !line.is_marked(state) {
                    return false;
                }
            }
        }
        true
    }

    #[cfg(feature = "thread_local_gc")]
    pub fn publish_lines_for_object<VM: VMBinding>(object: ObjectReference, state: u8) {
        // mark line as public
        let (start_line, end_line) = Self::mark_lines_for_object_impl::<VM>(object, state, false);

        let iter = RegionIterator::<Line>::new(start_line, end_line);
        for line in iter {
            debug_assert!(
                line.is_marked(state),
                "public object: {:?} is not marked properly ({:?})",
                object,
                state
            );
            #[cfg(debug_assertions)]
            unsafe {
                Line::LINE_PUBLICATION_TABLE.store::<u8>(line.start(), 1);
            }
        }
    }

    #[cfg(all(feature = "thread_local_gc", debug_assertions))]
    pub fn is_public_line(&self) -> bool {
        unsafe { Line::LINE_PUBLICATION_TABLE.load::<u8>(self.start()) != 0 }
    }

    #[cfg(all(feature = "thread_local_gc", debug_assertions))]
    pub fn reset_public_line(&self) {
        unsafe {
            Line::LINE_PUBLICATION_TABLE.store::<u8>(self.start(), 0);
        }
    }

    #[cfg(feature = "thread_local_gc")]
    /// Mark all lines the object is spanned to, but keep public lines untouched
    pub fn thread_local_mark_lines_for_object<VM: VMBinding>(object: ObjectReference, state: u8) {
        // It is safe/sound to retain public state because the lines/memory visited
        // here in the local gc are private/owned by the current mutator, other mutators cannot
        // see them. And since the current mutator is stopped, no more private objects will be
        // published. So there is no race
        debug_assert!(
            state < 128,
            "local gc line mark state is incorrect, top bit is set to 1"
        );
        Self::mark_lines_for_object_impl::<VM>(object, state, true);
    }

    #[cfg(feature = "thread_local_gc")]
    fn mark_lines_for_object_impl<VM: VMBinding>(
        object: ObjectReference,
        state: u8,
        retain_public_state: bool,
    ) -> (Line, Line) {
        debug_assert!(!super::BLOCK_ONLY);
        #[cfg(not(feature = "debug_publish_object"))]
        let start = object.to_object_start::<VM>();
        #[cfg(not(feature = "debug_publish_object"))]
        let end = start + VM::VMObjectModel::get_current_size(object);
        #[cfg(feature = "debug_publish_object")]
        let start = object.to_object_start::<VM>() - VM::EXTRA_HEADER_BYTES;
        #[cfg(feature = "debug_publish_object")]
        let end = start + VM::VMObjectModel::get_current_size(object) + VM::EXTRA_HEADER_BYTES;
        let start_line = Line::from_unaligned_address(start);
        let mut end_line = Line::from_unaligned_address(end);
        if !Line::is_aligned(end) {
            end_line = end_line.next();
        }

        let iter = RegionIterator::<Line>::new(start_line, end_line);
        for line in iter {
            if retain_public_state && line.is_published(Self::public_line_mark_state(state)) {
                continue;
            }
            // benign race here, as the state is the same,
            // multiple gc threads are trying to write the same byte value
            line.mark(state);
        }
        (start_line, end_line)
    }
}
