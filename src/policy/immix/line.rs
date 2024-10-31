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
    pub const MAX_MARK_STATE: u8 = 127;

    /// Line mark table (side)
    pub const MARK_TABLE: SideMetadataSpec =
        crate::util::metadata::side_metadata::spec_defs::IX_LINE_MARK;

    #[cfg(feature = "thread_local_gc")]
    /// Line level public table (side)
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
        Self::mark_lines_for_object_impl::<VM>(object, state);
    }

    #[cfg(all(feature = "thread_local_gc", feature = "debug_publish_object"))]
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

        let is_public_object = crate::util::metadata::public_bit::is_public::<VM>(object);

        let iter = RegionIterator::<Line>::new(start_line, end_line);
        for line in iter {
            if is_public_object {
                if !line.is_line_published() {
                    error!(
                        "line: {:?} is not published properly. published: {}, marked: {}",
                        line,
                        line.is_line_published(),
                        line.is_marked(state)
                    );
                    return false;
                }
            } else {
                // private object may reside in a public line,
            }
        }
        true
    }

    #[cfg(feature = "thread_local_gc")]
    pub fn publish_lines_of_object<VM: VMBinding>(object: ObjectReference, state: u8) {
        // mark line as public

        let (_start_line, _end_line) = Self::mark_lines_for_object_impl::<VM>(object, state);
        #[cfg(debug_assertions)]
        {
            let iter = RegionIterator::<Line>::new(_start_line, _end_line);
            for line in iter {
                debug_assert!(
                    line.is_marked(state),
                    "public object: {:?} is not marked properly ({:?})",
                    object,
                    state
                );
                debug_assert!(line.is_line_published());
            }
        }
        #[cfg(feature = "debug_thread_local_gc_copying")]
        {
            use crate::util::TOTAL_PU8LISHED_LINES;

            let iter = RegionIterator::<Line>::new(_start_line, _end_line);
            let mut lines = 0;
            for _ in iter {
                lines += 1;
            }
            TOTAL_PU8LISHED_LINES.fetch_add(lines, atomic::Ordering::SeqCst);
        }
    }

    #[cfg(feature = "thread_local_gc")]
    pub fn is_line_published(&self) -> bool {
        Line::LINE_PUBLICATION_TABLE.load_atomic::<u8>(self.start(), atomic::Ordering::SeqCst) != 0
    }

    #[cfg(feature = "thread_local_gc")]
    /// Mark all lines the object is spanned to, but keep public lines untouched
    pub fn thread_local_mark_lines_for_object<VM: VMBinding>(object: ObjectReference, state: u8) {
        Self::mark_lines_for_object_impl::<VM>(object, state);
    }

    #[cfg(feature = "thread_local_gc")]
    fn mark_lines_for_object_impl<VM: VMBinding>(
        object: ObjectReference,
        state: u8,
    ) -> (Line, Line) {
        debug_assert!(!super::BLOCK_ONLY);
        #[cfg(not(feature = "extra_header"))]
        let start = object.to_object_start::<VM>();
        #[cfg(not(feature = "extra_header"))]
        let end = start + VM::VMObjectModel::get_current_size(object);
        #[cfg(feature = "extra_header")]
        let start = object.to_object_start::<VM>() - VM::EXTRA_HEADER_BYTES;
        #[cfg(feature = "extra_header")]
        let end = start + VM::VMObjectModel::get_current_size(object) + VM::EXTRA_HEADER_BYTES;
        let start_line = Line::from_unaligned_address(start);
        let mut end_line = Line::from_unaligned_address(end);
        if !Line::is_aligned(end) {
            end_line = end_line.next();
        }

        let iter = RegionIterator::<Line>::new(start_line, end_line);
        for line in iter {
            // benign race here, as the state is the same,
            // multiple gc threads are trying to write the same byte value
            line.mark(state);
            // also publish lines if object is public
            if crate::util::metadata::public_bit::is_public::<VM>(object) {
                // benign race here, line is shared by multiple objects, set 1 can occur concurrently
                // during global gc phase
                Line::LINE_PUBLICATION_TABLE.store_atomic::<u8>(
                    line.start(),
                    1,
                    atomic::Ordering::SeqCst,
                );
            }
        }
        (start_line, end_line)
    }
}
