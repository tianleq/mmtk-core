use super::defrag::Histogram;
use super::line::Line;
use super::ImmixSpace;
use crate::util::constants::*;
use crate::util::heap::blockpageresource::BlockPool;
use crate::util::heap::chunk_map::Chunk;
use crate::util::linear_scan::{Region, RegionIterator};
use crate::util::metadata::side_metadata::{MetadataByteArrayRef, SideMetadataSpec};
#[cfg(feature = "vo_bit")]
use crate::util::metadata::vo_bit;
use crate::util::Address;
use crate::vm::*;
use std::sync::atomic::Ordering;

/// The block allocation state.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum BlockState {
    /// the block is not allocated.
    Unallocated,
    /// the block is allocated but not marked.
    Unmarked,
    /// the block is allocated and marked.
    Marked,
    /// the block is marked as reusable.
    Reusable { unavailable_lines: u8 },
}

impl BlockState {
    /// Private constant
    const MARK_UNALLOCATED: u8 = 0;
    /// Private constant
    const MARK_UNMARKED: u8 = u8::MAX;
    /// Private constant
    const MARK_MARKED: u8 = u8::MAX - 1;
}

impl From<u8> for BlockState {
    fn from(state: u8) -> Self {
        match state {
            Self::MARK_UNALLOCATED => BlockState::Unallocated,
            Self::MARK_UNMARKED => BlockState::Unmarked,
            Self::MARK_MARKED => BlockState::Marked,
            unavailable_lines => BlockState::Reusable { unavailable_lines },
        }
    }
}

impl From<BlockState> for u8 {
    fn from(state: BlockState) -> Self {
        match state {
            BlockState::Unallocated => BlockState::MARK_UNALLOCATED,
            BlockState::Unmarked => BlockState::MARK_UNMARKED,
            BlockState::Marked => BlockState::MARK_MARKED,
            BlockState::Reusable { unavailable_lines } => unavailable_lines,
        }
    }
}

impl BlockState {
    /// Test if the block is reuasable.
    pub const fn is_reusable(&self) -> bool {
        matches!(self, BlockState::Reusable { .. })
    }
}

/// Data structure to reference an immix block.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialOrd, PartialEq)]
pub struct Block(Address);

impl Region for Block {
    #[cfg(not(feature = "immix_smaller_block"))]
    const LOG_BYTES: usize = 15;
    #[cfg(feature = "immix_smaller_block")]
    const LOG_BYTES: usize = 13;

    fn from_aligned_address(address: Address) -> Self {
        debug_assert!(address.is_aligned_to(Self::BYTES));
        Self(address)
    }

    fn start(&self) -> Address {
        self.0
    }
}

impl Block {
    /// Log pages in block
    pub const LOG_PAGES: usize = Self::LOG_BYTES - LOG_BYTES_IN_PAGE as usize;
    /// Pages in block
    pub const PAGES: usize = 1 << Self::LOG_PAGES;
    /// Log lines in block
    pub const LOG_LINES: usize = Self::LOG_BYTES - Line::LOG_BYTES;
    /// Lines in block
    pub const LINES: usize = 1 << Self::LOG_LINES;

    /// Block defrag state table (side)
    pub const DEFRAG_STATE_TABLE: SideMetadataSpec =
        crate::util::metadata::side_metadata::spec_defs::IX_BLOCK_DEFRAG;

    /// Block mark table (side)
    pub const MARK_TABLE: SideMetadataSpec =
        crate::util::metadata::side_metadata::spec_defs::IX_BLOCK_MARK;

    /// Block owner table (side)
    #[cfg(feature = "thread_local_gc")]
    pub const OWNER_TABLE: SideMetadataSpec =
        crate::util::metadata::side_metadata::spec_defs::IX_BLOCK_OWNER;

    /// Block level public table (side)
    #[cfg(feature = "thread_local_gc")]
    pub const BLOCK_PUBLICATION_TABLE: SideMetadataSpec =
        crate::util::metadata::side_metadata::spec_defs::IX_BLOCK_PUBLICATION;

    /// Get the chunk containing the block.
    pub fn chunk(&self) -> Chunk {
        Chunk::from_unaligned_address(self.0)
    }

    /// Get the address range of the block's line mark table.
    #[allow(clippy::assertions_on_constants)]
    pub fn line_mark_table(&self) -> MetadataByteArrayRef<{ Block::LINES }> {
        debug_assert!(!super::BLOCK_ONLY);
        MetadataByteArrayRef::<{ Block::LINES }>::new(&Line::MARK_TABLE, self.start(), Self::BYTES)
    }

    /// Get block mark state.
    pub fn get_state(&self) -> BlockState {
        let byte = Self::MARK_TABLE.load_atomic::<u8>(self.start(), Ordering::SeqCst);
        byte.into()
    }

    /// Set block mark state.
    pub fn set_state(&self, state: BlockState) {
        let state: u8 = u8::from(state);
        Self::MARK_TABLE.store_atomic::<u8>(self.start(), state, Ordering::SeqCst);
    }

    /// Publish block
    #[cfg(feature = "thread_local_gc")]
    pub fn publish(&self, #[cfg(feature = "debug_publish_object")] _mutator: Option<u32>) -> bool {
        let prev_value =
            Self::BLOCK_PUBLICATION_TABLE.fetch_or_atomic::<u8>(self.start(), 1, Ordering::SeqCst);

        prev_value == 0
    }

    #[cfg(feature = "thread_local_gc")]
    pub fn reset_publication(&self) {
        Self::BLOCK_PUBLICATION_TABLE.store_atomic::<u8>(self.start(), 0, Ordering::SeqCst);
        // Also rest all lines within the block
        Line::LINE_PUBLICATION_TABLE.bzero_metadata(self.start(), Self::BYTES);
    }

    #[cfg(feature = "thread_local_gc")]
    pub fn reset_line_mark_state(&self) {
        // rest all lines within the block
        Line::MARK_TABLE.bzero_metadata(self.start(), Self::BYTES);
    }

    #[cfg(feature = "thread_local_gc")]
    pub fn is_block_published(&self) -> bool {
        Self::BLOCK_PUBLICATION_TABLE.load_atomic::<u8>(self.start(), Ordering::SeqCst) == 1
    }

    #[cfg(feature = "thread_local_gc_copying")]
    pub fn are_lines_private(&self) -> bool {
        for line in self.lines() {
            if line.is_line_published() {
                return false;
            }
        }
        true
    }

    #[cfg(all(feature = "thread_local_gc_copying", debug_assertions))]
    pub fn all_public_lines_marked_and_free_lines_unmarked(&self, state: u8) {
        if !self.is_block_published() {
            // trivially true for private block
            return;
        }
        for line in self.lines() {
            if line.is_line_published() {
                if !line.is_marked(state) {
                    panic!(
                        "public block: {:?} -> public line: {:?} is not marked",
                        self, line
                    );
                }
            } else {
                // free lines should not be marked
                if line.is_marked(state) {
                    panic!(
                        "public block: {:?} -> free line: {:?} is marked",
                        self, line
                    );
                }
            }
        }
    }

    // Defrag byte

    const DEFRAG_SOURCE_STATE: u8 = u8::MAX;

    /// Test if the block is marked for defragmentation.
    pub fn is_defrag_source(&self) -> bool {
        let byte = Self::DEFRAG_STATE_TABLE.load_atomic::<u8>(self.start(), Ordering::SeqCst);
        // The byte should be 0 (not defrag source) or 255 (defrag source) if this is a major defrag GC, as we set the values in PrepareBlockState.
        // But it could be any value in a nursery GC.
        byte == Self::DEFRAG_SOURCE_STATE
    }

    /// Mark the block for defragmentation.
    pub fn set_as_defrag_source(&self, defrag: bool) {
        let byte = if defrag { Self::DEFRAG_SOURCE_STATE } else { 0 };
        Self::DEFRAG_STATE_TABLE.store_atomic::<u8>(self.start(), byte, Ordering::SeqCst);
    }

    /// Record the number of holes in the block.
    pub fn set_holes(&self, holes: usize) {
        Self::DEFRAG_STATE_TABLE.store_atomic::<u8>(self.start(), holes as u8, Ordering::SeqCst);
    }

    /// Get the number of holes.
    pub fn get_holes(&self) -> usize {
        let byte = Self::DEFRAG_STATE_TABLE.load_atomic::<u8>(self.start(), Ordering::SeqCst);
        debug_assert_ne!(byte, Self::DEFRAG_SOURCE_STATE);
        byte as usize
    }

    /// Initialize a clean block after acquired from page-resource.
    pub fn init(&self, copy: bool) {
        self.set_state(if copy {
            BlockState::Marked
        } else {
            BlockState::Unmarked
        });

        Self::DEFRAG_STATE_TABLE.store_atomic::<u8>(self.start(), 0, Ordering::SeqCst);
    }

    /// Deinitalize a block before releasing.
    pub fn deinit(&self) {
        self.set_state(BlockState::Unallocated);

        #[cfg(feature = "public_bit")]
        crate::util::metadata::public_bit::bzero_public_bit(self.start(), Self::BYTES);
    }

    pub fn start_line(&self) -> Line {
        Line::from_aligned_address(self.start())
    }

    pub fn end_line(&self) -> Line {
        Line::from_aligned_address(self.end())
    }

    /// Get the range of lines within the block.
    #[allow(clippy::assertions_on_constants)]
    pub fn lines(&self) -> RegionIterator<Line> {
        debug_assert!(!super::BLOCK_ONLY);
        RegionIterator::<Line>::new(self.start_line(), self.end_line())
    }

    /// Clear VO bits metadata for unmarked regions.
    /// This is useful for clearing VO bits during nursery GC for StickyImmix
    /// at which time young objects (allocated in unmarked regions) may die
    /// but we always consider old objects (in marked regions) as live.
    #[cfg(feature = "vo_bit")]
    pub fn clear_vo_bits_for_unmarked_regions(&self, line_mark_state: Option<u8>) {
        match line_mark_state {
            None => {
                match self.get_state() {
                    BlockState::Unmarked => {
                        // It may contain young objects.  Clear it.
                        vo_bit::bzero_vo_bit(self.start(), Self::BYTES);
                    }
                    BlockState::Marked => {
                        // It contains old objects.  Skip it.
                    }
                    _ => unreachable!(),
                }
            }
            Some(state) => {
                // With lines.
                for line in self.lines() {
                    if !line.is_marked(state) {
                        // It may contain young objects.  Clear it.
                        vo_bit::bzero_vo_bit(line.start(), Line::BYTES);
                    }
                }
            }
        }
    }

    #[cfg(feature = "thread_local_gc")]
    pub fn thread_local_can_sweep<VM: VMBinding>(
        &self,
        _space: &ImmixSpace<VM>,
        mark_histogram: &mut Histogram,
        line_mark_state: Option<u8>,
    ) -> bool {
        if super::BLOCK_ONLY {
            match self.get_state() {
                BlockState::Unmarked => {
                    #[cfg(feature = "vo_bit")]
                    vo_bit::helper::on_region_swept::<VM, _>(self, false);

                    if self.is_block_published() {
                        // liveness of public block is unknown during thread-local gc
                        // so conservatively treat it as live
                        false
                    } else {
                        // true if it is private and not marked by the current GC
                        // or if the current one is a global gc
                        true
                    }
                }
                BlockState::Marked => {
                    #[cfg(feature = "vo_bit")]
                    vo_bit::helper::on_region_swept::<VM, _>(self, true);

                    // The block is live.
                    false
                }
                _ => unreachable!(),
            }
        } else {
            // Calculate number of marked lines and holes.
            let mut marked_lines = 0;
            let mut holes = 0;
            let mut prev_line_is_marked = true;
            let line_mark_state = line_mark_state.unwrap();

            let is_published = self.is_block_published();

            for line in self.lines() {
                #[cfg(debug_assertions)]
                {
                    #[cfg(feature = "thread_local_gc_copying")]
                    {
                        if line.is_line_published() {
                            debug_assert!(
                                is_published,
                                "private block: {:?} contains public line: {:?} after a local gc",
                                self, line
                            );
                            // public lines should not be marked as private objects are strictly evacuated
                            debug_assert!(
                                !line.is_marked(line_mark_state),
                                "public line is marked in a local gc"
                            );
                        } else {
                            // line is not public, then it must be either marked and in a private block, or unmarked
                            debug_assert!(
                                !is_published || !line.is_marked(line_mark_state),
                                "public block: {:?} contains marked private line: {:?} after a local gc",
                                self,
                                line
                            );
                        }
                    }
                }

                // public lines are implicitly marked, mark them explicitly so that
                // in the future mutator/collector phase, free lines can always be
                // correctly found by looking at line marks
                if line.is_line_published() {
                    line.mark(line_mark_state);
                }
                if line.is_marked(line_mark_state) {
                    marked_lines += 1;
                    prev_line_is_marked = true;
                } else {
                    if prev_line_is_marked {
                        holes += 1;
                    }
                    #[cfg(debug_assertions)]
                    {
                        crate::util::memory::set(line.start(), 0xCA, Line::BYTES);
                    }

                    #[cfg(feature = "immix_zero_on_release")]
                    crate::util::memory::zero(line.start(), Line::BYTES);

                    prev_line_is_marked = false;
                }
            }

            if marked_lines == 0 {
                #[cfg(feature = "vo_bit")]
                vo_bit::helper::on_region_swept::<VM, _>(self, false);
                // liveness of public block is unknown during thread-local gc
                // so conservatively treat it as alive
                if is_published {
                    debug_assert!(self.get_state() == BlockState::Unmarked);
                    false
                } else {
                    #[cfg(all(feature = "debug_publish_object", debug_assertions))]
                    {
                        // check if locally freed blocks exist in global reusable pool
                        _space.reusable_blocks.iterate_blocks(|block| {
                            debug_assert!(
                                self.0 != block.0,
                                "Block: {:?} is now reclaimed and should not be in the reusable pool",
                                self
                            )
                        });
                    }

                    true
                }
            } else {
                // There are some marked lines. Keep the block live.
                if marked_lines != Block::LINES {
                    // There are holes. Mark the block as reusable.
                    self.set_state(BlockState::Reusable {
                        unavailable_lines: marked_lines as _,
                    });
                } else {
                    // Clear mark state.
                    self.set_state(BlockState::Unmarked);
                }
                // Update mark_histogram
                mark_histogram[holes] += marked_lines;
                // Record number of holes in block side metadata.
                self.set_holes(holes);

                #[cfg(feature = "vo_bit")]
                vo_bit::helper::on_region_swept::<VM, _>(self, true);

                false
            }
        }
    }

    /// Sweep this block.
    /// Return true if the block is swept.
    pub fn sweep<VM: VMBinding>(
        &self,
        space: &ImmixSpace<VM>,
        mark_histogram: &mut Histogram,
        line_mark_state: Option<u8>,
        #[cfg(feature = "debug_thread_local_gc_copying")] gc_stats: &mut crate::util::GCStatistics,
    ) -> bool {
        // This function is only called in a global gc
        if super::BLOCK_ONLY {
            match self.get_state() {
                BlockState::Unallocated => false,
                BlockState::Unmarked => {
                    #[cfg(feature = "vo_bit")]
                    vo_bit::helper::on_region_swept::<VM, _>(self, false);

                    // Release the block if it is allocated but not marked by the current GC.
                    #[cfg(feature = "thread_local_gc")]
                    {
                        self.clear_owner();
                        self.reset_publication();
                    }
                    // release_block will set the block state to BlockState::Unallocated
                    space.release_block(*self);
                    true
                }
                BlockState::Marked => {
                    #[cfg(feature = "vo_bit")]
                    vo_bit::helper::on_region_swept::<VM, _>(self, true);

                    // The block is live.
                    false
                }
                _ => unreachable!(),
            }
        } else {
            // Calculate number of marked lines and holes.
            let mut marked_lines = 0;
            let mut holes = 0;
            let mut prev_line_is_marked = true;
            let line_mark_state = line_mark_state.unwrap();
            #[cfg(feature = "thread_local_gc_copying")]
            let mut is_block_pubic = false;

            for line in self.lines() {
                if line.is_marked(line_mark_state) {
                    marked_lines += 1;
                    prev_line_is_marked = true;
                    // a line is public iff it is marked and line level public bit is set
                    if !is_block_pubic && line.is_line_published() {
                        is_block_pubic = true;
                    }
                } else {
                    if prev_line_is_marked {
                        holes += 1;
                    }
                    // line is not marked, so it is free, line level public bit
                    // needs to be cleared
                    #[cfg(feature = "thread_local_gc_ibm_style")]
                    line.reset_publication();

                    #[cfg(feature = "immix_zero_on_release")]
                    crate::util::memory::zero(line.start(), Line::BYTES);

                    prev_line_is_marked = false;
                }
            }

            #[cfg(feature = "thread_local_gc")]
            if !is_block_pubic {
                self.reset_publication();
            } else {
                debug_assert!(self.is_block_published());
            }

            if marked_lines == 0 {
                #[cfg(feature = "vo_bit")]
                vo_bit::helper::on_region_swept::<VM, _>(self, false);
                // Release the block if non of its lines are marked.
                #[cfg(feature = "thread_local_gc")]
                {
                    self.clear_owner();
                    debug_assert!(self.is_block_published() == false);
                }
                space.release_block(*self);
                #[cfg(feature = "debug_thread_local_gc_copying")]
                {
                    (*gc_stats).number_of_blocks_freed += 1;
                }

                true
            } else {
                #[cfg(feature = "debug_thread_local_gc_copying")]
                {
                    (*gc_stats).number_of_live_blocks += 1;
                    if is_block_pubic {
                        (*gc_stats).number_of_live_public_blocks += 1;
                    }
                }
                // There are some marked lines. Keep the block live.
                if marked_lines != Block::LINES {
                    // There are holes. Mark the block as reusable.
                    self.set_state(BlockState::Reusable {
                        unavailable_lines: marked_lines as _,
                    });

                    #[cfg(not(feature = "thread_local_gc"))]
                    space.reusable_blocks.push(*self);
                    // If copying thread local gc is enabled, only public blocks
                    // can be reused
                    #[cfg(feature = "thread_local_gc_copying")]
                    {
                        if is_block_pubic {
                            debug_assert!(
                                self.owner() == u32::MAX,
                                "block: {:?}, owner: {}, defrag source: {}",
                                self,
                                self.owner(),
                                self.is_defrag_source()
                            );
                            space.reusable_blocks.push(*self);
                            #[cfg(feature = "debug_thread_local_gc_copying")]
                            {
                                (*gc_stats).number_of_global_reusable_blocks += 1;
                            }
                        } else {
                            debug_assert!(self.owner() != u32::MAX);
                            debug_assert!(!self.is_block_published());
                            #[cfg(feature = "debug_thread_local_gc_copying")]
                            {
                                (*gc_stats).number_of_local_reusable_blocks += 1;
                            }
                        }
                    }
                } else {
                    // Clear mark state.
                    self.set_state(BlockState::Unmarked);
                }
                // Update mark_histogram
                mark_histogram[holes] += marked_lines;
                // Record number of holes in block side metadata.
                self.set_holes(holes);

                #[cfg(feature = "vo_bit")]
                vo_bit::helper::on_region_swept::<VM, _>(self, true);

                false
            }
        }
    }

    #[cfg(feature = "thread_local_gc")]
    pub fn owner(&self) -> u32 {
        Self::OWNER_TABLE.load_atomic::<u32>(self.start(), Ordering::SeqCst)
    }

    #[cfg(feature = "thread_local_gc")]
    pub fn clear_owner(&self) {
        self.set_owner(0);
    }

    #[cfg(feature = "thread_local_gc")]
    pub fn set_owner(&self, owner: u32) {
        Self::OWNER_TABLE.store_atomic::<u32>(self.start(), owner, Ordering::SeqCst)
    }
}

/// A non-blocking single-linked list to store blocks.
pub struct ReusableBlockPool {
    queue: BlockPool<Block>,
    num_workers: usize,
}

impl ReusableBlockPool {
    /// Create empty block list
    pub fn new(num_workers: usize) -> Self {
        Self {
            queue: BlockPool::new(num_workers),
            num_workers,
        }
    }

    /// Get number of blocks in this list.
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    #[cfg(not(feature = "thread_local_gc_ibm_style"))]
    /// Add a block to the list.
    pub fn push(&self, block: Block) {
        self.queue.push(block)
    }

    #[cfg(not(feature = "thread_local_gc_ibm_style"))]
    /// Pop a block out of the list.
    pub fn pop(&self) -> Option<Block> {
        self.queue.pop()
    }

    /// Clear the list.
    pub fn reset(&mut self) {
        self.queue = BlockPool::new(self.num_workers);
    }

    /// Iterate all the blocks in the queue. Call the visitor for each reported block.
    pub fn iterate_blocks(&self, mut f: impl FnMut(Block)) {
        self.queue.iterate_blocks(&mut f);
    }

    #[cfg(feature = "thread_local_gc_copying")]
    pub fn thread_local_flush_blocks(&self, blocks: impl IntoIterator<Item = Block>) {
        self.queue.flush_blocks(blocks);
    }

    /// Flush the block queue
    pub fn flush_all(&self) {
        self.queue.flush_all();
    }
}
