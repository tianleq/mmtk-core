use std::sync::Arc;

use super::allocator::{align_allocation_no_fill, fill_alignment_gap, AllocatorContext};
use super::BumpPointer;
use crate::policy::immix::block::Block;
#[cfg(feature = "thread_local_gc")]
use crate::policy::immix::block::BlockState;
use crate::policy::immix::line::*;
use crate::policy::immix::ImmixSpace;
use crate::policy::space::Space;
use crate::util::alloc::allocator::get_maximum_aligned_size;
use crate::util::alloc::Allocator;
use crate::util::linear_scan::Region;
use crate::util::opaque_pointer::VMThread;
use crate::util::rust_util::unlikely;
use crate::util::Address;
#[cfg(feature = "debug_thread_local_gc_copying")]
use crate::util::VMMutatorThread;
use crate::vm::*;

#[repr(i32)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ImmixAllocSemantics {
    Private = 0,
    Public = 1,
}

pub struct ReusableBlocks {
    dense: Vec<Block>,
    #[cfg(feature = "sparse_immix_block")]
    sparse: Vec<Block>,
}

impl ReusableBlocks {
    pub fn new() -> Self {
        ReusableBlocks {
            dense: vec![],
            #[cfg(feature = "sparse_immix_block")]
            sparse: vec![],
        }
    }

    pub fn push_dense_block(&mut self, block: Block) {
        self.dense.push(block);
    }

    #[cfg(feature = "sparse_immix_block")]
    pub fn push_sparse_block(&mut self, block: Block) {
        self.sparse.push(block);
    }

    pub fn len(&self) -> usize {
        #[cfg(feature = "sparse_immix_block")]
        let sparse_len = self.sparse.len();
        #[cfg(not(feature = "sparse_immix_block"))]
        let sparse_len = 0;
        self.dense.len() + sparse_len
    }

    #[cfg(feature = "sparse_immix_block")]
    pub fn sparse_block_len(&self) -> usize {
        self.sparse.len()
    }

    pub fn dense_block_len(&self) -> usize {
        self.dense.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Immix allocator
#[repr(C)]
pub struct ImmixAllocator<VM: VMBinding> {
    /// [`VMThread`] associated with this allocator instance
    pub tls: VMThread,
    #[cfg(feature = "thread_local_gc")]
    mutator_id: u32,
    /// The fastpath bump pointer.
    pub bump_pointer: BumpPointer,
    /// [`Space`](src/policy/space/Space) instance associated with this allocator instance.
    space: &'static ImmixSpace<VM>,
    context: Arc<AllocatorContext<VM>>,
    #[cfg(not(feature = "thread_local_gc"))]
    /// *unused*
    pub hot: bool,
    #[cfg(feature = "thread_local_gc")]
    pub local_copy_reserve_exhausted: bool,
    /// Is this a copy allocator?
    copy: bool,
    /// Bump pointer for large objects
    pub(in crate::util::alloc) large_bump_pointer: BumpPointer,
    /// Is the current request for large or small?
    request_for_large: bool,
    /// Hole-searching cursor
    line: Option<Line>,
    #[cfg(feature = "thread_local_gc")]
    /// Large hole-searching cursor
    sparse_line: Option<Line>,
    #[cfg(feature = "thread_local_gc")]
    pub local_blocks: Box<Vec<Block>>,
    #[cfg(feature = "thread_local_gc")]
    pub local_free_blocks: Box<Vec<Block>>,
    #[cfg(feature = "thread_local_gc")]
    pub local_reusable_blocks: Box<ReusableBlocks>,
    #[cfg(feature = "thread_local_gc")]
    semantic: Option<ImmixAllocSemantics>,
}

impl<VM: VMBinding> ImmixAllocator<VM> {
    pub(crate) fn reset(&mut self) {
        self.bump_pointer.reset(Address::ZERO, Address::ZERO);
        self.large_bump_pointer.reset(Address::ZERO, Address::ZERO);
        self.request_for_large = false;
        self.line = None;
        self.sparse_line = None;
    }

    #[cfg(feature = "thread_local_gc")]
    pub fn set_mutator(&mut self, mutator_id: u32) {
        self.mutator_id = mutator_id;
    }

    #[cfg(feature = "thread_local_gc")]
    pub fn get_mutator(&self) -> u32 {
        self.mutator_id
    }

    #[cfg(feature = "thread_local_gc")]
    pub fn prepare(&mut self) {
        #[cfg(debug_assertions)]
        {
            for &block in self.local_blocks.iter() {
                if block.get_state() == BlockState::Unallocated {
                    panic!(
                        "Local block: {:?} state is Unallocated before a gc occurs",
                        block,
                    );
                }

                debug_assert!(
                    block.owner() == self.mutator_id,
                    "block: {:?} | block state: {:?} | owner: {} != mutator: {}",
                    block,
                    block.get_state(),
                    block.owner(),
                    self.mutator_id
                );

                debug_assert!(block.is_block_dirty());
            }
        }
    }

    // This function is only called after a global gc
    #[cfg(feature = "thread_local_gc")]
    pub fn release(&mut self) {
        // #[cfg(feature = "thread_local_gc_copying")]
        // use crate::policy::immix::LOCAL_GC_COPY_RESERVE_PAGES;
        #[cfg(feature = "thread_local_gc_copying")]
        {
            self.copy = false;
        }

        // clear local reusable block list so that it can be rebuilt
        self.local_blocks
            .extend(self.local_reusable_blocks.dense.drain(..));
        #[cfg(feature = "sparse_immix_block")]
        self.local_blocks
            .extend(self.local_reusable_blocks.sparse.drain(..));

        // remove freed blocks from the local block list
        // After the global gc, freed blocks have been given
        // back to the global block page resource, so simply
        // remove those from the local block list.
        // also remove public reusable blocks from the local block list
        let mut blocks = vec![];
        for block in self.local_blocks.drain(..) {
            if block.get_state() == BlockState::Unallocated {
                #[cfg(debug_assertions)]
                {
                    debug_assert!(
                        block.owner() == 0,
                        "block: {:?} state is Unallocated but owner is {}",
                        block,
                        block.owner()
                    );
                    debug_assert!(!block.is_block_dirty());
                }
            } else {
                #[cfg(feature = "thread_local_gc_copying")]
                if block.get_state().is_reusable() {
                    #[cfg(feature = "sparse_immix_block")]
                    let is_block_sparse = block.is_block_sparse();
                    if block.is_block_published() {
                        if block.is_block_dirty() {
                            #[cfg(debug_assertions)]
                            {
                                // public reusable blocks should have been added to the global list
                                let mut exist = false;
                                self.space.reusable_blocks.iterate_blocks(|b| {
                                    if b == block {
                                        exist = true;
                                    }
                                });
                                self.space.sparse_reusable_blocks.iterate_blocks(|b| {
                                    if b == block {
                                        exist = true;
                                    }
                                });
                                assert!(!exist, "conservative private reusable block: {:?} exist in global reusable list", block);
                                debug_assert!(block.owner() != Block::ANONYMOUS_OWNER);
                                block.set_owner(self.mutator_id);
                            }
                            // block may contain live private objects, so it cannot be reused by other mutator
                            #[cfg(feature = "sparse_immix_block")]
                            if is_block_sparse {
                                self.local_reusable_blocks.push_sparse_block(block);
                            } else {
                                self.local_reusable_blocks.push_dense_block(block);
                            }
                            #[cfg(not(feature = "sparse_immix_block"))]
                            self.local_reusable_blocks.push_dense_block(block);
                        } else {
                            #[cfg(debug_assertions)]
                            {
                                // public reusable blocks should have been added to the global list
                                let mut exist = false;
                                self.space.reusable_blocks.iterate_blocks(|b| {
                                    if b == block {
                                        exist = true;
                                    }
                                });
                                self.space.sparse_reusable_blocks.iterate_blocks(|b| {
                                    if b == block {
                                        exist = true;
                                    }
                                });
                                assert!(exist, "public reusable block: {:?} does not exist in global reusable list", block);
                                debug_assert!(
                                    block.owner() == Block::ANONYMOUS_OWNER,
                                    "block owner: {:?}",
                                    block.owner(),
                                );
                            }
                        }
                    } else {
                        #[cfg(debug_assertions)]
                        {
                            debug_assert!(block.owner() == self.mutator_id);
                            debug_assert!(block.are_lines_private());
                            block.get_reusable_block_info(
                                self.space
                                    .line_unavail_state
                                    .load(atomic::Ordering::Acquire),
                                self.space.line_mark_state.load(atomic::Ordering::Acquire),
                            );
                        }
                        #[cfg(feature = "sparse_immix_block")]
                        if is_block_sparse {
                            self.local_reusable_blocks.push_sparse_block(block);
                        } else {
                            self.local_reusable_blocks.push_dense_block(block);
                        }
                        #[cfg(not(feature = "sparse_immix_block"))]
                        self.local_reusable_blocks.push_dense_block(block);
                    }
                } else {
                    // always add non-reusable block back
                    debug_assert!(block.get_state() == BlockState::Unmarked);

                    if block.is_block_dirty() {
                        #[cfg(debug_assertions)]
                        debug_assert!(block.owner() == self.mutator_id);

                        blocks.push(block);
                    } else {
                        #[cfg(debug_assertions)]
                        {
                            // here the block is full, and if the block is not dirty,
                            // it must be a public block and the current mutator
                            // must have just done a defrag
                            debug_assert!(
                                block.is_block_published(),
                                "block: {:?} without dirty bit set is not published",
                                block
                            );
                            debug_assert!(
                                block.are_lines_public(),
                                "fully occupied block: {:?} contains private lines",
                                block
                            );
                            block.all_public_lines_marked(
                                self.space.line_mark_state.load(atomic::Ordering::Acquire),
                            );
                        }
                    }
                }

                // In a non-moving setting, there is no public reusable blocks
                // because public objects and private objects resides in the same
                // block
                #[cfg(not(feature = "thread_local_gc_copying"))]
                {
                    if block.get_state().is_reusable() {
                        self.local_reusable_blocks.push(block);
                    } else {
                        blocks.push(block);
                    }
                }
            }
            #[cfg(debug_assertions)]
            {
                if block.get_state() != BlockState::Unallocated && !block.is_block_published() {
                    assert!(
                        block.is_block_dirty(),
                        "block: {:?}, state: {:?}",
                        block,
                        block.get_state()
                    );
                }
            }
        }
        // keep local blocks list accurate
        self.local_blocks.extend(blocks);

        // update the local gc copy reserve
        // #[cfg(feature = "thread_local_gc_copying")]
        // {
        //     LOCAL_GC_COPY_RESERVE_PAGES.fetch_max(
        //         Block::PAGES * (self.local_blocks.len() + self.local_reusable_blocks.len()),
        //         atomic::Ordering::SeqCst,
        //     );
        // }

        // verify thread local block list
        #[cfg(debug_assertions)]
        {
            for &block in self.local_blocks.iter() {
                if crate::policy::immix::BLOCK_ONLY {
                    // After a global gc, local list should not have blocks whose state
                    // is BlockState::Unmarked or BlockState::Unallocated
                    debug_assert!(
                        (block.get_state() != BlockState::Unmarked
                            && block.get_state() != BlockState::Unallocated),
                        "Block: {:?},  state: {:?} found",
                        block,
                        block.get_state()
                    );
                } else {
                    debug_assert!(
                        block.get_state() != BlockState::Unallocated,
                        "Block: {:?},  state: {:?} found",
                        block,
                        block.get_state()
                    );
                }
                debug_assert!(
                    block.owner() == self.mutator_id,
                    "Block: {:?} owner: {:?} should not in mutator: {:?} 's local list",
                    block,
                    block.owner(),
                    self.mutator_id,
                );
                debug_assert!(block.is_block_dirty());
            }
            if self.local_reusable_blocks.len() > 16 {
                println!(
                    "after gc | mutator: {:?} local reusable blocks: {}",
                    self.mutator_id,
                    self.local_reusable_blocks.len()
                );
            }
        }
    }

    #[cfg(feature = "debug_thread_local_gc_copying")]
    pub fn collect_thread_local_heap_stats(&self) {
        let mutator = VM::VMActivePlan::mutator(VMMutatorThread(self.tls));
        mutator.stats.number_of_live_blocks = self.local_blocks.len();
        for b in &*self.local_blocks {
            if b.is_block_published() {
                mutator.stats.number_of_live_public_blocks += 1;
            }
        }
        mutator.stats.number_of_local_reusable_blocks = self.local_reusable_blocks.len();
    }

    #[cfg(feature = "debug_thread_local_gc_copying")]
    pub fn local_reusable_blocks_size(&self) -> usize {
        self.local_reusable_blocks.len()
    }

    #[cfg(feature = "thread_local_gc_copying")]
    pub fn thread_local_defrag_prepare(&mut self) {
        debug_assert!(VM::VMActivePlan::is_mutator(self.tls));
        self.copy = true;

        // move local reusable blocks to local blocks
        self.local_blocks
            .extend(self.local_reusable_blocks.dense.drain(..));
        #[cfg(feature = "sparse_immix_block")]
        self.local_blocks
            .extend(self.local_reusable_blocks.sparse.drain(..));

        for &block in self.local_blocks.iter() {
            debug_assert!(block.get_state() == BlockState::Unmarked);
            // all local blocks should be defrag source
            block.set_as_defrag_source(true);
            // dirty bit will be set again later on blocks containing private objects
            block.reset_dirty();
        }
        #[cfg(all(feature = "thread_local_gc_copying", debug_assertions))]
        self.space
            .mutator_in_defrag
            .lock()
            .unwrap()
            .push(self.mutator_id);
    }

    #[cfg(feature = "thread_local_gc")]
    pub fn thread_local_prepare(&mut self) {
        #[cfg(feature = "thread_local_gc_copying")]
        {
            self.copy = true;
            self.local_copy_reserve_exhausted = false;
        }

        // move local reusable blocks to local blocks
        self.local_blocks
            .extend(self.local_reusable_blocks.dense.drain(..));
        #[cfg(feature = "sparse_immix_block")]
        self.local_blocks
            .extend(self.local_reusable_blocks.sparse.drain(..));

        // A local gc does not have PrepareBlockState work packets
        // So resetting the state manually
        for &block in self.local_blocks.iter() {
            // local list should not contain unallocated blocks
            // it may contain unmarked blocks(newly allocated)
            #[cfg(debug_assertions)]
            {
                debug_assert!(
                    block.get_state() != BlockState::Unallocated,
                    "mutator: {} | block: {:?} state: {:?}",
                    self.mutator_id,
                    block,
                    block.get_state()
                );

                debug_assert!(
                    self.mutator_id == block.owner(),
                    "local block list is corrupted, mutator: {}, owner: {}",
                    self.mutator_id,
                    block.owner()
                );

                debug_assert!(block.is_block_dirty());
            }

            #[cfg(not(feature = "thread_local_gc_copying"))]
            let is_defrag_source = false;
            #[cfg(feature = "thread_local_gc_copying")]
            let is_defrag_source = true;

            // in a local gc, always do evacuation
            block.set_as_defrag_source(is_defrag_source);

            // clear object mark bit
            if let crate::util::metadata::MetadataSpec::OnSide(side) =
                *VM::VMObjectModel::LOCAL_MARK_BIT_SPEC
            {
                side.bzero_metadata(block.start(), Block::BYTES);
            }

            // Clear forwarding bits if necessary.
            if let crate::util::metadata::MetadataSpec::OnSide(side) =
                *VM::VMObjectModel::LOCAL_FORWARDING_BITS_SPEC
            {
                // Clear on-the-side forwarding bits.
                side.bzero_metadata(block.start(), Block::BYTES);
            }

            // clear line mark state (public lines have line level public bit set)
            block.reset_line_mark_state();
            block.set_state(BlockState::Unmarked);
            // clear the dirty bit on all owned blocks, if private objects left in place, the dirty bit will be reset
            #[cfg(feature = "thread_local_gc_copying")]
            block.reset_dirty();
        }
        // pre-allocate copy reserve pages
        #[cfg(feature = "thread_local_gc_copying")]
        self.thread_local_gc_reserve_pages();
    }

    #[cfg(feature = "thread_local_gc")]
    /// This is the sweeping blocks work
    pub fn thread_local_release(&mut self) {
        #[cfg(feature = "thread_local_gc_copying")]
        {
            self.copy = false;
        }
        #[cfg(feature = "debug_thread_local_gc_copying")]
        let mutator = VM::VMActivePlan::mutator(VMMutatorThread(self.tls));

        // TODO defrag in local gc is different, needs to be revisited
        let mut histogram = self.space.defrag.new_histogram();
        let line_mark_state = if crate::policy::immix::BLOCK_ONLY {
            None
        } else {
            Some(self.space.line_mark_state.load(atomic::Ordering::Relaxed))
        };
        debug_assert!(self.local_reusable_blocks.is_empty());
        let mark_hisogram = &mut histogram;
        let mut blocks = vec![];
        #[cfg(feature = "thread_local_gc_copying")]
        let mut global_reusable_blocks = vec![];
        #[cfg(feature = "thread_local_gc_copying")]
        let mut global_sparse_reusable_blocks = vec![];
        for block in self.local_blocks.drain(..) {
            #[cfg(debug_assertions)]
            debug_assert!(self.mutator_id == block.owner());

            if block.thread_local_can_sweep(self.space, mark_hisogram, line_mark_state) {
                // release free blocks for now, may cache those blocks locally

                debug_assert!(block.is_block_published() == false);
                self.local_free_blocks.push(block);
                #[cfg(feature = "debug_thread_local_gc_copying")]
                {
                    mutator.stats.number_of_blocks_freed += 1;
                }
            } else {
                #[cfg(not(feature = "thread_local_gc_copying"))]
                {
                    if block.get_state().is_reusable() {
                        self.local_reusable_blocks.push(block);
                    } else {
                        blocks.push(block);
                    }
                }
                #[cfg(feature = "thread_local_gc_copying")]
                {
                    #[cfg(debug_assertions)]
                    let published = block.is_block_published();
                    let is_dirty = block.is_block_dirty();

                    // After a local gc, public blocks should contain public
                    // objects only, so those blocks are no longer ditry(This is no longer the case, private objects may be left in-place)
                    // if published {
                    //     // block.reset_dirty();
                    //     #[cfg(debug_assertions)]
                    //     block.set_owner(Block::ANONYMOUS_OWNER);
                    // }

                    if block.get_state().is_reusable() {
                        #[cfg(debug_assertions)]
                        {
                            block.get_reusable_block_info(
                                self.space
                                    .line_unavail_state
                                    .load(atomic::Ordering::Acquire),
                                self.space.line_mark_state.load(atomic::Ordering::Acquire),
                            );
                        }
                        #[cfg(feature = "sparse_immix_block")]
                        let is_block_sparse = block.is_block_sparse();
                        if !is_dirty {
                            #[cfg(debug_assertions)]
                            {
                                debug_assert!(published, "block: {:?} is not published", block);
                                block.set_owner(Block::ANONYMOUS_OWNER);
                            }

                            #[cfg(feature = "sparse_immix_block")]
                            if is_block_sparse {
                                global_sparse_reusable_blocks.push(block);
                            } else {
                                global_reusable_blocks.push(block);
                            }
                            #[cfg(not(feature = "sparse_immix_block"))]
                            global_reusable_blocks.push(block);
                        } else {
                            #[cfg(debug_assertions)]
                            debug_assert!(
                                block.owner() == self.mutator_id,
                                "block owner: {:?}, mutator: {:?}",
                                block.owner(),
                                self.mutator_id
                            );
                            #[cfg(feature = "sparse_immix_block")]
                            if is_block_sparse {
                                // private reusable block
                                self.local_reusable_blocks.push_sparse_block(block);
                            } else {
                                // private reusable block
                                self.local_reusable_blocks.push_dense_block(block);
                            }
                            #[cfg(not(feature = "sparse_immix_block"))]
                            self.local_reusable_blocks.push_dense_block(block);
                        }
                    } else {
                        // block is not reusable, add to local block list if it is dirty
                        // a fully occupied public block cannot be reused until a global gc
                        // occurs, so it is meaningless keep it in the local block list
                        if is_dirty {
                            #[cfg(debug_assertions)]
                            debug_assert!(block.owner() == self.mutator_id);

                            blocks.push(block);
                        } else {
                            #[cfg(debug_assertions)]
                            {
                                assert!(
                                    block.are_lines_public(),
                                    "fully occupied public block: {:?} contains private lines",
                                    block
                                );
                                assert!(published);
                                block.set_owner(Block::ANONYMOUS_OWNER);
                            }
                        }
                    }
                }
            }
            #[cfg(debug_assertions)]
            {
                block.all_public_lines_marked(
                    self.space.line_mark_state.load(atomic::Ordering::Relaxed),
                );
            }
        }

        #[cfg(feature = "debug_thread_local_gc_copying")]
        {
            mutator.stats.number_of_global_reusable_blocks = global_reusable_blocks.len();
            mutator.stats.number_of_local_reusable_blocks = self.local_reusable_blocks.len();
            mutator.stats.number_of_live_blocks = self.local_reusable_blocks.len() + blocks.len();
            mutator.stats.number_of_live_public_blocks = 0;
            for b in &blocks {
                if b.is_block_published() {
                    mutator.stats.number_of_live_public_blocks += 1;
                }
            }
            for b in &*self.local_reusable_blocks {
                let lines_delta = match b.get_state() {
                    BlockState::Reusable { unavailable_lines } => {
                        Block::LINES - unavailable_lines as usize
                    }
                    _ => unreachable!("{:?} {:?}", b, b.get_state()),
                };
                mutator.stats.number_of_free_lines_in_local_reusable_blocks += lines_delta;
            }
            for b in &global_reusable_blocks {
                let lines_delta = match b.get_state() {
                    BlockState::Reusable { unavailable_lines } => {
                        Block::LINES - unavailable_lines as usize
                    }
                    _ => unreachable!("{:?} {:?}", b, b.get_state()),
                };
                mutator.stats.number_of_free_lines_in_global_reusable_blocks += lines_delta;
            }
        }

        self.local_blocks.extend(blocks);

        // Give back free blocks
        // local free block list may contain pre-allocated blocks
        // need to deinit those blocks, otherwise, a subsequent global
        // gc will double free those blocks
        self.space
            .thread_local_release_blocks(self.local_free_blocks.drain(..).map(|block| {
                #[cfg(debug_assertions)]
                block.clear_owner();
                block.reset_metadata();
                block.deinit();
                block
            }));
        #[cfg(feature = "thread_local_gc_copying")]
        {
            // Give back public reusable blocks
            self.space
                .reusable_blocks
                .thread_local_flush_blocks(global_reusable_blocks.drain(..));
            self.space
                .sparse_reusable_blocks
                .thread_local_flush_blocks(global_sparse_reusable_blocks.drain(..));
        }

        // verify thread local block list
        #[cfg(debug_assertions)]
        {
            #[cfg(feature = "thread_local_gc_copying")]
            let mut reusable_count = 0;
            for &block in self.local_blocks.iter() {
                if crate::policy::immix::BLOCK_ONLY {
                    // After a local gc, public block may be BlockState::Unmarked
                    debug_assert!(
                        block.is_block_published()
                            || (block.get_state() != BlockState::Unmarked
                                && block.get_state() != BlockState::Unallocated)
                    );
                } else {
                    // After a local gc, public block may be BlockState::Unmarked
                    debug_assert!(block.get_state() != BlockState::Unallocated);
                    debug_assert!(
                        block.owner() == self.mutator_id,
                        "local block list is corrupted"
                    )
                }
                #[cfg(feature = "thread_local_gc_copying")]
                if block.get_state().is_reusable() {
                    reusable_count += 1;
                }

                #[cfg(feature = "debug_publish_object")]
                {
                    assert!(
                        !self.local_free_blocks.contains(&block),
                        "local free block: {:?} is still in the local block list",
                        block
                    );
                }
            }
            #[cfg(feature = "thread_local_gc_copying")]
            assert!(
                reusable_count <= 1,
                "private objects are not strictly evacuated in the local gc"
            );
        }
    }

    #[cfg(feature = "thread_local_gc_copying")]
    pub fn thread_local_copy_reserve_exhausted(&mut self) -> bool {
        debug_assert!(self.copy && VM::VMActivePlan::is_mutator(self.tls));
        if self.local_copy_reserve_exhausted {
            return true;
        } else {
            if self.local_free_blocks.is_empty()
                && (self.bump_pointer.limit - self.bump_pointer.cursor < Line::BYTES << 1)
            {
                self.local_copy_reserve_exhausted = true;
                return true;
            }
            false
        }
    }

    fn alloc_impl(
        &mut self,
        size: usize,
        align: usize,
        offset: usize,
        clean_page_only: bool,
    ) -> Address {
        debug_assert!(
            size <= crate::policy::immix::MAX_IMMIX_OBJECT_SIZE,
            "Trying to allocate a {} bytes object, which is larger than MAX_IMMIX_OBJECT_SIZE {}",
            size,
            crate::policy::immix::MAX_IMMIX_OBJECT_SIZE
        );

        let result = align_allocation_no_fill::<VM>(self.bump_pointer.cursor, align, offset);
        let new_cursor = result + size;

        if new_cursor > self.bump_pointer.limit {
            trace!(
                "{:?}: Thread local buffer used up, go to alloc slow path",
                self.tls
            );

            if get_maximum_aligned_size::<VM>(size, align) > Line::BYTES {
                // Size larger than a line: do large allocation
                let rtn = self.overflow_alloc(size, align, offset); // overflow_allow will never use reusable blocks

                rtn
            } else {
                // Size smaller than a line: fit into holes
                let rtn = self.alloc_slow_hot(size, align, offset, clean_page_only);

                rtn
            }
        } else {
            // Simple bump allocation.
            fill_alignment_gap::<VM>(self.bump_pointer.cursor, result);
            self.bump_pointer.cursor = new_cursor;
            trace!(
                "{:?}: Bump allocation size: {}, result: {}, new_cursor: {}, limit: {}",
                self.tls,
                size,
                result,
                self.bump_pointer.cursor,
                self.bump_pointer.limit
            );

            result
        }
    }

    #[cfg(feature = "thread_local_gc")]
    pub fn alloc_copy(&mut self, size: usize, align: usize, offset: usize) -> Address {
        debug_assert!(self.copy, "calling alloc_copy with self.copy == false");
        let is_mutator = self.mutator_id != u32::MAX;
        let clean_page_only = is_mutator;

        #[cfg(not(feature = "extra_header"))]
        {
            let rtn = self.alloc_impl(size, align, offset, clean_page_only);

            #[cfg(debug_assertions)]
            {
                debug_assert!(
                    self.mutator_id == Block::from_unaligned_address(rtn).owner(),
                    "mutator_id: {} != block owner: {}",
                    self.mutator_id,
                    Block::from_unaligned_address(rtn).owner()
                );
                // if it is mutator, then the dirty bit must be set
                debug_assert!(!is_mutator || Block::from_unaligned_address(rtn).is_block_dirty());
            }

            rtn
        }

        #[cfg(feature = "extra_header")]
        {
            let rtn = self.alloc_impl(
                size + VM::EXTRA_HEADER_BYTES,
                align,
                offset,
                clean_page_only,
            );
            #[cfg(debug_assertions)]
            debug_assert!(
                self.mutator_id == Block::from_unaligned_address(rtn).owner(),
                "mutator_id: {} != block owner: {}",
                self.mutator_id,
                Block::from_unaligned_address(rtn).owner()
            );
            // if it is mutator, then the dirty bit must be set
            debug_assert!(!is_mutator || Block::from_unaligned_address(rtn).is_block_dirty());
            if !rtn.is_zero() {
                debug_assert!(
                    !crate::util::metadata::public_bit::is_public_object(
                        rtn + VM::EXTRA_HEADER_BYTES
                    ),
                    "public bit is not cleared properly"
                );
                rtn + VM::EXTRA_HEADER_BYTES
            } else {
                rtn
            }
        }
    }

    #[cfg(feature = "thread_local_gc_copying")]
    pub fn thread_local_gc_reserve_pages(&mut self) {
        let number_of_clean_blocks =
            usize::from(self.space.get_max_thread_local_gc_copy_reserve_blocks());
        debug_assert!(
            VM::VMActivePlan::is_mutator(self.tls),
            "only mutator doing local gc should call thread_local_gc_reserve_pages"
        );

        #[cfg(feature = "debug_thread_local_gc_copying")]
        {
            let mutator = VM::VMActivePlan::mutator(VMMutatorThread(self.tls));
            mutator.stats.number_of_blocks_acquired_for_evacuation += number_of_clean_blocks;
        }
        // pre-allocate copy reserve pages into local cache to make sure
        // local gc can always evacuate
        match self
            .immix_space()
            .get_clean_blocks(self.tls, false, number_of_clean_blocks)
        {
            Some(blocks) => {
                self.local_free_blocks.extend(blocks);
            }
            None => panic!("does not have enough space to do evacuation during local gc"),
        }
    }
}

impl<VM: VMBinding> Allocator<VM> for ImmixAllocator<VM> {
    fn get_space(&self) -> &'static dyn Space<VM> {
        self.space as _
    }

    fn get_context(&self) -> &AllocatorContext<VM> {
        &self.context
    }

    fn does_thread_local_allocation(&self) -> bool {
        true
    }

    fn get_thread_local_buffer_granularity(&self) -> usize {
        crate::policy::immix::block::Block::BYTES
    }

    #[cfg(not(feature = "extra_header"))]
    fn alloc(&mut self, size: usize, align: usize, offset: usize) -> Address {
        self.alloc_impl(size, align, offset, false)
    }

    #[cfg(feature = "extra_header")]
    fn alloc(&mut self, size: usize, align: usize, offset: usize) -> Address {
        // #[cfg(feature = "thread_local_gc_copying")]
        // use crate::policy::immix::LOCAL_GC_COPY_RESERVE_PAGES;

        let rtn = self.alloc_impl(size + VM::EXTRA_HEADER_BYTES, align, offset, false);

        // #[cfg(feature = "thread_local_gc_copying")]
        // LOCAL_GC_COPY_RESERVE_PAGES.fetch_max(
        //     Block::PAGES * (self.local_blocks.len() + self.local_reusable_blocks.len()),
        //     atomic::Ordering::SeqCst,
        // );
        // Check if the result is valid and return the actual object start address
        // Note that `rtn` can be null in the case of OOM
        if !rtn.is_zero() {
            debug_assert!(
                !crate::util::metadata::public_bit::is_public_object(rtn + VM::EXTRA_HEADER_BYTES),
                "public bit is not cleared properly"
            );
            rtn + VM::EXTRA_HEADER_BYTES
        } else {
            rtn
        }
    }

    /// Acquire a clean block from ImmixSpace for allocation.
    fn alloc_slow_once(&mut self, size: usize, align: usize, offset: usize) -> Address {
        trace!("{:?}: alloc_slow_once", self.tls);
        #[cfg(feature = "debug_thread_local_gc_copying")]
        {
            use crate::util::GLOBAL_GC_STATISTICS;

            if self.copy && self.mutator_id == u32::MAX {
                // allocation occurs in gc phase
                let mut guard = GLOBAL_GC_STATISTICS.lock().unwrap();
                guard.number_of_blocks_acquired_for_evacuation += 1;
            } else {
                debug_assert!(VM::VMActivePlan::is_mutator(self.tls));
                let mutator = VM::VMActivePlan::mutator(VMMutatorThread(self.tls));
                if self.copy {
                    mutator.stats.number_of_blocks_acquired_for_evacuation += 1;
                }
                mutator.stats.number_of_clean_blocks_acquired += 1;
            }
        }

        self.acquire_clean_block(size, align, offset)
    }

    /// This is called when precise stress is used. We try use the thread local buffer for
    /// the allocation (after restoring the correct limit for thread local buffer). If we cannot
    /// allocate from thread local buffer, we will go to the actual slowpath. After allocation,
    /// we will set the fake limit so future allocations will fail the slowpath and get here as well.
    fn alloc_slow_once_precise_stress(
        &mut self,
        size: usize,
        align: usize,
        offset: usize,
        need_poll: bool,
    ) -> Address {
        trace!("{:?}: alloc_slow_once_precise_stress", self.tls);
        // If we are required to make a poll, we call acquire_clean_block() which will acquire memory
        // from the space which includes a GC poll.
        if need_poll {
            trace!(
                "{:?}: alloc_slow_once_precise_stress going to poll",
                self.tls
            );
            let ret = self.acquire_clean_block(size, align, offset);
            // Set fake limits so later allocation will fail in the fastpath, and end up going to this
            // special slowpath.
            self.set_limit_for_stress();
            trace!(
                "{:?}: alloc_slow_once_precise_stress done - forced stress poll",
                self.tls
            );
            return ret;
        }

        // We are not yet required to do a stress GC. We will try to allocate from thread local
        // buffer if possible.  Restore the fake limit to the normal limit so we can do thread
        // local allocation normally. Check if we have exhausted our current thread local block,
        // and if so, then directly acquire a new one
        self.restore_limit_for_stress();
        let ret = if self.require_new_block(size, align, offset) {
            // We don't have enough space in thread local block to service the allocation request,
            // hence allocate a new block
            trace!(
                "{:?}: alloc_slow_once_precise_stress - acquire new block",
                self.tls
            );
            self.acquire_clean_block(size, align, offset)
        } else {
            // This `alloc()` call should always succeed given the if-branch checks if we are out
            // of thread local block space
            trace!("{:?}: alloc_slow_once_precise_stress - alloc()", self.tls,);
            self.alloc(size, align, offset)
        };
        // Set fake limits
        self.set_limit_for_stress();
        ret
    }

    fn get_tls(&self) -> VMThread {
        self.tls
    }

    #[cfg(feature = "thread_local_gc_copying")]
    fn local_heap_in_pages(&self) -> usize {
        Block::PAGES * (self.local_blocks.len() + self.local_reusable_blocks.len())
    }
}

impl<VM: VMBinding> ImmixAllocator<VM> {
    pub(crate) fn new(
        tls: VMThread,
        _mutator_id: u32,
        space: Option<&'static dyn Space<VM>>,
        context: Arc<AllocatorContext<VM>>,
        copy: bool,
        _semantic: Option<ImmixAllocSemantics>,
    ) -> Self {
        let _space = space.unwrap().downcast_ref::<ImmixSpace<VM>>().unwrap();
        // Local line mark state has to be in line with global line mark state, cannot use the default
        // value GLOBAL_RESET_MARK_STATE as mutator threads can be spawned at any time
        // let _global_line_mark_state = _space.line_mark_state.load(atomic::Ordering::Acquire);
        return ImmixAllocator {
            tls,
            #[cfg(feature = "thread_local_gc")]
            mutator_id: _mutator_id,
            space: _space,
            context,
            bump_pointer: BumpPointer::default(),
            #[cfg(not(feature = "thread_local_gc"))]
            hot: false,
            #[cfg(feature = "thread_local_gc")]
            local_copy_reserve_exhausted: false,
            copy,
            large_bump_pointer: BumpPointer::default(),
            request_for_large: false,
            line: None,
            #[cfg(feature = "thread_local_gc")]
            sparse_line: None,
            #[cfg(feature = "thread_local_gc")]
            semantic: _semantic,
            #[cfg(feature = "thread_local_gc")]
            local_blocks: Box::new(Vec::new()),
            #[cfg(feature = "thread_local_gc")]
            local_free_blocks: Box::new(Vec::new()),
            #[cfg(feature = "thread_local_gc")]
            local_reusable_blocks: Box::new(ReusableBlocks::new()),
        };
    }

    pub(crate) fn immix_space(&self) -> &'static ImmixSpace<VM> {
        self.space
    }

    /// Large-object (larger than a line) bump allocation.
    fn overflow_alloc(&mut self, size: usize, align: usize, offset: usize) -> Address {
        trace!("{:?}: overflow_alloc", self.tls);
        let start = align_allocation_no_fill::<VM>(self.large_bump_pointer.cursor, align, offset);
        let end = start + size;
        if end > self.large_bump_pointer.limit {
            #[cfg(feature = "sparse_immix_block")]
            {
                // try looking for sparse reusable blocks first
                self.request_for_large = true;
                // use sparse reusable blocks during mutator phase only
                let rtn = if !self.copy && self.acquire_sparse_recyclable_lines(size, align, offset)
                {
                    self.alloc(size, align, offset)
                } else {
                    self.alloc_slow_inline(size, align, offset)
                };
                // let rtn = self.alloc_slow_inline(size, align, offset);
                self.request_for_large = false;

                rtn
            }
            #[cfg(not(feature = "sparse_immix_block"))]
            {
                self.request_for_large = true;
                let rtn = self.alloc_slow_inline(size, align, offset);
                self.request_for_large = false;

                rtn
            }
        } else {
            fill_alignment_gap::<VM>(self.large_bump_pointer.cursor, start);
            self.large_bump_pointer.cursor = end;
            start
        }
    }

    /// Bump allocate small objects into recyclable lines (i.e. holes).
    fn alloc_slow_hot(
        &mut self,
        size: usize,
        align: usize,
        offset: usize,
        clean_page_only: bool,
    ) -> Address {
        trace!("{:?}: alloc_slow_hot", self.tls);
        if !clean_page_only && self.acquire_recyclable_lines(size, align, offset) {
            // If stress test is active, then we need to go to the slow path instead of directly
            // calling `alloc()`. This is because the `acquire_recyclable_lines()` function
            // manipulates the cursor and limit if a line can be recycled and if we directly call
            // `alloc()` after recyling a line, then we will miss updating the `allocation_bytes`
            // as the newly recycled line will service the allocation request. If we set the stress
            // factor limit directly in `acquire_recyclable_lines()`, then we risk running into an
            // loop of failing the fastpath (i.e. `alloc()`) and then trying to allocate from a
            // recyclable line.  Hence, we bring the "if we're in stress test" check up a level and
            // directly call `alloc_slow_inline()` which will properly account for the allocation
            // request as well as allocate from the newly recycled line
            let stress_test = self.context.options.is_stress_test_gc_enabled();
            let precise_stress = *self.context.options.precise_stress;
            if unlikely(stress_test && precise_stress) {
                self.alloc_slow_inline(size, align, offset)
            } else {
                self.alloc(size, align, offset)
            }
        } else {
            self.alloc_slow_inline(size, align, offset)
        }
    }

    /// Search for recyclable lines.
    fn acquire_recyclable_lines(&mut self, size: usize, align: usize, offset: usize) -> bool {
        while self.line.is_some()
            || self.acquire_recyclable_block(
                #[cfg(feature = "sparse_immix_block")]
                1,
            )
        {
            let line = self.line.unwrap();

            if let Some((start_line, end_line)) =
                self.immix_space().get_next_available_lines(line, 1)
            {
                // Find recyclable lines. Update the bump allocation cursor and limit.
                self.bump_pointer.cursor = start_line.start();
                self.bump_pointer.limit = end_line.start();

                #[cfg(feature = "thread_local_gc_copying")]
                {
                    // conservatively increase the local allocation size
                    if !self.copy {
                        debug_assert_ne!(self.mutator_id, u32::MAX);
                        debug_assert!(VM::VMActivePlan::is_mutator(self.tls));
                        let mutator =
                            VM::VMActivePlan::mutator(crate::util::VMMutatorThread(self.tls));
                        mutator.local_allocation_size += end_line.start() - start_line.start();
                    }
                }

                trace!(
                    "{:?}: acquire_recyclable_lines -> {:?} [{:?}, {:?}) {:?}",
                    self.tls,
                    self.line,
                    start_line,
                    end_line,
                    self.tls
                );

                crate::util::memory::zero(
                    self.bump_pointer.cursor,
                    self.bump_pointer.limit - self.bump_pointer.cursor,
                );
                debug_assert!(
                    align_allocation_no_fill::<VM>(self.bump_pointer.cursor, align, offset) + size
                        <= self.bump_pointer.limit
                );

                #[cfg(feature = "public_bit")]
                crate::util::metadata::public_bit::bzero_public_bit(
                    self.bump_pointer.cursor,
                    self.bump_pointer.limit - self.bump_pointer.cursor,
                );

                #[cfg(all(feature = "thread_local_gc", debug_assertions))]
                {
                    let iter =
                        crate::util::linear_scan::RegionIterator::<Line>::new(start_line, end_line);
                    for line in iter {
                        debug_assert!(!line.is_line_published());
                    }
                }

                let block = line.block();
                self.line = if end_line == block.end_line() {
                    // Hole searching reached the end of a reusable block. Set the hole-searching cursor to None.
                    None
                } else {
                    // Update the hole-searching cursor .
                    Some(end_line)
                };
                return true;
            } else {
                // No more recyclable lines. Set the hole-searching cursor to None.
                self.line = None
            }
        }
        false
    }

    #[cfg(feature = "sparse_immix_block")]
    /// Search for recyclable lines.
    fn acquire_sparse_recyclable_lines(
        &mut self,
        size: usize,
        align: usize,
        offset: usize,
    ) -> bool {
        let lines_required = if (size % Line::BYTES as usize) != 0 {
            1 + (size / Line::BYTES)
        } else {
            size / Line::BYTES
        };

        let mut fresh = false;

        while self.sparse_line.is_some() || {
            fresh = self.acquire_recyclable_block(lines_required as u8);
            fresh
        } {
            let line = self.sparse_line.unwrap();

            if let Some((start_line, end_line)) = self
                .immix_space()
                .get_next_available_lines(line, u8::try_from(lines_required).unwrap())
            {
                #[cfg(debug_assertions)]
                {
                    debug_assert!(end_line.start() - start_line.start() < Block::BYTES);
                    let end_block = end_line.block();
                    let start_block = start_line.block();
                    if fresh {
                        debug_assert!(
                            end_block.start() == start_block.start()
                                || end_block.start() == start_block.next().start()
                        );
                    } else {
                        debug_assert!(
                            line.block().start() == start_block.start()
                                && (line.block().start() == end_block.start()
                                    || line.block().next().start() == end_block.start())
                        );
                    }
                }

                // Find recyclable lines. Update the bump allocation cursor and limit.
                self.large_bump_pointer.cursor = start_line.start();
                self.large_bump_pointer.limit = end_line.start();

                #[cfg(feature = "thread_local_gc_copying")]
                {
                    // only mutator phase use sparse reusable blocks
                    debug_assert!(VM::VMActivePlan::is_mutator(self.tls));
                    let mutator = VM::VMActivePlan::mutator(crate::util::VMMutatorThread(self.tls));
                    mutator.local_allocation_size += end_line.start() - start_line.start();
                }

                trace!(
                    "{:?}: acquire_sparse_recyclable_lines -> {:?} [{:?}, {:?}) {:?}",
                    self.tls,
                    self.line,
                    start_line,
                    end_line,
                    self.tls
                );

                #[cfg(feature = "public_bit")]
                crate::util::metadata::public_bit::bzero_public_bit(
                    self.large_bump_pointer.cursor,
                    self.large_bump_pointer.limit - self.large_bump_pointer.cursor,
                );
                crate::util::memory::zero(
                    self.large_bump_pointer.cursor,
                    self.large_bump_pointer.limit - self.large_bump_pointer.cursor,
                );

                #[cfg(all(feature = "thread_local_gc", debug_assertions))]
                {
                    let iter =
                        crate::util::linear_scan::RegionIterator::<Line>::new(start_line, end_line);
                    for line in iter {
                        debug_assert!(!line.is_line_published());
                    }
                }
                debug_assert!(
                    align_allocation_no_fill::<VM>(self.large_bump_pointer.cursor, align, offset)
                        + size
                        <= self.large_bump_pointer.limit
                );

                let block = line.block();
                self.sparse_line = if end_line == block.end_line() {
                    // Hole searching reached the end of a reusable block. Set the hole-searching cursor to None.
                    None
                } else {
                    // Update the hole-searching cursor .
                    Some(end_line)
                };

                return true;
            } else {
                let block = line.block();
                debug_assert!(
                    self.large_bump_pointer.cursor.is_zero()
                        || block.start()
                            == Block::from_unaligned_address(self.large_bump_pointer.cursor)
                                .start()
                );
                // No more recyclable lines. Set the hole-searching cursor to None.
                self.sparse_line = None;
                // It is guaranteed that freshly acquired reusable block has a hole large enough
                debug_assert!(
                    fresh == false,
                    "fresh block: {:?} does not have enough space. hole size: {:?}",
                    block,
                    block.get_hole_size()
                );
            }
        }
        false
    }

    #[cfg(not(feature = "thread_local_gc"))]
    fn acquire_recyclable_block(&mut self) -> bool {
        match self.immix_space().get_reusable_block(self.copy) {
            Some(block) => {
                trace!("{:?}: acquire_recyclable_block -> {:?}", self.tls, block);

                // Set the hole-searching cursor to the start of this block.
                self.line = Some(block.start_line());
                true
            }
            _ => false,
        }
    }

    #[cfg(feature = "thread_local_gc")]
    /// Get a recyclable block from ImmixSpace.
    fn acquire_recyclable_block(
        &mut self,
        #[cfg(feature = "sparse_immix_block")] lines_required: u8,
    ) -> bool {
        // In a non-moving setting, there is no concept of global public reusable blocks
        // (live public objects and live private objects always share the same block)
        // Therefore, only local/private reusable block can be used

        #[cfg(not(feature = "thread_local_gc_copying"))]
        {
            if let Some(block) = self.local_reusable_blocks.pop() {
                trace!(
                    "{:?}: Acquired a reusable block {:?} -> {:?} from thread local buffer",
                    self.tls,
                    block.start(),
                    block.end()
                );
                debug_assert!(!self.copy, "evacuation should always acquire a clean page");
                block.init(self.copy);
                // Set the hole-searching cursor to the start of this block.
                self.line = Some(block.start_line());
                debug_assert!(
                    block.owner() == self.mutator_id,
                    "block owner: {}",
                    block.owner()
                );
                // add local reusable block to local block list
                self.local_blocks.push(block);
                return true;
            }
            return false;
        }
        #[cfg(feature = "thread_local_gc_copying")]
        {
            match self.semantic {
                Some(ImmixAllocSemantics::Public) => {
                    // this is collector
                    // only use gobal reusable blocks in global gc
                    debug_assert!(self.copy);
                    debug_assert!(self.request_for_large == false);
                    self.acquire_public_recyclable_block().is_some()
                }
                Some(ImmixAllocSemantics::Private) => {
                    panic!("Private objects are not evacuated by GC thread")
                }
                None => {
                    #[cfg(feature = "sparse_immix_block")]
                    let local_reusable_block = if self.request_for_large {
                        debug_assert_ne!(lines_required, 1);
                        // search the local list and find a sparse block that can fit the object
                        self.acquire_local_sparse_reusable_block(lines_required)
                    } else {
                        self.local_reusable_blocks.dense.pop()
                    };
                    #[cfg(not(feature = "sparse_immix_block"))]
                    let local_reusable_block = self.local_reusable_blocks.dense.pop();
                    // this is a mutator
                    if let Some(block) = local_reusable_block {
                        trace!(
                            "{:?}: Acquired a reusable block {:?} -> {:?} from thread local buffer",
                            self.tls,
                            block.start(),
                            block.end()
                        );

                        #[cfg(debug_assertions)]
                        let exists_in_local;
                        #[cfg(debug_assertions)]
                        {
                            exists_in_local = !block.get_state().is_reusable();

                            debug_assert!(
                                !self.copy,
                                "evacuation should always acquire a clean page"
                            );
                            block.get_reusable_block_info(
                                self.space
                                    .line_unavail_state
                                    .load(atomic::Ordering::Acquire),
                                self.space.line_mark_state.load(atomic::Ordering::Acquire),
                            );
                        }
                        block.init(false);
                        block.taint();
                        // Set the hole-searching cursor to the start of this block.
                        if self.request_for_large {
                            self.sparse_line = Some(block.start_line());
                        } else {
                            self.line = Some(block.start_line());
                        }

                        #[cfg(debug_assertions)]
                        debug_assert!(
                            block.owner() == self.mutator_id,
                            "block owner: {} | mutator: {}",
                            block.owner(),
                            self.mutator_id
                        );
                        // add local reusable block to local block list
                        #[cfg(debug_assertions)]
                        debug_assert!(exists_in_local == false);
                        self.local_blocks.push(block);
                        // block.set_hole_size(0);
                        return true;
                    }

                    #[cfg(feature = "sparse_immix_block")]
                    let public_reusable_block = if self.request_for_large {
                        debug_assert_ne!(lines_required, 1);
                        // search the local list that can fit the object
                        self.acquire_public_sparse_recyclable_block(lines_required)
                    } else {
                        self.acquire_public_recyclable_block()
                    };
                    #[cfg(not(feature = "sparse_immix_block"))]
                    let public_reusable_block = self.acquire_public_recyclable_block();
                    match public_reusable_block {
                        Some(block) => {
                            #[cfg(debug_assertions)]
                            {
                                debug_assert!(!block.are_lines_private());
                                block.set_owner(self.mutator_id);
                            }
                            // add reusable block to local block list, otherwise, those blocks
                            // leaked and local gc cannot reuse them
                            self.local_blocks.push(block);
                            block.taint();
                            // block.set_hole_size(0);
                            true
                        }
                        _ => false,
                    }
                }
            }
        }
    }

    #[cfg(feature = "thread_local_gc")]
    fn acquire_public_recyclable_block(&mut self) -> Option<Block> {
        match self.immix_space().get_reusable_block(self.copy) {
            Some(block) => {
                trace!("{:?}: acquire_recyclable_block -> {:?}", self.tls, block);
                #[cfg(debug_assertions)]
                {
                    debug_assert!(block.is_block_published());
                    #[cfg(feature = "sparse_immix_block")]
                    debug_assert!(block.is_block_sparse() == false);
                    debug_assert!(block.owner() == Block::ANONYMOUS_OWNER);
                    debug_assert!(
                        block.is_block_dirty() == false,
                        "global reusable block: {:?} has dirty bit set",
                        block
                    );
                    debug_assert!(block.get_state().is_reusable() == false);
                }

                // Set the hole-searching cursor to the start of this block.
                self.line = Some(block.start_line());
                Some(block)
            }
            _ => None,
        }
    }
    #[cfg(feature = "sparse_immix_block")]
    fn acquire_public_sparse_recyclable_block(&mut self, lines_required: u8) -> Option<Block> {
        loop {
            if let Some(block) = self.immix_space().get_sparse_reusable_block(self.copy) {
                #[cfg(debug_assertions)]
                debug_assert!(block.is_block_sparse());
                if block.get_hole_size() >= lines_required {
                    // Set the hole-searching cursor to the start of this block.
                    self.sparse_line = Some(block.start_line());

                    return Some(block);
                } else {
                    // block.taint();
                    self.local_reusable_blocks.push_sparse_block(block);
                }
            } else {
                return None;
            }
        }
    }

    #[cfg(feature = "sparse_immix_block")]
    fn acquire_local_sparse_reusable_block(&mut self, lines_required: u8) -> Option<Block> {
        let mut result = None;
        let length = self.local_reusable_blocks.sparse_block_len();

        for i in 0..length {
            let b = self.local_reusable_blocks.sparse.get(i).unwrap();
            #[cfg(debug_assertions)]
            {
                use atomic::Ordering;

                debug_assert!(b.is_block_sparse());
                debug_assert!(b.is_block_dirty());
                b.get_reusable_block_info(
                    self.space.line_unavail_state.load(Ordering::Acquire),
                    self.space.line_mark_state.load(Ordering::Acquire),
                );
            }
            if b.get_hole_size() >= lines_required {
                result = Some(*b);
                self.local_reusable_blocks.sparse.swap_remove(i);
                break;
            }
        }
        #[cfg(debug_assertions)]
        {
            if result.is_none() {
                debug_assert_eq!(length, self.local_reusable_blocks.sparse_block_len());
            } else {
                debug_assert_eq!(length - 1, self.local_reusable_blocks.sparse_block_len());
            }
        }
        result
    }

    // Get a clean block from ImmixSpace.
    fn acquire_clean_block(&mut self, size: usize, align: usize, offset: usize) -> Address {
        // when thread local gc is enabled, it will search the thread local free block list first
        match self.try_get_clean_block() {
            None => {
                #[cfg(feature = "thread_local_gc")]
                // add an assertion here, assume collectors can always allocate new blocks
                assert!(
                    self.semantic.is_none() && !self.copy,
                    "cannot acquire clean blocks to evacuate"
                );

                Address::ZERO
            }
            Some(block) => {
                trace!(
                    "{:?}: Acquired a new block {:?} -> {:?}",
                    self.tls,
                    block.start(),
                    block.end()
                );

                #[cfg(feature = "thread_local_gc")]
                {
                    #[cfg(debug_assertions)]
                    block.set_owner(self.mutator_id);

                    if let Some(semantic) = self.semantic {
                        // This is collector
                        debug_assert!(
                            !VM::VMActivePlan::is_mutator(self.tls),
                            "Only collector thread should reach here"
                        );
                        match semantic {
                            ImmixAllocSemantics::Public => {
                                #[cfg(debug_assertions)]
                                debug_assert!(
                                    block.owner() == u32::MAX,
                                    "block: {:?}, owner: {:?} ",
                                    block,
                                    block.owner()
                                );
                                // This only occurs during global gc, since only global gc evacuates public objects
                                block.publish(false);
                            }
                            ImmixAllocSemantics::Private => {
                                panic!("local gc is now done by the mutator")
                            }
                        }
                    } else {
                        // This is a mutator
                        debug_assert!(
                            VM::VMActivePlan::is_mutator(self.tls),
                            "Only mutator thread should reach here"
                        );
                        // Only add freshly allocated block into the local block list
                        self.local_blocks.push(block);
                        // update the local heap size
                        #[cfg(feature = "thread_local_gc_copying")]
                        {
                            block.taint();
                            // mutator phase only
                            if !self.copy {
                                let mutator = VM::VMActivePlan::mutator(
                                    crate::util::VMMutatorThread(self.tls),
                                );
                                mutator.local_allocation_size += Block::BYTES;
                                #[cfg(feature = "debug_thread_local_gc_copying")]
                                {
                                    crate::util::TOTAL_ALLOCATION_BYTES
                                        .fetch_add(Block::BYTES, atomic::Ordering::SeqCst);
                                    mutator.stats.bytes_allocated += Block::BYTES;
                                    let mut guard =
                                        crate::util::GLOBAL_GC_STATISTICS.lock().unwrap();
                                    guard.bytes_allocated += Block::BYTES;
                                }
                            }
                        }
                    }
                }

                if self.request_for_large {
                    self.large_bump_pointer.cursor = block.start();
                    self.large_bump_pointer.limit = block.end();
                } else {
                    self.bump_pointer.cursor = block.start();
                    self.bump_pointer.limit = block.end();
                }
                self.alloc(size, align, offset)
            }
        }
    }

    fn try_get_clean_block(&mut self) -> Option<crate::policy::immix::block::Block> {
        #[cfg(feature = "thread_local_gc")]
        if let Some(block) = self.local_free_blocks.pop() {
            trace!(
                "{:?}: Acquired a new block {:?} -> {:?} from thread local buffer",
                self.tls,
                block.start(),
                block.end()
            );

            // only reach here during local gc phase
            #[cfg(feature = "thread_local_gc_copying")]
            {
                debug_assert!(self.copy && VM::VMActivePlan::is_mutator(self.tls));
                // if self.local_free_blocks.is_empty() {
                //     self.local_copy_reserve_exhausted = true;
                // }
            }

            block.init(self.copy);
            #[cfg(debug_assertions)]
            block.set_owner(self.mutator_id);
            // Not sure if the following is needed
            self.immix_space().chunk_map.set(
                block.chunk(),
                crate::util::heap::chunk_map::ChunkState::Allocated,
            );

            return Some(block);
        }
        #[cfg(not(feature = "thread_local_gc_copying"))]
        let tls = self.tls;
        #[cfg(feature = "thread_local_gc_copying")]
        let tls = if self.copy {
            VMThread::UNINITIALIZED
        } else {
            self.tls
        };

        let block = self.immix_space().get_clean_block(tls, self.copy);
        #[cfg(debug_assertions)]
        debug_assert!(
            block.is_none() || block.unwrap().owner() == 0,
            "block: {:?}, existing owner: {:?}, mutator: {:?}",
            block.unwrap(),
            block.unwrap().owner(),
            self.mutator_id
        );
        #[cfg(feature = "debug_thread_local_gc_copying")]
        {
            use crate::util::GLOBAL_GC_STATISTICS;

            if block.is_some() {
                if VM::VMActivePlan::is_mutator(self.tls) {
                    let mutator = VM::VMActivePlan::mutator(VMMutatorThread(self.tls));
                    mutator.stats.number_of_live_blocks += 1;
                }
                let mut guard = GLOBAL_GC_STATISTICS.lock().unwrap();
                guard.number_of_live_blocks += 1;
            }
        }

        block
    }

    /// Return whether the TLAB has been exhausted and we need to acquire a new block. Assumes that
    /// the buffer limits have been restored using [`ImmixAllocator::restore_limit_for_stress`].
    /// Note that this function may implicitly change the limits of the allocator.
    fn require_new_block(&mut self, size: usize, align: usize, offset: usize) -> bool {
        let result = align_allocation_no_fill::<VM>(self.bump_pointer.cursor, align, offset);
        let new_cursor = result + size;
        let insufficient_space = new_cursor > self.bump_pointer.limit;

        // We want this function to behave as if `alloc()` has been called. Hence, we perform a
        // size check and then return the conditions where `alloc_slow_inline()` would be called
        // in an `alloc()` call, namely when both `overflow_alloc()` and `alloc_slow_hot()` fail
        // to service the allocation request
        if insufficient_space && get_maximum_aligned_size::<VM>(size, align) > Line::BYTES {
            let start =
                align_allocation_no_fill::<VM>(self.large_bump_pointer.cursor, align, offset);
            let end = start + size;
            end > self.large_bump_pointer.limit
        } else {
            // We try to acquire recyclable lines here just like `alloc_slow_hot()`
            insufficient_space && !self.acquire_recyclable_lines(size, align, offset)
        }
    }

    /// Set fake limits for the bump allocation for stress tests. The fake limit is the remaining
    /// thread local buffer size, which should be always smaller than the bump cursor. This method
    /// may be reentrant. We need to check before setting the values.
    fn set_limit_for_stress(&mut self) {
        if self.bump_pointer.cursor < self.bump_pointer.limit {
            let old_limit = self.bump_pointer.limit;
            let new_limit =
                unsafe { Address::from_usize(self.bump_pointer.limit - self.bump_pointer.cursor) };
            self.bump_pointer.limit = new_limit;
            trace!(
                "{:?}: set_limit_for_stress. normal c {} l {} -> {}",
                self.tls,
                self.bump_pointer.cursor,
                old_limit,
                new_limit,
            );
        }

        if self.large_bump_pointer.cursor < self.large_bump_pointer.limit {
            let old_lg_limit = self.large_bump_pointer.limit;
            let new_lg_limit = unsafe {
                Address::from_usize(self.large_bump_pointer.limit - self.large_bump_pointer.cursor)
            };
            self.large_bump_pointer.limit = new_lg_limit;
            trace!(
                "{:?}: set_limit_for_stress. large c {} l {} -> {}",
                self.tls,
                self.large_bump_pointer.cursor,
                old_lg_limit,
                new_lg_limit,
            );
        }
    }

    /// Restore the real limits for the bump allocation so we can properly do a thread local
    /// allocation. The fake limit is the remaining thread local buffer size, and we restore the
    /// actual limit from the size and the cursor. This method may be reentrant. We need to check
    /// before setting the values.
    fn restore_limit_for_stress(&mut self) {
        if self.bump_pointer.limit < self.bump_pointer.cursor {
            let old_limit = self.bump_pointer.limit;
            let new_limit = self.bump_pointer.cursor + self.bump_pointer.limit.as_usize();
            self.bump_pointer.limit = new_limit;
            trace!(
                "{:?}: restore_limit_for_stress. normal c {} l {} -> {}",
                self.tls,
                self.bump_pointer.cursor,
                old_limit,
                new_limit,
            );
        }

        if self.large_bump_pointer.limit < self.large_bump_pointer.cursor {
            let old_lg_limit = self.large_bump_pointer.limit;
            let new_lg_limit =
                self.large_bump_pointer.cursor + self.large_bump_pointer.limit.as_usize();
            self.large_bump_pointer.limit = new_lg_limit;
            trace!(
                "{:?}: restore_limit_for_stress. large c {} l {} -> {}",
                self.tls,
                self.large_bump_pointer.cursor,
                old_lg_limit,
                new_lg_limit,
            );
        }
    }
}
