use super::allocator::{align_allocation_no_fill, fill_alignment_gap};
use crate::plan::Plan;
use crate::policy::immix::block::Block;
use crate::policy::immix::block::BlockState;
use crate::policy::immix::line::*;
use crate::policy::immix::ImmixSpace;
use crate::policy::immix::LOCAL_LINE_MARK_STATES_MAP;
use crate::policy::space::Space;
use crate::util::alloc::allocator::get_maximum_aligned_size;
use crate::util::alloc::Allocator;
use crate::util::linear_scan::Region;
use crate::util::opaque_pointer::VMThread;
use crate::util::rust_util::unlikely;
use crate::util::Address;
use crate::vm::*;
use crossbeam::queue::SegQueue;
use std::collections::HashMap;

#[repr(i32)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ImmixAllocSemantics {
    Private = 0,
    Public = 1,
}

/// Immix allocator
#[repr(C)]
pub struct ImmixAllocator<VM: VMBinding> {
    pub tls: VMThread,
    mutator_id: u32,
    /// Bump pointer
    cursor: Address,
    /// Limit for bump pointer
    limit: Address,
    /// [`Space`](src/policy/space/Space) instance associated with this allocator instance.
    space: &'static ImmixSpace<VM>,
    /// [`Plan`] instance that this allocator instance is associated with.
    plan: &'static dyn Plan<VM = VM>,
    /// *unused*
    hot: bool,
    /// Is this a copy allocator?
    pub copy: bool,
    /// Bump pointer for large objects
    large_cursor: Address,
    /// Limit for bump pointer for large objects
    large_limit: Address,
    /// Is the current request for large or small?
    request_for_large: bool,
    /// Hole-searching cursor
    line: Option<Line>,
    semantic: Option<ImmixAllocSemantics>,
    /// This is only used by collector thread
    /// It collects blocks alloected on behalf of the mutator
    pub local_block_map: Option<Box<HashMap<u32, Vec<Block>>>>,
    /// header of the block acquired by this allocator
    block_header: Option<Block>,
    local_blocks: Box<SegQueue<Block>>,
    /// line mark state of local gc
    pub local_line_mark_state: u8,
    pub local_unavailable_line_mark_state: u8,
}

impl<VM: VMBinding> ImmixAllocator<VM> {
    pub fn reset(&mut self) {
        self.cursor = Address::ZERO;
        self.limit = Address::ZERO;
        self.large_cursor = Address::ZERO;
        self.large_limit = Address::ZERO;
        self.request_for_large = false;
        self.line = None;
    }

    #[cfg(feature = "thread_local_gc")]
    pub fn prepare(&mut self) {
        if self.local_blocks.is_empty() {
            return;
        }
        let blocks = std::mem::replace(&mut self.local_blocks, Box::new(SegQueue::new()));
        for block in blocks.into_iter() {
            info!("add {:?} into local block list", block);
            self.add_block_to_list(block);
        }
    }

    #[cfg(feature = "thread_local_gc")]
    pub fn thread_local_prepare(&mut self) {
        self.prepare();
        // A local gc does not have PrepareBlockState work packets
        // So resetting the satete manually
        let mut current = self.block_header;
        while let Some(block) = current {
            // local list should not contain unallocated blocks
            // it may contain unmarked blocks(newly allocated)
            debug_assert!(
                block.get_state() != BlockState::Unallocated,
                "block: {:?} state: {:?}",
                block,
                block.get_state()
            );
            block.set_state(BlockState::Unmarked);
            // in a local gc, always do the defrag if it is a private block
            let is_defrag_source = !block.is_block_published();
            block.set_as_defrag_source(is_defrag_source);
            // clear object mark bit
            if let crate::util::metadata::MetadataSpec::OnSide(side) =
                *VM::VMObjectModel::LOCAL_MARK_BIT_SPEC
            {
                side.bzero_metadata(block.start(), Block::BYTES);
            }
            // Clear forwarding bits if necessary.
            if is_defrag_source {
                // Note that `ImmixSpace::is_live` depends on the fact that we only clear side
                // forwarding bits for defrag sources.  If we change the code here, we need to
                // make sure `ImmixSpace::is_live` is fixed, too.
                if let crate::util::metadata::MetadataSpec::OnSide(side) =
                    *VM::VMObjectModel::LOCAL_FORWARDING_BITS_SPEC
                {
                    // Clear on-the-side forwarding bits.
                    side.bzero_metadata(block.start(), Block::BYTES);
                }
            }
            current = self.next_block(block);
        }

        self.local_line_mark_state += 1;

        if self.local_line_mark_state >= Line::LOCAL_MAX_MARK_STATE {
            self.local_line_mark_state = Line::RESET_MARK_STATE;
        }
        let mut state_map = LOCAL_LINE_MARK_STATES_MAP.write().unwrap();
        let states = state_map.get_mut(&self.mutator_id).unwrap();
        *states = (
            self.local_line_mark_state,
            self.local_unavailable_line_mark_state,
        );
    }

    fn alloc_impl(
        &mut self,
        size: usize,
        align: usize,
        offset: usize,
        always_acquire_clean_page: bool,
    ) -> Address {
        debug_assert!(
            size <= crate::policy::immix::MAX_IMMIX_OBJECT_SIZE,
            "Trying to allocate a {} bytes object, which is larger than MAX_IMMIX_OBJECT_SIZE {}",
            size,
            crate::policy::immix::MAX_IMMIX_OBJECT_SIZE
        );
        let result = align_allocation_no_fill::<VM>(self.cursor, align, offset);
        let new_cursor = result + size;

        if new_cursor > self.limit {
            trace!(
                "{:?}: Thread local buffer used up, go to alloc slow path",
                self.tls
            );
            if get_maximum_aligned_size::<VM>(size, align) > Line::BYTES {
                // Size larger than a line: do large allocation
                let rtn = self.overflow_alloc(size, align, offset);
                rtn
            } else {
                // Size smaller than a line: fit into holes
                let rtn = self.alloc_slow_hot(size, align, offset, always_acquire_clean_page);
                rtn
            }
        } else {
            // Simple bump allocation.
            fill_alignment_gap::<VM>(self.cursor, result);
            self.cursor = new_cursor;
            trace!(
                "{:?}: Bump allocation size: {}, result: {}, new_cursor: {}, limit: {}",
                self.tls,
                size,
                result,
                self.cursor,
                self.limit
            );
            result
        }
    }

    #[cfg(feature = "thread_local_gc")]
    pub fn release(&mut self) {
        self.local_unavailable_line_mark_state = 0;
        self.local_line_mark_state = 0;
        let mut state_map = LOCAL_LINE_MARK_STATES_MAP.write().unwrap();
        let states = state_map.get_mut(&self.mutator_id).unwrap();
        *states = (
            self.local_line_mark_state,
            self.local_unavailable_line_mark_state,
        );
        // self.release_impl(false);
        // remove freed blocks from the local doubly-linked-list
        let mut current = self.block_header;
        while let Some(block) = current {
            if block.get_state() == BlockState::Unallocated {
                debug_assert!(
                    block.owner() == 0,
                    "block: {:?} state is Unallocated but owner is {}",
                    block,
                    block.owner()
                );
                current = self.remove_block_from_list(block);
                // info!("{:?} block: {:?} removed", block.get_state(), block);
            } else {
                // info!("{:?} block: {:?} found", block.get_state(), block);
                current = self.next_block(block);
            }
        }
        // verify thread local block list
        #[cfg(debug_assertions)]
        {
            let mut current = self.block_header;
            while let Some(block) = current {
                // After a global gc, local list should not have blocks whose state
                // is BlockState::Unmarked or BlockState::Unallocated
                debug_assert!(
                    (block.get_state() != BlockState::Unmarked
                        && block.get_state() != BlockState::Unallocated),
                    "Block: {:?},  state: {:?} found",
                    block,
                    block.get_state()
                );
                current = self.next_block(block);
            }
        }
    }

    #[cfg(feature = "thread_local_gc")]
    /// This is the sweeping blocks work
    pub fn thread_local_release(&mut self) {
        // Update local line_unavail_state for hole searching after this GC.

        self.local_unavailable_line_mark_state = self.local_line_mark_state;
        self.release_impl(true);
        self.space.thread_local_flush_page_resource();
        let mut state_map = LOCAL_LINE_MARK_STATES_MAP.write().unwrap();
        let states = state_map.get_mut(&self.mutator_id).unwrap();
        *states = (
            self.local_line_mark_state,
            self.local_unavailable_line_mark_state,
        );
    }

    #[cfg(feature = "thread_local_gc")]
    fn release_impl(&mut self, thread_local_gc_active: bool) {
        let mut histogram = self.space.defrag.new_histogram(); // TODO defrag in local gc is different, needs to be revisited
        let line_mark_state = if crate::policy::immix::BLOCK_ONLY {
            None
        } else {
            let global_state = self.space.line_mark_state.load(atomic::Ordering::Acquire);
            let state = (global_state << Line::LOCAL_MARK_STATE_BITS) | self.local_line_mark_state;
            Some(state)
        };
        let mut current = self.block_header;
        let mark_hisogram = &mut histogram;
        while let Some(block) = current {
            if block.can_sweep(
                thread_local_gc_active,
                self.space,
                mark_hisogram,
                line_mark_state,
            ) {
                // block will be reclaimed, need to remove it from the list
                current = self.remove_block_from_list(block);
                if thread_local_gc_active {
                    #[cfg(debug_assertions)]
                    info!("A block has been released in a local gc");
                    block.thread_local_sweep(self.space);
                }
            } else {
                current = self.next_block(block);
            }
        }

        // verify thread local block list
        #[cfg(debug_assertions)]
        {
            let mut current = self.block_header;
            while let Some(block) = current {
                // After a local gc, public block may be BlockState::Unmarked
                debug_assert!(
                    block.is_block_published()
                        || (block.get_state() != BlockState::Unmarked
                            && block.get_state() != BlockState::Unallocated)
                );
                current = self.next_block(block);
            }
        }
    }
}

impl<VM: VMBinding> Allocator<VM> for ImmixAllocator<VM> {
    fn get_space(&self) -> &'static dyn Space<VM> {
        self.space as _
    }

    fn get_plan(&self) -> &'static dyn Plan<VM = VM> {
        self.plan
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
        let rtn = self.alloc_impl(size + VM::EXTRA_HEADER_BYTES, align, offset, false);

        // Check if the result is valid and return the actual object start address
        // Note that `rtn` can be null in the case of OOM
        if !rtn.is_zero() {
            debug_assert!(
                !crate::util::public_bit::is_public_object(rtn + VM::EXTRA_HEADER_BYTES),
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

    #[cfg(feature = "thread_local_gc")]
    /// This function will maintain the invariant that an object can only lives in
    /// a block allocated by its owner
    fn alloc_as_mutator(
        &mut self,
        mutator_id: u32,
        size: usize,
        align: usize,
        offset: usize,
    ) -> Address {
        // set the mutator id so that if new pages are acquired,
        // they are associated with the correct mutator
        self.mutator_id = mutator_id;

        let rtn = self.alloc_impl(size, align, offset, true);
        debug_assert!(
            mutator_id == Block::from_unaligned_address(rtn).owner(),
            "mutator_id: {} != block owner: {}",
            mutator_id,
            Block::from_unaligned_address(rtn).owner()
        );

        rtn
    }
}

impl<VM: VMBinding> ImmixAllocator<VM> {
    pub fn new(
        tls: VMThread,
        mutator_id: u32,
        space: Option<&'static dyn Space<VM>>,
        plan: &'static dyn Plan<VM = VM>,
        copy: bool,
        semantic: Option<ImmixAllocSemantics>,
        local_block_map: Option<Box<HashMap<u32, Vec<Block>>>>,
    ) -> Self {
        return ImmixAllocator {
            tls,
            mutator_id,
            space: space.unwrap().downcast_ref::<ImmixSpace<VM>>().unwrap(),
            plan,
            cursor: Address::ZERO,
            limit: Address::ZERO,
            hot: false,
            copy,
            large_cursor: Address::ZERO,
            large_limit: Address::ZERO,
            request_for_large: false,
            line: None,
            semantic,
            block_header: None,
            local_block_map,
            local_blocks: Box::new(SegQueue::new()),
            local_line_mark_state: Line::RESET_MARK_STATE,
            local_unavailable_line_mark_state: Line::RESET_MARK_STATE,
        };
    }

    pub fn immix_space(&self) -> &'static ImmixSpace<VM> {
        self.space
    }

    /// Large-object (larger than a line) bump allocation.
    fn overflow_alloc(&mut self, size: usize, align: usize, offset: usize) -> Address {
        trace!("{:?}: overflow_alloc", self.tls);
        let start = align_allocation_no_fill::<VM>(self.large_cursor, align, offset);
        let end = start + size;
        if end > self.large_limit {
            self.request_for_large = true;
            let rtn = self.alloc_slow_inline(size, align, offset);
            self.request_for_large = false;

            rtn
        } else {
            fill_alignment_gap::<VM>(self.large_cursor, start);
            self.large_cursor = end;
            start
        }
    }

    /// Bump allocate small objects into recyclable lines (i.e. holes).
    fn alloc_slow_hot(
        &mut self,
        size: usize,
        align: usize,
        offset: usize,
        alway_acquire_clean_page: bool,
    ) -> Address {
        trace!("{:?}: alloc_slow_hot", self.tls);
        if !alway_acquire_clean_page && self.acquire_recyclable_lines(size, align, offset) {
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
            let stress_test = self.plan.base().is_stress_test_gc_enabled();
            let precise_stress = self.plan.base().is_precise_stress();
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
        while self.line.is_some() || self.acquire_recyclable_block() {
            let line = self.line.unwrap();

            #[cfg(not(feature = "thread_local_gc"))]
            let unavail_state = None;
            #[cfg(not(feature = "thread_local_gc"))]
            let current_state = None;
            #[cfg(feature = "thread_local_gc")]
            let unavail_state = Some(self.local_unavailable_line_mark_state);
            #[cfg(feature = "thread_local_gc")]
            let current_state = Some(self.local_line_mark_state);

            if let Some((start_line, end_line)) =
                self.immix_space()
                    .get_next_available_lines(line, unavail_state, current_state)
            {
                // Find recyclable lines. Update the bump allocation cursor and limit.
                self.cursor = start_line.start();
                self.limit = end_line.start();
                trace!(
                    "{:?}: acquire_recyclable_lines -> {:?} [{:?}, {:?}) {:?}",
                    self.tls,
                    self.line,
                    start_line,
                    end_line,
                    self.tls
                );
                crate::util::public_bit::bzero_public_bit(self.cursor, self.limit - self.cursor);
                crate::util::memory::zero(self.cursor, self.limit - self.cursor);
                debug_assert!(
                    align_allocation_no_fill::<VM>(self.cursor, align, offset) + size <= self.limit
                );
                let block = line.block();
                self.line = if end_line == block.end_line() {
                    // Hole searching reached the end of a reusable block. Set the hole-searching cursor to None.
                    None
                } else {
                    // Update the hole-searching cursor to None.
                    Some(end_line)
                };
                return true;
            } else {
                // No more recyclable lines. Set the hole-searching cursor to None.
                self.line = None;
            }
        }
        false
    }

    /// Get a recyclable block from ImmixSpace.
    fn acquire_recyclable_block(&mut self) -> bool {
        match self
            .immix_space()
            .get_reusable_block(self.copy, self.mutator_id, self.semantic)
        {
            Some(block) => {
                trace!("{:?}: acquire_recyclable_block -> {:?}", self.tls, block);
                // Set the hole-searching cursor to the start of this block.
                self.line = Some(block.start_line());
                true
            }
            _ => false,
        }
    }

    // Get a clean block from ImmixSpace.
    fn acquire_clean_block(&mut self, size: usize, align: usize, offset: usize) -> Address {
        match self.immix_space().get_clean_block(self.tls, self.copy) {
            None => {
                // add an assertion here, assume collectors can always allocate new blocks
                debug_assert!(
                    self.semantic.is_none(),
                    "collector cannot acquire clean blocks"
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
                #[cfg(not(feature = "thread_local_gc"))]
                let block_start_offset: usize = 0;
                #[cfg(feature = "thread_local_gc")]
                let block_start_offset = Block::BLOCK_LINKED_LIST_BYTES;
                #[cfg(feature = "thread_local_gc")]
                {
                    block.set_owner(self.mutator_id);
                    // maintain the doubly linked list of blocks allocated by this allocator

                    if let Some(semantic) = self.semantic {
                        // This is collector
                        debug_assert!(
                            !VM::VMActivePlan::is_mutator(self.tls) && self.mutator_id == u32::MAX,
                            "Only collector thread should reach here"
                        );
                        match semantic {
                            ImmixAllocSemantics::Public => {
                                block.publish_block();
                            }
                            _ => {}
                        }
                        let v = self
                            .local_block_map
                            .as_deref_mut()
                            .unwrap()
                            .get_mut(&self.mutator_id);
                        if let Some(blocks) = v {
                            blocks.push(block);
                        } else {
                            self.local_block_map
                                .as_deref_mut()
                                .unwrap()
                                .insert(self.mutator_id, vec![block]);
                        }
                    } else {
                        // This is a mutator
                        debug_assert!(
                            VM::VMActivePlan::is_mutator(self.tls),
                            "Only mutator thread should reach here"
                        );
                        self.add_block_to_list(block);
                    }
                }

                if self.request_for_large {
                    self.large_cursor = block.start() + block_start_offset;
                    self.large_limit = block.end();
                } else {
                    self.cursor = block.start() + block_start_offset;
                    self.limit = block.end();
                }
                self.alloc(size, align, offset)
            }
        }
    }

    /// Return whether the TLAB has been exhausted and we need to acquire a new block. Assumes that
    /// the buffer limits have been restored using [`ImmixAllocator::restore_limit_for_stress`].
    /// Note that this function may implicitly change the limits of the allocator.
    fn require_new_block(&mut self, size: usize, align: usize, offset: usize) -> bool {
        let result = align_allocation_no_fill::<VM>(self.cursor, align, offset);
        let new_cursor = result + size;
        let insufficient_space = new_cursor > self.limit;

        // We want this function to behave as if `alloc()` has been called. Hence, we perform a
        // size check and then return the conditions where `alloc_slow_inline()` would be called
        // in an `alloc()` call, namely when both `overflow_alloc()` and `alloc_slow_hot()` fail
        // to service the allocation request
        if insufficient_space && get_maximum_aligned_size::<VM>(size, align) > Line::BYTES {
            let start = align_allocation_no_fill::<VM>(self.large_cursor, align, offset);
            let end = start + size;
            end > self.large_limit
        } else {
            // We try to acquire recyclable lines here just like `alloc_slow_hot()`
            insufficient_space && !self.acquire_recyclable_lines(size, align, offset)
        }
    }

    /// Set fake limits for the bump allocation for stress tests. The fake limit is the remaining
    /// thread local buffer size, which should be always smaller than the bump cursor. This method
    /// may be reentrant. We need to check before setting the values.
    fn set_limit_for_stress(&mut self) {
        if self.cursor < self.limit {
            let old_limit = self.limit;
            let new_limit = unsafe { Address::from_usize(self.limit - self.cursor) };
            self.limit = new_limit;
            trace!(
                "{:?}: set_limit_for_stress. normal c {} l {} -> {}",
                self.tls,
                self.cursor,
                old_limit,
                new_limit,
            );
        }

        if self.large_cursor < self.large_limit {
            let old_lg_limit = self.large_limit;
            let new_lg_limit = unsafe { Address::from_usize(self.large_limit - self.large_cursor) };
            self.large_limit = new_lg_limit;
            trace!(
                "{:?}: set_limit_for_stress. large c {} l {} -> {}",
                self.tls,
                self.large_cursor,
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
        if self.limit < self.cursor {
            let old_limit = self.limit;
            let new_limit = self.cursor + self.limit.as_usize();
            self.limit = new_limit;
            trace!(
                "{:?}: restore_limit_for_stress. normal c {} l {} -> {}",
                self.tls,
                self.cursor,
                old_limit,
                new_limit,
            );
        }

        if self.large_limit < self.large_cursor {
            let old_lg_limit = self.large_limit;
            let new_lg_limit = self.large_cursor + self.large_limit.as_usize();
            self.large_limit = new_lg_limit;
            trace!(
                "{:?}: restore_limit_for_stress. large c {} l {} -> {}",
                self.tls,
                self.large_cursor,
                old_lg_limit,
                new_lg_limit,
            );
        }
    }

    #[cfg(feature = "thread_local_gc")]
    pub fn add_block_to_list(&mut self, block: Block) {
        use crate::policy::immix::block::BlockListNode;

        unsafe {
            if let Some(header) = self.block_header {
                block.start().store(BlockListNode {
                    prev: 0,
                    next: header.start().as_usize(),
                });
                header.start().store::<usize>(block.start().as_usize())
            }
            // block will be the new header
            self.block_header = Some(block);
        }
    }

    #[cfg(feature = "thread_local_gc")]
    // This method should be called by collletor thread only
    pub fn add_blocks_to_list(&self, blocks: &Vec<Block>) {
        debug_assert!(
            !VM::VMActivePlan::is_mutator(self.tls) && self.mutator_id == u32::MAX,
            "Only collector thread should call add_blocks_to_list"
        );

        debug_assert!(
            self.block_header.is_none(),
            "collector should not have block_header"
        );

        for &block in blocks {
            self.local_blocks.push(block);
        }
    }

    #[cfg(feature = "thread_local_gc")]
    pub fn next_block(&self, block: Block) -> Option<Block> {
        use crate::policy::immix::block::BlockListNode;
        unsafe {
            let node = block.start().load::<BlockListNode>();
            let next_block_address = Address::from_usize(node.next);
            if next_block_address.is_zero() {
                None
            } else {
                Some(Block::from_aligned_address(next_block_address))
            }
        }
    }

    #[cfg(feature = "thread_local_gc")]
    pub fn remove_block_from_list(&mut self, block: Block) -> Option<Block> {
        use crate::policy::immix::block::BlockListNode;
        unsafe {
            let node = block.start().load::<BlockListNode>();
            let prev_block_address = Address::from_usize(node.prev);
            let next_block_address = Address::from_usize(node.next);
            let rtn: Option<Block> = if next_block_address.is_zero() {
                None
            } else {
                Some(Block::from_aligned_address(next_block_address))
            };

            // delete the header
            if block == self.block_header.unwrap() {
                self.block_header = rtn;
            }

            if !next_block_address.is_zero() {
                let next_node = next_block_address.load::<BlockListNode>();
                next_block_address.store(BlockListNode {
                    prev: node.prev,
                    next: next_node.next,
                });
            }

            if !prev_block_address.is_zero() {
                let prev_node = prev_block_address.load::<BlockListNode>();
                prev_block_address.store(BlockListNode {
                    prev: prev_node.prev,
                    next: node.next,
                });
            }
            rtn
        }
    }
}
