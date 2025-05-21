//! Mutator context for each application thread.
use crate::plan::barriers::Barrier;
use crate::plan::global::Plan;
use crate::plan::AllocationSemantics;
use crate::policy::space::Space;
use crate::util::alloc::allocators::{AllocatorSelector, Allocators};
use crate::util::alloc::Allocator;
use crate::util::{Address, ObjectReference};
use crate::util::{VMMutatorThread, VMWorkerThread};
use crate::vm::VMBinding;
use crate::MMTK;

use enum_map::EnumMap;

use super::barriers::NoBarrier;

pub(crate) type SpaceMapping<VM> = Vec<(AllocatorSelector, &'static dyn Space<VM>)>;

/// A place-holder implementation for `MutatorConfig::prepare_func` that should not be called.
/// It is the most often used by plans that sets `PlanConstraints::needs_prepare_mutator` to
/// `false`.  It is also used by `NoGC` because it must not trigger GC.
pub(crate) fn unreachable_prepare_func<VM: VMBinding>(
    _mutator: &mut Mutator<VM>,
    _tls: VMWorkerThread,
) {
    unreachable!("`MutatorConfig::prepare_func` must not be called for the current plan.")
}

/// A place-holder implementation for `MutatorConfig::release_func` that should not be called.
/// Currently only used by `NoGC`.
pub(crate) fn unreachable_release_func<VM: VMBinding>(
    _mutator: &mut Mutator<VM>,
    _tls: VMWorkerThread,
) {
    unreachable!("`MutatorConfig::release_func` must not be called for the current plan.")
}

/// A place-holder implementation for `MutatorConfig::release_func` that does nothing.
pub(crate) fn no_op_release_func<VM: VMBinding>(_mutator: &mut Mutator<VM>, _tls: VMWorkerThread) {}

// This struct is part of the Mutator struct.
// We are trying to make it fixed-sized so that VM bindings can easily define a Mutator type to have the exact same layout as our Mutator struct.
#[repr(C)]
pub struct MutatorConfig<VM: VMBinding> {
    /// Mapping between allocation semantics and allocator selector
    pub allocator_mapping: &'static EnumMap<AllocationSemantics, AllocatorSelector>,
    /// Mapping between allocator selector and spaces. Each pair represents a mapping.
    /// Put this behind a box, so it is a pointer-sized field.
    #[allow(clippy::box_collection)]
    pub space_mapping: Box<SpaceMapping<VM>>,
    /// Plan-specific code for mutator prepare. The VMWorkerThread is the worker thread that executes this prepare function.
    pub prepare_func: &'static (dyn Fn(&mut Mutator<VM>, VMWorkerThread) + Send + Sync),
    /// Plan-specific code for mutator release. The VMWorkerThread is the worker thread that executes this release function.
    pub release_func: &'static (dyn Fn(&mut Mutator<VM>, VMWorkerThread) + Send + Sync),
    #[cfg(feature = "thread_local_gc")]
    /// Plan-specific code for mutator thread_loal_prepare.
    pub thread_local_prepare_func: &'static (dyn Fn(&mut Mutator<VM>) + Send + Sync),
    #[cfg(feature = "thread_local_gc")]
    /// Plan-specific code for mutator thread_local_release. 
    pub thread_local_release_func: &'static (dyn Fn(&mut Mutator<VM>) + Send + Sync),
    #[cfg(feature = "thread_local_gc_copying")]
    /// Plan-specific code for mutator thread-local copy alloc. 
    pub thread_local_alloc_copy_func:
        &'static (dyn Fn(&mut Mutator<VM>, usize, usize, usize) -> Address + Send + Sync),
    #[cfg(feature = "thread_local_gc_copying")]
    /// Plan-specific code for mutator post thread-local copy.
    pub thread_local_post_copy_func:
        &'static (dyn Fn(&mut Mutator<VM>, ObjectReference, usize) + Send + Sync),
    #[cfg(feature = "thread_local_gc_copying")]
    pub thread_local_defrag_prepare_func: &'static (dyn Fn(&mut Mutator<VM>) + Send + Sync),
}

impl<VM: VMBinding> std::fmt::Debug for MutatorConfig<VM> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MutatorConfig:\n")?;
        f.write_str("Semantics mapping:\n")?;
        for (semantic, selector) in self.allocator_mapping.iter() {
            let space_name: &str = match self
                .space_mapping
                .iter()
                .find(|(selector_to_find, _)| selector_to_find == selector)
            {
                Some((_, space)) => space.name(),
                None => "!!!missing space here!!!",
            };
            f.write_fmt(format_args!(
                "- {:?} = {:?} ({:?})\n",
                semantic, selector, space_name
            ))?;
        }
        f.write_str("Space mapping:\n")?;
        for (selector, space) in self.space_mapping.iter() {
            f.write_fmt(format_args!("- {:?} = {:?}\n", selector, space.name()))?;
        }
        Ok(())
    }
}

/// Used to build a mutator struct
pub struct MutatorBuilder<VM: VMBinding> {
    barrier: Box<dyn Barrier<VM>>,
    /// The mutator thread that is bound with this Mutator struct.
    mutator_tls: VMMutatorThread,
    mmtk: &'static MMTK<VM>,
    config: MutatorConfig<VM>,
    #[cfg(feature = "thread_local_gc")]
    mutator_id: u32,
    #[cfg(feature = "thread_local_gc")]
    thread_local_gc_status: u32,
    #[allow(clippy::box_collection)]
    #[cfg(feature = "thread_local_gc")]
    finalizable_candidates:
        Box<Vec<<VM::VMReferenceGlue as crate::vm::ReferenceGlue<VM>>::FinalizableType>>,
    #[cfg(feature = "thread_local_gc_copying")]
    local_allocation_size: usize
}

impl<VM: VMBinding> MutatorBuilder<VM> {
    pub fn new(
        mutator_tls: VMMutatorThread,
        mmtk: &'static MMTK<VM>,
        config: MutatorConfig<VM>,
    ) -> Self {
        MutatorBuilder {
            barrier: Box::new(NoBarrier),
            mutator_tls,
            mmtk,
            config,
            #[cfg(feature = "thread_local_gc")]
            mutator_id: 0,
            #[cfg(feature = "thread_local_gc")]
            thread_local_gc_status: 0,
            #[cfg(feature = "thread_local_gc")]
            finalizable_candidates: Box::new(Vec::new()),
            #[cfg(feature = "thread_local_gc_copying")]
            local_allocation_size: 0,
        }
    }

    pub fn barrier(mut self, barrier: Box<dyn Barrier<VM>>) -> Self {
        self.barrier = barrier;
        self
    }

    pub fn mutator_id(mut self, mutator_id: u32) -> Self {
        self.mutator_id = mutator_id;
        self
    }

    pub fn build(self) -> Mutator<VM> {
        Mutator {
            allocators: Allocators::<VM>::new(
                self.mutator_tls,
                #[cfg(feature = "thread_local_gc")]
                self.mutator_id,
                self.mmtk,
                &self.config.space_mapping,
            ),
            barrier: self.barrier,
            mutator_tls: self.mutator_tls,
            plan: self.mmtk.get_plan(),
            config: self.config,
            mutator_id: self.mutator_id,
            thread_local_gc_status: self.thread_local_gc_status,
            finalizable_candidates: self.finalizable_candidates,
            local_allocation_size: self.local_allocation_size,
            #[cfg(any(
                feature = "debug_thread_local_gc_copying",
                feature = "debug_publish_object"
            ))]
            request_id: 0,
            #[cfg(feature = "debug_thread_local_gc_copying")]
            stats: Box::new(crate::util::LocalGCStatistics::default()),
        }
    }
}

/// A mutator is a per-thread data structure that manages allocations and barriers. It is usually highly coupled with the language VM.
/// It is recommended for MMTk users 1) to have a mutator struct of the same layout in the thread local storage that can be accessed efficiently,
/// and 2) to implement fastpath allocation and barriers for the mutator in the VM side.
// We are trying to make this struct fixed-sized so that VM bindings can easily define a type to have the exact same layout as this struct.
// Currently Mutator is fixed sized, and we should try keep this invariant:
// - Allocators are fixed-length arrays of allocators.
// - MutatorConfig only has pointers/refs (including fat pointers), and is fixed sized.
#[repr(C)]
pub struct Mutator<VM: VMBinding> {
    pub(crate) allocators: Allocators<VM>,
    /// Holds some thread-local states for the barrier.
    pub barrier: Box<dyn Barrier<VM>>,
    /// The mutator thread that is bound with this Mutator struct.
    pub mutator_tls: VMMutatorThread,
    pub(crate) plan: &'static dyn Plan<VM = VM>,
    pub(crate) config: MutatorConfig<VM>,
    #[cfg(feature = "thread_local_gc")]
    pub mutator_id: u32,
    #[cfg(feature = "thread_local_gc")]
    pub thread_local_gc_status: u32,
    #[cfg(feature = "thread_local_gc")]
    pub finalizable_candidates:
        Box<Vec<<VM::VMReferenceGlue as crate::vm::ReferenceGlue<VM>>::FinalizableType>>,
    #[cfg(any(
        feature = "debug_thread_local_gc_copying",
        feature = "debug_publish_object"
    ))]
    pub request_id: usize,
    #[cfg(feature = "debug_thread_local_gc_copying")]
    pub(crate) stats: Box<crate::util::LocalGCStatistics>,
    #[cfg(feature = "thread_local_gc_copying")]
    pub(crate) local_allocation_size: usize,
}

impl<VM: VMBinding> MutatorContext<VM> for Mutator<VM> {
    fn prepare(&mut self, tls: VMWorkerThread) {
        (*self.config.prepare_func)(self, tls)
    }
    fn release(&mut self, tls: VMWorkerThread) {
        (*self.config.release_func)(self, tls)
    }

    // Note that this method is slow, and we expect VM bindings that care about performance to implement allocation fastpath sequence in their bindings.
    fn alloc(
        &mut self,
        size: usize,
        align: usize,
        offset: usize,
        semantics: AllocationSemantics,
    ) -> Address {
        unsafe {
            self.allocators
                .get_allocator_mut(self.config.allocator_mapping[semantics])
        }
        .alloc(size, align, offset)
    }

    fn alloc_slow(
        &mut self,
        size: usize,
        align: usize,
        offset: usize,
        allocator: AllocationSemantics,
    ) -> Address {
        unsafe {
            self.allocators
                .get_allocator_mut(self.config.allocator_mapping[allocator])
        }
        .alloc_slow(size, align, offset)
    }

    // Note that this method is slow, and we expect VM bindings that care about performance to implement allocation fastpath sequence in their bindings.
    fn post_alloc(
        &mut self,
        refer: ObjectReference,
        _bytes: usize,
        semantics: AllocationSemantics,
    ) {
        #[cfg(feature = "thread_local_gc")]
        use crate::util::alloc::LargeObjectAllocator;
        #[cfg(feature = "debug_publish_object")]
        use crate::util::object_extra_header_metadata;
        let space = unsafe {
            self.allocators
                .get_allocator_mut(self.config.allocator_mapping[semantics])
        }
        .get_space();
        space.initialize_object_metadata(refer, true);

        // Large object allocation always go through the slow-path, so it is fine to
        // do the following book-keeping in post_alloc(only executed in slow-path)
        #[cfg(feature = "thread_local_gc")]
        if semantics == AllocationSemantics::Los {
            // store los objects into a local set
            let allocator = unsafe {
                self.allocators
                    .get_allocator_mut(self.config.allocator_mapping[semantics])
                    .downcast_mut::<LargeObjectAllocator<VM>>()
                    .unwrap()
            };
            allocator.add_los_objects(refer);
            // large object need to record its owner
            let metadata = usize::try_from(self.mutator_id).unwrap();
            unsafe {
                #[cfg(feature = "extra_header")]
                let offset = 8;
                #[cfg(not(feature = "extra_header"))]
                let offset = 0;
                let metadata_address =
                    crate::util::conversions::page_align_down(refer.to_object_start::<VM>());
                metadata_address.store(metadata);
                // Store request_id|mutator_id into the extra header
                #[cfg(feature = "debug_publish_object")]
                ((metadata_address + offset) as Address)
                    .store(metadata | self.request_id << object_extra_header_metadata::SHIFT);
                debug_assert!(
                    crate::util::conversions::is_page_aligned(Address::from_usize(
                        refer.to_object_start::<VM>().as_usize() - offset - 8
                    )),
                    "los object is not aligned"
                );
            }
            #[cfg(feature = "debug_thread_local_gc_copying")]
            {
                self.stats.los_bytes_allocated += _bytes;
            }
        } else {
            #[cfg(feature = "debug_publish_object")]
            {

                let metadata: usize = usize::try_from(self.mutator_id).unwrap();
                object_extra_header_metadata::store_extra_header_metadata::<VM, usize>(
                    refer, metadata,
                );
            }
        }


    }

    fn get_tls(&self) -> VMMutatorThread {
        self.mutator_tls
    }

    fn barrier(&mut self) -> &mut dyn Barrier<VM> {
        &mut *self.barrier
    }

    #[cfg(feature = "thread_local_gc_copying")]
    fn alloc_copy(&mut self, size: usize, align: usize, offset: usize) -> Address {
        (*self.config.thread_local_alloc_copy_func)(self, size, align, offset)
    }

    #[cfg(feature = "thread_local_gc_copying")]
    fn post_copy(&mut self, obj: ObjectReference, bytes: usize) {
        (*self.config.thread_local_post_copy_func)(self, obj, bytes)
    }

    #[cfg(feature = "thread_local_gc_copying")]
    fn thread_local_defrag_prepare(&mut self) {
        (*self.config.thread_local_defrag_prepare_func)(self)
    }

    #[cfg(feature = "thread_local_gc")]
    fn thread_local_prepare(&mut self) {
        (*self.config.thread_local_prepare_func)(self)
    }

    #[cfg(feature = "thread_local_gc")]
    fn thread_local_release(&mut self) {
        (*self.config.thread_local_release_func)(self)
    }
}

impl<VM: VMBinding> Mutator<VM> {
    /// Get all the valid allocator selector (no duplicate)
    fn get_all_allocator_selectors(&self) -> Vec<AllocatorSelector> {
        use itertools::Itertools;
        self.config
            .allocator_mapping
            .iter()
            .map(|(_, selector)| *selector)
            .sorted()
            .dedup()
            .filter(|selector| *selector != AllocatorSelector::None)
            .collect()
    }

    /// Inform each allocator about destroying. Call allocator-specific on destroy methods.
    pub fn on_destroy(&mut self) {
        for selector in self.get_all_allocator_selectors() {
            unsafe { self.allocators.get_allocator_mut(selector) }.on_mutator_destroy();
        }
    }

    /// Get the allocator for the selector.
    ///
    /// # Safety
    /// The selector needs to be valid, and points to an allocator that has been initialized.
    /// [`crate::memory_manager::get_allocator_mapping`] can be used to get a selector.
    pub unsafe fn allocator(&self, selector: AllocatorSelector) -> &dyn Allocator<VM> {
        self.allocators.get_allocator(selector)
    }

    /// Get the mutable allocator for the selector.
    ///
    /// # Safety
    /// The selector needs to be valid, and points to an allocator that has been initialized.
    /// [`crate::memory_manager::get_allocator_mapping`] can be used to get a selector.
    pub unsafe fn allocator_mut(&mut self, selector: AllocatorSelector) -> &mut dyn Allocator<VM> {
        self.allocators.get_allocator_mut(selector)
    }

    /// Get the allocator of a concrete type for the selector.
    ///
    /// # Safety
    /// The selector needs to be valid, and points to an allocator that has been initialized.
    /// [`crate::memory_manager::get_allocator_mapping`] can be used to get a selector.
    pub unsafe fn allocator_impl<T: Allocator<VM>>(&self, selector: AllocatorSelector) -> &T {
        self.allocators.get_typed_allocator(selector)
    }

    /// Get the mutable allocator of a concrete type for the selector.
    ///
    /// # Safety
    /// The selector needs to be valid, and points to an allocator that has been initialized.
    /// [`crate::memory_manager::get_allocator_mapping`] can be used to get a selector.
    pub unsafe fn allocator_impl_mut<T: Allocator<VM>>(
        &mut self,
        selector: AllocatorSelector,
    ) -> &mut T {
        self.allocators.get_typed_allocator_mut(selector)
    }

    /// Return the base offset from a mutator pointer to the allocator specified by the selector.
    pub fn get_allocator_base_offset(selector: AllocatorSelector) -> usize {
        use crate::util::alloc::*;
        use memoffset::offset_of;
        use std::mem::size_of;
        offset_of!(Mutator<VM>, allocators)
            + match selector {
                AllocatorSelector::BumpPointer(index) => {
                    offset_of!(Allocators<VM>, bump_pointer)
                        + size_of::<BumpAllocator<VM>>() * index as usize
                }
                AllocatorSelector::FreeList(index) => {
                    offset_of!(Allocators<VM>, free_list)
                        + size_of::<FreeListAllocator<VM>>() * index as usize
                }
                AllocatorSelector::Immix(index) => {
                    offset_of!(Allocators<VM>, immix)
                        + size_of::<ImmixAllocator<VM>>() * index as usize
                }
                AllocatorSelector::LargeObject(index) => {
                    offset_of!(Allocators<VM>, large_object)
                        + size_of::<LargeObjectAllocator<VM>>() * index as usize
                }
                AllocatorSelector::Malloc(index) => {
                    offset_of!(Allocators<VM>, malloc)
                        + size_of::<MallocAllocator<VM>>() * index as usize
                }
                AllocatorSelector::MarkCompact(index) => {
                    offset_of!(Allocators<VM>, markcompact)
                        + size_of::<MarkCompactAllocator<VM>>() * index as usize
                }
                AllocatorSelector::None => panic!("Expect a valid AllocatorSelector, found None"),
            }
    }

    #[cfg(feature = "debug_thread_local_gc_copying")]
    pub fn print_stats_before_thread_local_gc(&self) {
        use crate::util::{alloc::ImmixAllocator, TOTAL_ALLOCATION_BYTES};
       
        use std::{fs::OpenOptions, io::Write};

        let allocator = unsafe {
            self.allocators
                .get_allocator(self.config.allocator_mapping[AllocationSemantics::Default])
                .downcast_ref::<ImmixAllocator<VM>>()
                .unwrap()
        };
        allocator.collect_thread_local_heap_stats();

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(format!("/home/tianleq/misc/local-gc-debug-logs/{}-local-gc-stats.log", self.mutator_id))
            .unwrap();

        writeln!(file,
            "Before Local {}->{} | bytes allocated: {}, bytes_published: {}, los_bytes_allocated: {}, los_bytes_published {}, blocks_published: {}, number_of_clean_blocks_acquired: {}, number_of_local_reusable_blocks: {}, number_of_live_blocks: {}, number_of_live_public_blocks: {}, number_of_los_pages: {}, total_allocation: {}",
            self.mutator_id,
            self.request_id,
            self.stats.bytes_allocated,
            self.stats.bytes_published,
            self.stats.los_bytes_allocated,
            self.stats.los_bytes_published,
            self.stats.blocks_published,
            self.stats.number_of_clean_blocks_acquired,
            self.stats.number_of_local_reusable_blocks,
            self.stats.number_of_live_blocks,
            self.stats.number_of_live_public_blocks,
            self.stats.number_of_los_pages,
            TOTAL_ALLOCATION_BYTES.load(atomic::Ordering::SeqCst)
        ).unwrap();

    }

    #[cfg(feature = "debug_thread_local_gc_copying")]
    pub fn print_stats_after_thread_local_gc(&self) {
        use crate::util::TOTAL_ALLOCATION_BYTES;
        use std::{fs::OpenOptions, io::Write};

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(format!("/home/tianleq/misc/local-gc-debug-logs/{}-local-gc-stats.log", self.mutator_id))
            .unwrap();

            writeln!(file, 
                "After Local {}->{} | bytes_copied: {}, number_of_blocks_acquired_for_evacuation: {}, number_of_blocks_freed: {}, number_of_global_reusable_blocks: {}, number_of_local_reusable_blocks: {}, number_of_live_blocks: {}, number_of_live_public_blocks: {}, number_of_los_pages_freed: {}, number_of_los_pages: {}, number_of_free_lines_in_global_reusable_blocks:{}, number_of_free_lines_in_local_reusable_blocks: {}, total_allocation: {}",
                self.mutator_id,
                self.request_id,
                self.stats.bytes_copied,
                self.stats.number_of_blocks_acquired_for_evacuation,
                self.stats.number_of_blocks_freed,
                self.stats.number_of_global_reusable_blocks,
                self.stats.number_of_local_reusable_blocks,
                self.stats.number_of_live_blocks,
                self.stats.number_of_live_public_blocks,
                self.stats.number_of_los_pages_freed,
                self.stats.number_of_los_pages,
                self.stats.number_of_free_lines_in_global_reusable_blocks,
                self.stats.number_of_free_lines_in_local_reusable_blocks,
                TOTAL_ALLOCATION_BYTES.load(atomic::Ordering::SeqCst)
            ).unwrap();


    }

    #[cfg(feature = "debug_thread_local_gc_copying")]
    pub fn reset_stats(&mut self) {
        self.stats.bytes_allocated = 0;
        self.stats.bytes_copied = 0;
        self.stats.bytes_published = 0;
        self.stats.number_of_blocks_acquired_for_evacuation = 0;
        self.stats.number_of_clean_blocks_acquired = 0;
        self.stats.number_of_blocks_freed = 0;
        self.stats.blocks_published = 0;
        self.stats.number_of_free_lines_in_global_reusable_blocks = 0;
        self.stats.number_of_free_lines_in_local_reusable_blocks = 0;
        self.stats.number_of_live_public_blocks = 0;
        self.stats.number_of_live_blocks = 0;
        self.stats.los_bytes_allocated = 0;
        self.stats.los_bytes_published = 0;
        self.stats.number_of_los_pages_freed = 0;
    }

}

/// Each GC plan should provide their implementation of a MutatorContext. *Note that this trait is no longer needed as we removed
/// per-plan mutator implementation and we will remove this trait as well in the future.*
// TODO: We should be able to remove this trait, as we removed per-plan mutator implementation, and there is no other type that implements this trait.
// The Mutator struct above is the only type that implements this trait. We should be able to merge them.
pub trait MutatorContext<VM: VMBinding>: Send + 'static {
    /// Do the prepare work for this mutator.
    fn prepare(&mut self, tls: VMWorkerThread);
    /// Do the release work for this mutator.
    fn release(&mut self, tls: VMWorkerThread);
    /// Allocate memory for an object.
    ///
    /// Arguments:
    /// * `size`: the number of bytes required for the object.
    /// * `align`: required alignment for the object.
    /// * `offset`: offset associated with the alignment. The result plus the offset will be aligned to the given alignment.
    /// * `allocator`: the allocation semantic used for this object.
    fn alloc(
        &mut self,
        size: usize,
        align: usize,
        offset: usize,
        allocator: AllocationSemantics,
    ) -> Address;
    /// The slow path allocation. This is only useful when the binding
    /// implements the fast path allocation, and would like to explicitly
    /// call the slow path after the fast path allocation fails.
    fn alloc_slow(
        &mut self,
        size: usize,
        align: usize,
        offset: usize,
        allocator: AllocationSemantics,
    ) -> Address;
    /// Perform post-allocation actions.  For many allocators none are
    /// required.
    ///
    /// Arguments:
    /// * `refer`: the newly allocated object.
    /// * `bytes`: the size of the space allocated (in bytes).
    /// * `allocator`: the allocation semantic used.
    fn post_alloc(&mut self, refer: ObjectReference, bytes: usize, allocator: AllocationSemantics);
    /// Flush per-mutator remembered sets and create GC work for the remembered sets.
    fn flush_remembered_sets(&mut self) {
        self.barrier().flush();
    }
    /// Flush the mutator context.
    fn flush(&mut self) {
        self.flush_remembered_sets();
    }
    /// Get the mutator thread for this mutator context. This is the same value as the argument supplied in
    /// [`crate::memory_manager::bind_mutator`] when this mutator is created.
    fn get_tls(&self) -> VMMutatorThread;
    /// Get active barrier trait object
    fn barrier(&mut self) -> &mut dyn Barrier<VM>;

    #[cfg(feature = "thread_local_gc_copying")]
    fn alloc_copy(&mut self, size: usize, align: usize, offset: usize) -> Address;

    #[cfg(feature = "thread_local_gc_copying")]
    fn post_copy(&mut self, obj: ObjectReference, bytes: usize);

    #[cfg(feature = "thread_local_gc_copying")]
    fn thread_local_defrag_prepare(&mut self);

    #[cfg(feature = "thread_local_gc")]
    fn thread_local_prepare(&mut self);

    #[cfg(feature = "thread_local_gc")]
    fn thread_local_release(&mut self);
}

/// This is used for plans to indicate the number of allocators reserved for the plan.
/// This is used as a parameter for creating allocator/space mapping.
/// A plan is required to reserve the first few allocators. For example, if n_bump_pointer is 1,
/// it means the first bump pointer allocator will be reserved for the plan (and the plan should
/// initialize its mapping itself), and the spaces in common/base plan will use the following bump
/// pointer allocators.
#[allow(dead_code)]
#[derive(Default)]
pub(crate) struct ReservedAllocators {
    pub n_bump_pointer: u8,
    pub n_large_object: u8,
    pub n_malloc: u8,
    pub n_immix: u8,
    pub n_mark_compact: u8,
    pub n_free_list: u8,
}

impl ReservedAllocators {
    pub const DEFAULT: Self = ReservedAllocators {
        n_bump_pointer: 0,
        n_large_object: 0,
        n_malloc: 0,
        n_immix: 0,
        n_mark_compact: 0,
        n_free_list: 0,
    };
    /// check if the number of each allocator is okay. Panics if any allocator exceeds the max number.
    fn validate(&self) {
        use crate::util::alloc::allocators::*;
        assert!(
            self.n_bump_pointer as usize <= MAX_BUMP_ALLOCATORS,
            "Allocator mapping declared more bump pointer allocators than the max allowed."
        );
        assert!(
            self.n_large_object as usize <= MAX_LARGE_OBJECT_ALLOCATORS,
            "Allocator mapping declared more large object allocators than the max allowed."
        );
        assert!(
            self.n_malloc as usize <= MAX_MALLOC_ALLOCATORS,
            "Allocator mapping declared more malloc allocators than the max allowed."
        );
        assert!(
            self.n_immix as usize <= MAX_IMMIX_ALLOCATORS,
            "Allocator mapping declared more immix allocators than the max allowed."
        );
        assert!(
            self.n_mark_compact as usize <= MAX_MARK_COMPACT_ALLOCATORS,
            "Allocator mapping declared more mark compact allocators than the max allowed."
        );
        assert!(
            self.n_free_list as usize <= MAX_FREE_LIST_ALLOCATORS,
            "Allocator mapping declared more free list allocators than the max allowed."
        );
    }

    // We may add more allocators from common/base plan after reserved allocators.

    fn add_bump_pointer_allocator(&mut self) -> AllocatorSelector {
        let selector = AllocatorSelector::BumpPointer(self.n_bump_pointer);
        self.n_bump_pointer += 1;
        selector
    }
    fn add_large_object_allocator(&mut self) -> AllocatorSelector {
        let selector = AllocatorSelector::LargeObject(self.n_large_object);
        self.n_large_object += 1;
        selector
    }
    #[allow(dead_code)]
    fn add_malloc_allocator(&mut self) -> AllocatorSelector {
        let selector = AllocatorSelector::Malloc(self.n_malloc);
        self.n_malloc += 1;
        selector
    }
    #[allow(dead_code)]
    fn add_immix_allocator(&mut self) -> AllocatorSelector {
        let selector = AllocatorSelector::Immix(self.n_immix);
        self.n_immix += 1;
        selector
    }
    #[allow(dead_code)]
    fn add_mark_compact_allocator(&mut self) -> AllocatorSelector {
        let selector = AllocatorSelector::MarkCompact(self.n_mark_compact);
        self.n_mark_compact += 1;
        selector
    }
    #[allow(dead_code)]
    fn add_free_list_allocator(&mut self) -> AllocatorSelector {
        let selector = AllocatorSelector::FreeList(self.n_free_list);
        self.n_free_list += 1;
        selector
    }
}

/// Create an allocator mapping for spaces in Common/BasePlan for a plan. A plan should reserve its own allocators.
///
/// # Arguments
/// * `reserved`: the number of reserved allocators for the plan specific policies.
/// * `include_common_plan`: whether the plan uses common plan. If a plan uses CommonPlan, we will initialize allocator mapping for spaces in CommonPlan.
pub(crate) fn create_allocator_mapping(
    mut reserved: ReservedAllocators,
    include_common_plan: bool,
) -> EnumMap<AllocationSemantics, AllocatorSelector> {
    // If we need to add new allocators, or new spaces, we need to make sure the allocator we assign here matches the allocator
    // we used in create_space_mapping(). The easiest way is to add the space/allocator mapping in the same order. So for any modification to this
    // function, please check the other function.

    let mut map = EnumMap::<AllocationSemantics, AllocatorSelector>::default();

    // spaces in base plan

    #[cfg(feature = "code_space")]
    {
        map[AllocationSemantics::Code] = reserved.add_bump_pointer_allocator();
        map[AllocationSemantics::LargeCode] = reserved.add_bump_pointer_allocator();
    }

    #[cfg(feature = "ro_space")]
    {
        map[AllocationSemantics::ReadOnly] = reserved.add_bump_pointer_allocator();
    }

    // spaces in common plan

    if include_common_plan {
        map[AllocationSemantics::Immortal] = reserved.add_bump_pointer_allocator();
        map[AllocationSemantics::Los] = reserved.add_large_object_allocator();
        // TODO: This should be freelist allocator once we use marksweep for nonmoving space.
        map[AllocationSemantics::NonMoving] = reserved.add_bump_pointer_allocator();
    }

    reserved.validate();
    map
}

/// Create a space mapping for spaces in Common/BasePlan for a plan. A plan should reserve its own allocators.
///
/// # Arguments
/// * `reserved`: the number of reserved allocators for the plan specific policies.
/// * `include_common_plan`: whether the plan uses common plan. If a plan uses CommonPlan, we will initialize allocator mapping for spaces in CommonPlan.
/// * `plan`: the reference to the plan.
pub(crate) fn create_space_mapping<VM: VMBinding>(
    mut reserved: ReservedAllocators,
    include_common_plan: bool,
    plan: &'static dyn Plan<VM = VM>,
) -> Vec<(AllocatorSelector, &'static dyn Space<VM>)> {
    // If we need to add new allocators, or new spaces, we need to make sure the allocator we assign here matches the allocator
    // we used in create_space_mapping(). The easiest way is to add the space/allocator mapping in the same order. So for any modification to this
    // function, please check the other function.

    let mut vec: Vec<(AllocatorSelector, &'static dyn Space<VM>)> = vec![];

    // spaces in BasePlan

    #[cfg(feature = "code_space")]
    {
        vec.push((
            reserved.add_bump_pointer_allocator(),
            &plan.base().code_space,
        ));
        vec.push((
            reserved.add_bump_pointer_allocator(),
            &plan.base().code_lo_space,
        ));
    }

    #[cfg(feature = "ro_space")]
    vec.push((reserved.add_bump_pointer_allocator(), &plan.base().ro_space));

    // spaces in CommonPlan

    if include_common_plan {
        vec.push((
            reserved.add_bump_pointer_allocator(),
            plan.common().get_immortal(),
        ));
        vec.push((
            reserved.add_large_object_allocator(),
            plan.common().get_los(),
        ));
        // TODO: This should be freelist allocator once we use marksweep for nonmoving space.
        vec.push((
            reserved.add_bump_pointer_allocator(),
            plan.common().get_nonmoving(),
        ));
    }

    reserved.validate();
    vec
}

#[cfg(feature = "thread_local_gc")]
pub fn generic_thread_local_prepare<VM: VMBinding>(_mutator: &mut Mutator<VM>) {}

#[cfg(feature = "thread_local_gc")]
pub fn generic_thread_local_release<VM: VMBinding>(_mutator: &mut Mutator<VM>) {}

#[cfg(feature = "thread_local_gc_copying")]
pub fn generic_thread_local_alloc_copy<VM: VMBinding>(
    _mutator: &mut Mutator<VM>,
    _size: usize,
    _align: usize,
    _offset: usize,
) -> Address {
    Address::ZERO
}

#[cfg(feature = "thread_local_gc_copying")]
pub fn generic_thread_local_post_copy<VM: VMBinding>(
    _mutator: &mut Mutator<VM>,
    _obj: ObjectReference,
    _bytes: usize,
) {
}

#[cfg(feature = "thread_local_gc_copying")]
pub fn generic_thread_local_defrag_prepare<VM: VMBinding>(
    _mutator: &mut Mutator<VM>,
) {
}
