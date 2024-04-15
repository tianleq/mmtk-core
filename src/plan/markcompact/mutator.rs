use super::MarkCompact;
use crate::plan::barriers::NoBarrier;
use crate::plan::mutator_context::create_allocator_mapping;
use crate::plan::mutator_context::create_space_mapping;
#[cfg(feature = "thread_local_gc_copying")]
use crate::plan::mutator_context::generic_thread_local_alloc_copy;
#[cfg(feature = "thread_local_gc_copying")]
use crate::plan::mutator_context::generic_thread_local_post_copy;
#[cfg(feature = "thread_local_gc")]
use crate::plan::mutator_context::generic_thread_local_prepare;
#[cfg(feature = "thread_local_gc")]
use crate::plan::mutator_context::generic_thread_local_release;
use crate::plan::mutator_context::unreachable_prepare_func;
use crate::plan::mutator_context::Mutator;
use crate::plan::mutator_context::MutatorConfig;
use crate::plan::mutator_context::ReservedAllocators;
use crate::plan::AllocationSemantics;
use crate::util::alloc::allocators::{AllocatorSelector, Allocators};
use crate::util::alloc::MarkCompactAllocator;
use crate::util::opaque_pointer::*;
use crate::vm::VMBinding;
use crate::MMTK;
use enum_map::EnumMap;

const RESERVED_ALLOCATORS: ReservedAllocators = ReservedAllocators {
    n_mark_compact: 1,
    ..ReservedAllocators::DEFAULT
};

lazy_static! {
    pub static ref ALLOCATOR_MAPPING: EnumMap<AllocationSemantics, AllocatorSelector> = {
        let mut map = create_allocator_mapping(RESERVED_ALLOCATORS, true);
        map[AllocationSemantics::Default] = AllocatorSelector::MarkCompact(0);
        map
    };
}

pub fn create_markcompact_mutator<VM: VMBinding>(
    mutator_tls: VMMutatorThread,
    mmtk: &'static MMTK<VM>,
) -> Mutator<VM> {
    let markcompact = mmtk.get_plan().downcast_ref::<MarkCompact<VM>>().unwrap();
    let config = MutatorConfig {
        allocator_mapping: &ALLOCATOR_MAPPING,
        space_mapping: Box::new({
            let mut vec = create_space_mapping(RESERVED_ALLOCATORS, true, markcompact);
            vec.push((AllocatorSelector::MarkCompact(0), markcompact.mc_space()));
            vec
        }),
        prepare_func: &unreachable_prepare_func,
        release_func: &markcompact_mutator_release,
        #[cfg(feature = "thread_local_gc")]
        thread_local_prepare_func: &generic_thread_local_prepare,
        #[cfg(feature = "thread_local_gc")]
        thread_local_release_func: &generic_thread_local_release,
        #[cfg(feature = "thread_local_gc_copying")]
        thread_local_alloc_copy_func: &generic_thread_local_alloc_copy,
        #[cfg(feature = "thread_local_gc_copying")]
        thread_local_post_copy_func: &generic_thread_local_post_copy,
    };

    Mutator {
        allocators: Allocators::<VM>::new(mutator_tls, 0, mmtk, &config.space_mapping),
        barrier: Box::new(NoBarrier),
        mutator_tls,
        config,
        plan: markcompact,
        mutator_id: 0,
        #[cfg(feature = "thread_local_gc")]
        thread_local_gc_status: 0,
        #[cfg(feature = "thread_local_gc")]
        finalizable_candidates: Box::new(Vec::new()),
        #[cfg(feature = "public_object_analysis")]
        allocation_count: 0,
        #[cfg(any(
            feature = "debug_thread_local_gc_copying",
            feature = "debug_publish_object"
        ))]
        request_id: 0,
        #[cfg(feature = "debug_thread_local_gc_copying")]
        stats: Box::new(crate::util::GCStatistics::default()),
    }
}

pub fn markcompact_mutator_release<VM: VMBinding>(
    _mutator: &mut Mutator<VM>,
    _tls: VMWorkerThread,
) {
    // reset the thread-local allocation bump pointer
    let markcompact_allocator = unsafe {
        _mutator
            .allocators
            .get_allocator_mut(_mutator.config.allocator_mapping[AllocationSemantics::Default])
    }
    .downcast_mut::<MarkCompactAllocator<VM>>()
    .unwrap();
    markcompact_allocator.reset();
}
