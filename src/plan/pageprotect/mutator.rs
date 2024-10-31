use super::PageProtect;
#[cfg(feature = "thread_local_gc_copying")]
use crate::plan::mutator_context::generic_thread_local_alloc_copy;
#[cfg(feature = "thread_local_gc_copying")]
use crate::plan::mutator_context::generic_thread_local_defrag_prepare;
#[cfg(feature = "thread_local_gc_copying")]
use crate::plan::mutator_context::generic_thread_local_post_copy;
#[cfg(feature = "thread_local_gc")]
use crate::plan::mutator_context::generic_thread_local_prepare;
#[cfg(feature = "thread_local_gc")]
use crate::plan::mutator_context::generic_thread_local_release;
use crate::plan::mutator_context::no_op_release_func;
use crate::plan::mutator_context::unreachable_prepare_func;
use crate::plan::mutator_context::Mutator;
use crate::plan::mutator_context::MutatorConfig;
use crate::plan::mutator_context::{
    create_allocator_mapping, create_space_mapping, ReservedAllocators,
};
use crate::plan::AllocationSemantics;
use crate::util::alloc::allocators::{AllocatorSelector, Allocators};
use crate::vm::VMBinding;
use crate::MMTK;
use crate::{plan::barriers::NoBarrier, util::opaque_pointer::VMMutatorThread};
use enum_map::EnumMap;

const RESERVED_ALLOCATORS: ReservedAllocators = ReservedAllocators {
    n_large_object: 1,
    ..ReservedAllocators::DEFAULT
};

lazy_static! {
    pub static ref ALLOCATOR_MAPPING: EnumMap<AllocationSemantics, AllocatorSelector> = {
        let mut map = create_allocator_mapping(RESERVED_ALLOCATORS, true);
        map[AllocationSemantics::Default] = AllocatorSelector::LargeObject(0);
        map
    };
}

/// Create a mutator instance.
/// Every object is allocated to LOS.
pub fn create_pp_mutator<VM: VMBinding>(
    mutator_tls: VMMutatorThread,
    mmtk: &'static MMTK<VM>,
) -> Mutator<VM> {
    let page = mmtk.get_plan().downcast_ref::<PageProtect<VM>>().unwrap();
    let config = MutatorConfig {
        allocator_mapping: &ALLOCATOR_MAPPING,
        space_mapping: Box::new({
            let mut vec = create_space_mapping(RESERVED_ALLOCATORS, true, page);
            vec.push((AllocatorSelector::LargeObject(0), &page.space));
            vec
        }),
        prepare_func: &unreachable_prepare_func,
        release_func: &no_op_release_func,
        #[cfg(feature = "thread_local_gc")]
        thread_local_prepare_func: &generic_thread_local_prepare,
        #[cfg(feature = "thread_local_gc")]
        thread_local_release_func: &generic_thread_local_release,
        #[cfg(feature = "thread_local_gc_copying")]
        thread_local_alloc_copy_func: &generic_thread_local_alloc_copy,
        #[cfg(feature = "thread_local_gc_copying")]
        thread_local_post_copy_func: &generic_thread_local_post_copy,
        #[cfg(feature = "thread_local_gc_copying")]
        thread_local_defrag_prepare_func: &generic_thread_local_defrag_prepare,
    };

    Mutator {
        allocators: Allocators::<VM>::new(mutator_tls, 0, mmtk, &config.space_mapping),
        barrier: Box::new(NoBarrier),
        mutator_tls,
        config,
        plan: page,
        mutator_id: 0,
        #[cfg(feature = "thread_local_gc")]
        thread_local_gc_status: 0,
        #[cfg(feature = "thread_local_gc")]
        finalizable_candidates: Box::new(Vec::new()),

        #[cfg(any(
            feature = "debug_thread_local_gc_copying",
            feature = "debug_publish_object",
            feature = "extra_header"
        ))]
        request_id: 0,
        #[cfg(feature = "debug_thread_local_gc_copying")]
        stats: Box::new(crate::util::LocalGCStatistics::default()),
        #[cfg(feature = "thread_local_gc_copying")]
        local_allocation_size: 0,
        #[cfg(feature = "extra_header")]
        in_request: false,
        #[cfg(feature = "extra_header")]
        request_stats: Box::new(
            crate::scheduler::thread_local_gc_work::RequestScopeStats::default(),
        ),
    }
}
