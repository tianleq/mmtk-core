pub(super) use super::super::ALLOCATOR_MAPPING;
use super::GenCopy;
use crate::plan::barriers::ObjectBarrier;
use crate::plan::generational::barrier::GenObjectBarrierSemantics;
use crate::plan::generational::create_gen_space_mapping;
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
use crate::plan::mutator_context::unreachable_prepare_func;
use crate::plan::mutator_context::Mutator;
use crate::plan::mutator_context::MutatorConfig;
use crate::plan::AllocationSemantics;
use crate::util::alloc::allocators::Allocators;
use crate::util::alloc::BumpAllocator;
use crate::util::{VMMutatorThread, VMWorkerThread};
use crate::vm::VMBinding;
use crate::MMTK;

pub fn gencopy_mutator_release<VM: VMBinding>(mutator: &mut Mutator<VM>, _tls: VMWorkerThread) {
    // reset nursery allocator
    let bump_allocator = unsafe {
        mutator
            .allocators
            .get_allocator_mut(mutator.config.allocator_mapping[AllocationSemantics::Default])
    }
    .downcast_mut::<BumpAllocator<VM>>()
    .unwrap();
    bump_allocator.reset();
}

pub fn create_gencopy_mutator<VM: VMBinding>(
    mutator_tls: VMMutatorThread,
    mmtk: &'static MMTK<VM>,
) -> Mutator<VM> {
    let gencopy = mmtk.get_plan().downcast_ref::<GenCopy<VM>>().unwrap();
    let config = MutatorConfig {
        allocator_mapping: &ALLOCATOR_MAPPING,
        space_mapping: Box::new(create_gen_space_mapping(
            mmtk.get_plan(),
            &gencopy.gen.nursery,
        )),
        prepare_func: &unreachable_prepare_func,
        release_func: &gencopy_mutator_release,
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
        barrier: Box::new(ObjectBarrier::new(GenObjectBarrierSemantics::new(
            mmtk, gencopy,
        ))),
        mutator_tls,
        config,
        plan: gencopy,
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
