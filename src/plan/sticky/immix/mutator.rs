use crate::plan::barriers::ObjectBarrier;
use crate::plan::generational::barrier::GenObjectBarrierSemantics;
use crate::plan::immix;
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
use crate::plan::mutator_context::{create_space_mapping, unreachable_prepare_func, MutatorConfig};
use crate::plan::sticky::immix::global::StickyImmix;
use crate::util::alloc::allocators::Allocators;
use crate::util::alloc::AllocatorSelector;
use crate::util::opaque_pointer::VMWorkerThread;
use crate::util::VMMutatorThread;
use crate::vm::VMBinding;
use crate::{Mutator, MMTK};

pub fn stickyimmix_mutator_release<VM: VMBinding>(mutator: &mut Mutator<VM>, tls: VMWorkerThread) {
    immix::mutator::immix_mutator_release(mutator, tls)
}

pub use immix::mutator::ALLOCATOR_MAPPING;

pub fn create_stickyimmix_mutator<VM: VMBinding>(
    mutator_tls: VMMutatorThread,
    mmtk: &'static MMTK<VM>,
) -> Mutator<VM> {
    let stickyimmix = mmtk.get_plan().downcast_ref::<StickyImmix<VM>>().unwrap();
    let config = MutatorConfig {
        allocator_mapping: &ALLOCATOR_MAPPING,
        space_mapping: Box::new({
            let mut vec =
                create_space_mapping(immix::mutator::RESERVED_ALLOCATORS, true, mmtk.get_plan());
            vec.push((AllocatorSelector::Immix(0), stickyimmix.get_immix_space()));
            vec
        }),
        prepare_func: &unreachable_prepare_func,
        release_func: &stickyimmix_mutator_release,
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
            mmtk,
            stickyimmix,
        ))),
        mutator_tls,
        config,
        plan: mmtk.get_plan(),
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
