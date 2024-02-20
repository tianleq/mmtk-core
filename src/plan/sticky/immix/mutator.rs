use crate::plan::barriers::ObjectBarrier;
use crate::plan::generational::barrier::GenObjectBarrierSemantics;
use crate::plan::immix;
use crate::plan::mutator_context::{
    create_space_mapping, generic_thread_local_alloc_copy, generic_thread_local_post_copy,
    generic_thread_local_prepare, generic_thread_local_release, MutatorConfig,
};
use crate::plan::sticky::immix::global::StickyImmix;
use crate::util::alloc::allocators::Allocators;
use crate::util::alloc::AllocatorSelector;
use crate::util::opaque_pointer::VMWorkerThread;
use crate::util::VMMutatorThread;
use crate::vm::VMBinding;
use crate::{Mutator, MMTK};

pub fn stickyimmix_mutator_prepare<VM: VMBinding>(mutator: &mut Mutator<VM>, tls: VMWorkerThread) {
    immix::mutator::immix_mutator_prepare(mutator, tls)
}

pub fn stickyimmix_mutator_release<VM: VMBinding>(mutator: &mut Mutator<VM>, tls: VMWorkerThread) {
    immix::mutator::immix_mutator_release(mutator, tls)
}

pub use immix::mutator::ALLOCATOR_MAPPING;

pub fn create_stickyimmix_mutator<VM: VMBinding>(
    mutator_tls: VMMutatorThread,
    mmtk: &'static MMTK<VM>,
) -> Mutator<VM> {
    let stickyimmix = mmtk.plan.downcast_ref::<StickyImmix<VM>>().unwrap();
    let config = MutatorConfig {
        allocator_mapping: &ALLOCATOR_MAPPING,
        space_mapping: Box::new({
            let mut vec =
                create_space_mapping(immix::mutator::RESERVED_ALLOCATORS, true, &*mmtk.plan);
            vec.push((AllocatorSelector::Immix(0), stickyimmix.get_immix_space()));
            vec
        }),
        prepare_func: &stickyimmix_mutator_prepare,
        release_func: &stickyimmix_mutator_release,
        #[cfg(feature = "thread_local_gc")]
        thread_local_prepare_func: &generic_thread_local_prepare,
        #[cfg(feature = "thread_local_gc")]
        thread_local_release_func: &generic_thread_local_release,
        #[cfg(feature = "thread_local_gc")]
        thread_local_alloc_copy_func: &generic_thread_local_alloc_copy,
        #[cfg(feature = "thread_local_gc")]
        thread_local_post_copy_func: &generic_thread_local_post_copy,
    };

    Mutator {
        allocators: Allocators::<VM>::new(mutator_tls, 0, &*mmtk.plan, &config.space_mapping),
        barrier: Box::new(ObjectBarrier::new(GenObjectBarrierSemantics::new(
            mmtk,
            stickyimmix,
        ))),
        mutator_tls,
        config,
        plan: &*mmtk.plan,
        mutator_id: 0,
        #[cfg(feature = "thread_local_gc")]
        thread_local_gc_status: 0,
        #[cfg(feature = "thread_local_gc")]
        finalizable_candidates: Box::new(Vec::new()),
        #[cfg(feature = "public_object_analysis")]
        allocation_count: 0,
        #[cfg(feature = "public_object_analysis")]
        bytes_allocated: 0,
        #[cfg(all(feature = "thread_local_gc", feature = "debug_publish_object"))]
        request_id: 0,
        #[cfg(feature = "public_object_analysis")]
        global_request_id: 0,
    }
}
