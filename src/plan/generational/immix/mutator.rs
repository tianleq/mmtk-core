pub(super) use super::super::ALLOCATOR_MAPPING;
use crate::plan::barriers::ObjectBarrier;
use crate::plan::generational::barrier::GenObjectBarrierSemantics;
use crate::plan::generational::create_gen_space_mapping;
use crate::plan::generational::immix::GenImmix;
use crate::plan::mutator_context::Mutator;
use crate::plan::mutator_context::MutatorConfig;
use crate::plan::AllocationSemantics;
use crate::util::alloc::allocators::Allocators;
use crate::util::alloc::BumpAllocator;
use crate::util::{VMMutatorThread, VMWorkerThread};
use crate::vm::VMBinding;
use crate::MMTK;

pub fn genimmix_mutator_prepare<VM: VMBinding>(_mutator: &mut Mutator<VM>, _tls: VMWorkerThread) {}

pub fn genimmix_mutator_release<VM: VMBinding>(mutator: &mut Mutator<VM>, _tls: VMWorkerThread) {
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

pub fn create_genimmix_mutator<VM: VMBinding>(
    mutator_tls: VMMutatorThread,
    mmtk: &'static MMTK<VM>,
) -> Mutator<VM> {
    let genimmix = mmtk.plan.downcast_ref::<GenImmix<VM>>().unwrap();
    let config = MutatorConfig {
        allocator_mapping: &*ALLOCATOR_MAPPING,
        space_mapping: Box::new(create_gen_space_mapping(&*mmtk.plan, &genimmix.gen.nursery)),
        prepare_func: &genimmix_mutator_prepare,
        release_func: &genimmix_mutator_release,
    };

    Mutator {
        allocators: Allocators::<VM>::new(mutator_tls, &*mmtk.plan, &config.space_mapping),
        barrier: Box::new(ObjectBarrier::new(GenObjectBarrierSemantics::new(
            mmtk,
            &genimmix.gen,
        ))),
        mutator_tls,
        config,
        plan: genimmix,
        native_thread_id: 0,
        mutator_id: crate::util::MUTATOR_ID_GENERATOR.fetch_add(1, atomic::Ordering::SeqCst),
        in_request: false,
        request_id: 0,
        global_request_id: 0,
        request_scope_object_size: 0,
        request_scope_object_counter: 0,
        request_scope_public_object_size: 0,
        request_scope_public_object_counter: 0,
        request_scope_live_public_object_size: 0,
        request_scope_live_public_object_counter: 0,
        request_scope_live_private_object_size: 0,
        request_scope_live_private_object_counter: 0,
        request_scope_write_barrier_counter: 0,
        request_scope_write_barrier_slowpath_counter: 0,
    }
}
