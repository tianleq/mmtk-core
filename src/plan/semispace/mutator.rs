use super::SemiSpace;
use crate::plan::barriers::ObjectOwnerBarrier;
use crate::plan::mutator_context::Mutator;
use crate::plan::mutator_context::MutatorConfig;
use crate::plan::mutator_context::{
    create_allocator_mapping, create_space_mapping, ReservedAllocators,
};
use crate::plan::AllocationSemantics;
use crate::util::alloc::allocators::{AllocatorSelector, Allocators};
use crate::util::alloc::MarkCompactAllocator;
use crate::util::{VMMutatorThread, VMWorkerThread};
use crate::vm::{ObjectModel, VMBinding};

use enum_map::EnumMap;

pub fn ss_mutator_prepare<VM: VMBinding>(_mutator: &mut Mutator<VM>, _tls: VMWorkerThread) {
    // Do nothing
}

pub fn ss_mutator_release<VM: VMBinding>(mutator: &mut Mutator<VM>, _tls: VMWorkerThread) {
    // rebind the allocation bump pointer to the appropriate semispace
    let bump_allocator = unsafe {
        mutator
            .allocators
            .get_allocator_mut(mutator.config.allocator_mapping[AllocationSemantics::Default])
    }
    .downcast_mut::<MarkCompactAllocator<VM>>()
    .unwrap();
    bump_allocator.rebind(
        mutator
            .plan
            .downcast_ref::<SemiSpace<VM>>()
            .unwrap()
            .tospace(),
    );
}

const RESERVED_ALLOCATORS: ReservedAllocators = ReservedAllocators {
    n_bump_pointer: 1,
    ..ReservedAllocators::DEFAULT
};

lazy_static! {
    pub static ref ALLOCATOR_MAPPING: EnumMap<AllocationSemantics, AllocatorSelector> = {
        let mut map = create_allocator_mapping(RESERVED_ALLOCATORS, true);
        map[AllocationSemantics::Default] = AllocatorSelector::MarkCompact(0);
        map
    };
}

pub fn create_ss_mutator<VM: VMBinding>(
    mutator_tls: VMMutatorThread,
    // plan: &'static dyn Plan<VM = VM>,
    mmtk: &'static crate::MMTK<VM>,
) -> Mutator<VM> {
    let plan = &*mmtk.plan;
    let ss = plan.downcast_ref::<SemiSpace<VM>>().unwrap();
    let config = MutatorConfig {
        allocator_mapping: &*ALLOCATOR_MAPPING,
        space_mapping: Box::new({
            let mut vec = create_space_mapping(RESERVED_ALLOCATORS, true, plan);
            vec.push((AllocatorSelector::MarkCompact(0), ss.tospace()));
            vec
        }),
        prepare_func: &ss_mutator_prepare,
        release_func: &ss_mutator_release,
    };

    Mutator {
        allocators: Allocators::<VM>::new(mutator_tls, plan, &config.space_mapping),
        barrier: Box::new(ObjectOwnerBarrier::<VM>::new(
            mmtk,
            // *VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC,
            *VM::VMObjectModel::LOCAL_MARK_BIT_SPEC,
            0,
        )),
        mutator_tls,
        config,
        plan,
        critical_section_active: false,
        request_id: 0,
        critical_section_total_object_counter: 0,
        critical_section_total_object_bytes: 0,
        critical_section_total_local_object_counter: 0,
        critical_section_total_local_object_bytes: 0,
        critical_section_local_live_object_bytes: 0,
        critical_section_local_live_object_counter: 0,
        critical_section_local_live_private_object_counter: 0,
        critical_section_local_live_private_object_bytes: 0,
        critical_section_write_barrier_counter: 0,
        critical_section_write_barrier_slowpath_counter: 0,
        critical_section_write_barrier_public_counter: 0,
        critical_section_write_barrier_public_bytes: 0,
        access_non_local_object_counter: 0,
    }
}