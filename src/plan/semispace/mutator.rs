use super::SemiSpace;
use crate::MMTK;
// use crate::plan::barriers::NoBarrier;
use crate::plan::barriers::PublicObjectMarkingBarrier;
use crate::plan::barriers::PublicObjectMarkingBarrierSemantics;
use crate::plan::mutator_context::Mutator;
use crate::plan::mutator_context::MutatorConfig;
use crate::plan::mutator_context::{
    create_allocator_mapping, create_space_mapping, ReservedAllocators,
};
use crate::plan::AllocationSemantics;
use crate::util::alloc::allocators::{AllocatorSelector, Allocators};
use crate::util::alloc::BumpAllocator;
use crate::util::{VMMutatorThread, VMWorkerThread};
use crate::vm::VMBinding;
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
    // .downcast_mut::<BumpAllocator<VM>>()
    .downcast_mut::<BumpAllocator<VM>>()
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
        map[AllocationSemantics::Default] = AllocatorSelector::BumpPointer(0);
        map
    };
}

pub fn create_ss_mutator<VM: VMBinding>(
    mutator_tls: VMMutatorThread,
    mmtk: &'static MMTK<VM>,
) -> Mutator<VM> {
    let plan = &*mmtk.plan;
    let ss = plan.downcast_ref::<SemiSpace<VM>>().unwrap();
    let config = MutatorConfig {
        allocator_mapping: &ALLOCATOR_MAPPING,
        space_mapping: Box::new({
            let mut vec = create_space_mapping(RESERVED_ALLOCATORS, true, plan);
            vec.push((AllocatorSelector::BumpPointer(0), ss.tospace()));
            vec
        }),
        prepare_func: &ss_mutator_prepare,
        release_func: &ss_mutator_release,
    };
    let mutator_id = crate::util::MUTATOR_ID_GENERATOR.fetch_add(1, atomic::Ordering::SeqCst);
    Mutator {
        allocators: Allocators::<VM>::new(mutator_tls, 0, plan, &config.space_mapping),
        // barrier: Box::new(NoBarrier),
        barrier: Box::new(PublicObjectMarkingBarrier::new(
            PublicObjectMarkingBarrierSemantics::new(mmtk),
        )),
        mutator_tls,
        config,
        plan,
        thread_local_gc_status: 0,
        mutator_id,
    }
}
