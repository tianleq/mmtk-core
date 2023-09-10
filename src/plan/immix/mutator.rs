use std::borrow::BorrowMut;

use super::Immix;
use crate::plan::barriers::{PublicObjectMarkingBarrier, PublicObjectMarkingBarrierSemantics};
use crate::plan::mutator_context::create_allocator_mapping;
use crate::plan::mutator_context::create_space_mapping;
use crate::plan::mutator_context::Mutator;
use crate::plan::mutator_context::MutatorConfig;
use crate::plan::mutator_context::ReservedAllocators;
use crate::plan::AllocationSemantics;
use crate::util::alloc::allocators::{AllocatorSelector, Allocators};
use crate::util::alloc::ImmixAllocator;
use crate::util::opaque_pointer::{VMMutatorThread, VMWorkerThread};
use crate::vm::VMBinding;
use crate::MMTK;
use enum_map::EnumMap;

#[cfg(feature = "thread_local_gc")]
const THREAD_LOCAL_GC_ACTIVE: u32 = 1;

pub fn immix_mutator_prepare<VM: VMBinding>(mutator: &mut Mutator<VM>, _tls: VMWorkerThread) {
    let immix_allocator = unsafe {
        mutator
            .allocators
            .get_allocator_mut(mutator.config.allocator_mapping[AllocationSemantics::Default])
    }
    .downcast_mut::<ImmixAllocator<VM>>()
    .unwrap();
    immix_allocator.reset();
}

pub fn immix_mutator_release<VM: VMBinding>(mutator: &mut Mutator<VM>, _tls: VMWorkerThread) {
    #[cfg(feature = "thread_local_gc")]
    use crate::util::alloc::LargeObjectAllocator;
    let allocators: &mut Allocators<VM> = mutator.allocators.borrow_mut();
    #[cfg(feature = "thread_local_gc")]
    let thread_local_gc_active = mutator.thread_local_gc_status == THREAD_LOCAL_GC_ACTIVE;
    {
        let immix_allocator = unsafe {
            allocators
                .get_allocator_mut(mutator.config.allocator_mapping[AllocationSemantics::Default])
        }
        .downcast_mut::<ImmixAllocator<VM>>()
        .unwrap();
        immix_allocator.reset();
        #[cfg(feature = "thread_local_gc")]
        // For a thread local gc, it needs to sweep blocks
        // A global gc has dedicated work packets of sweeping blocks,
        // so no need to do it here
        immix_allocator.thread_local_release(thread_local_gc_active);
    }
    #[cfg(feature = "thread_local_gc")]
    {
        let los_allocator: &mut LargeObjectAllocator<VM> = unsafe {
            allocators.get_allocator_mut(mutator.config.allocator_mapping[AllocationSemantics::Los])
        }
        .downcast_mut::<LargeObjectAllocator<VM>>()
        .unwrap();
        los_allocator.thread_local_release(thread_local_gc_active);
    }
}

pub(in crate::plan) const RESERVED_ALLOCATORS: ReservedAllocators = ReservedAllocators {
    n_immix: 1,
    ..ReservedAllocators::DEFAULT
};

lazy_static! {
    pub static ref ALLOCATOR_MAPPING: EnumMap<AllocationSemantics, AllocatorSelector> = {
        let mut map = create_allocator_mapping(RESERVED_ALLOCATORS, true);
        map[AllocationSemantics::Default] = AllocatorSelector::Immix(0);
        map
    };
}

pub fn create_immix_mutator<VM: VMBinding>(
    mutator_tls: VMMutatorThread,
    mmtk: &'static MMTK<VM>,
) -> Mutator<VM> {
    let plan = &*mmtk.plan;
    let immix = plan.downcast_ref::<Immix<VM>>().unwrap();
    let config = MutatorConfig {
        allocator_mapping: &ALLOCATOR_MAPPING,
        space_mapping: Box::new({
            let mut vec = create_space_mapping(RESERVED_ALLOCATORS, true, plan);
            vec.push((AllocatorSelector::Immix(0), &immix.immix_space));
            vec
        }),
        prepare_func: &immix_mutator_prepare,
        release_func: &immix_mutator_release,
    };
    let mutator_id = crate::util::MUTATOR_ID_GENERATOR.fetch_add(1, atomic::Ordering::SeqCst);
    Mutator {
        allocators: Allocators::<VM>::new(mutator_tls, mutator_id, plan, &config.space_mapping),
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
