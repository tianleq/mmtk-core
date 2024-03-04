use super::SemiSpace;
#[cfg(not(feature = "public_bit"))]
use crate::plan::barriers::NoBarrier;
use crate::plan::barriers::PublicObjectMarkingBarrier;
use crate::plan::barriers::PublicObjectMarkingBarrierSemantics;
#[cfg(feature = "thread_local_gc")]
use crate::plan::mutator_context::generic_thread_local_alloc_copy;
#[cfg(feature = "thread_local_gc")]
use crate::plan::mutator_context::generic_thread_local_post_copy;
#[cfg(feature = "thread_local_gc")]
use crate::plan::mutator_context::generic_thread_local_prepare;
#[cfg(feature = "thread_local_gc")]
use crate::plan::mutator_context::generic_thread_local_release;
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
use crate::MMTK;
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
        #[cfg(feature = "thread_local_gc")]
        thread_local_prepare_func: &generic_thread_local_prepare,
        #[cfg(feature = "thread_local_gc")]
        thread_local_release_func: &generic_thread_local_release,
        #[cfg(feature = "thread_local_gc")]
        thread_local_alloc_copy_func: &generic_thread_local_alloc_copy,
        #[cfg(feature = "thread_local_gc")]
        thread_local_post_copy_func: &generic_thread_local_post_copy,
    };

    let mutator_id = crate::util::MUTATOR_ID_GENERATOR.fetch_add(1, atomic::Ordering::SeqCst);
    #[cfg(feature = "public_bit")]
    let barrier = Box::new(PublicObjectMarkingBarrier::new(
        PublicObjectMarkingBarrierSemantics::new(
            mmtk,
            #[cfg(feature = "public_object_analysis")]
            mutator_tls,
        ),
    ));
    #[cfg(not(feature = "public_bit"))]
    let barrier = Box::new(NoBarrier);

    Mutator {
        allocators: Allocators::<VM>::new(mutator_tls, 0, plan, &config.space_mapping),
        barrier,
        mutator_tls,
        config,
        plan,
        mutator_id,
        #[cfg(feature = "thread_local_gc")]
        thread_local_gc_status: 0,
        #[cfg(feature = "thread_local_gc")]
        finalizable_candidates: Box::new(Vec::new()),
        #[cfg(feature = "public_object_analysis")]
        allocation_count: 0,
        #[cfg(feature = "public_object_analysis")]
        bytes_allocated: 0,
        #[cfg(all(
            feature = "thread_local_gc",
            any(feature = "public_object_analysis", feature = "debug_publish_object")
        ))]
        request_id: 0,
        #[cfg(all(
            feature = "thread_local_gc",
            any(feature = "public_object_analysis", feature = "debug_publish_object")
        ))]
        request_active: false,
    }
}
