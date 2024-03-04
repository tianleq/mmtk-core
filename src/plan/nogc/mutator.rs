use crate::plan::barriers::NoBarrier;
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
use crate::plan::nogc::NoGC;
use crate::plan::AllocationSemantics;
use crate::plan::Plan;
use crate::util::alloc::allocators::{AllocatorSelector, Allocators};
use crate::util::{VMMutatorThread, VMWorkerThread};
use crate::vm::VMBinding;
use enum_map::{enum_map, EnumMap};

/// We use three bump allocators when enabling nogc_multi_space.
const MULTI_SPACE_RESERVED_ALLOCATORS: ReservedAllocators = ReservedAllocators {
    n_bump_pointer: 3,
    ..ReservedAllocators::DEFAULT
};

lazy_static! {
    /// When nogc_multi_space is disabled, force all the allocation go to the default allocator and space.
    static ref ALLOCATOR_MAPPING_SINGLE_SPACE: EnumMap<AllocationSemantics, AllocatorSelector> = enum_map! {
        _ => AllocatorSelector::BumpPointer(0),
    };
    pub static ref ALLOCATOR_MAPPING: EnumMap<AllocationSemantics, AllocatorSelector> = {
        if cfg!(feature = "nogc_multi_space") {
            let mut map = create_allocator_mapping(MULTI_SPACE_RESERVED_ALLOCATORS, false);
            map[AllocationSemantics::Default] = AllocatorSelector::BumpPointer(0);
            map[AllocationSemantics::Immortal] = AllocatorSelector::BumpPointer(1);
            map[AllocationSemantics::Los] = AllocatorSelector::BumpPointer(2);
            map
        } else {
            *ALLOCATOR_MAPPING_SINGLE_SPACE
        }
    };
}

pub fn nogc_mutator_noop<VM: VMBinding>(_mutator: &mut Mutator<VM>, _tls: VMWorkerThread) {
    unreachable!();
}

pub fn create_nogc_mutator<VM: VMBinding>(
    mutator_tls: VMMutatorThread,
    plan: &'static dyn Plan<VM = VM>,
) -> Mutator<VM> {
    let config = MutatorConfig {
        allocator_mapping: &ALLOCATOR_MAPPING,
        space_mapping: Box::new({
            let mut vec = create_space_mapping(MULTI_SPACE_RESERVED_ALLOCATORS, false, plan);
            vec.push((
                AllocatorSelector::BumpPointer(0),
                &plan.downcast_ref::<NoGC<VM>>().unwrap().nogc_space,
            ));
            vec.push((
                AllocatorSelector::BumpPointer(1),
                &plan.downcast_ref::<NoGC<VM>>().unwrap().immortal,
            ));
            vec.push((
                AllocatorSelector::BumpPointer(2),
                &plan.downcast_ref::<NoGC<VM>>().unwrap().los,
            ));
            vec
        }),
        prepare_func: &nogc_mutator_noop,
        release_func: &nogc_mutator_noop,
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
        allocators: Allocators::<VM>::new(mutator_tls, 0, plan, &config.space_mapping),
        barrier: Box::new(NoBarrier),
        mutator_tls,
        config,
        plan,
        mutator_id: 0,
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
