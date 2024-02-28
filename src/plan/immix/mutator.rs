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

pub fn immix_mutator_prepare<VM: VMBinding>(mutator: &mut Mutator<VM>, _tls: VMWorkerThread) {
    #[cfg(feature = "thread_local_gc")]
    use crate::util::alloc::LargeObjectAllocator;
    let allocators: &mut Allocators<VM> = mutator.allocators.borrow_mut();

    let immix_allocator = unsafe {
        allocators.get_allocator_mut(mutator.config.allocator_mapping[AllocationSemantics::Default])
    }
    .downcast_mut::<ImmixAllocator<VM>>()
    .unwrap();
    immix_allocator.reset();

    #[cfg(feature = "thread_local_gc")]
    {
        immix_allocator.prepare();
        let los_allocator: &mut LargeObjectAllocator<VM> = unsafe {
            allocators.get_allocator_mut(mutator.config.allocator_mapping[AllocationSemantics::Los])
        }
        .downcast_mut::<LargeObjectAllocator<VM>>()
        .unwrap();
        los_allocator.prepare();
    }
}

pub fn immix_mutator_release<VM: VMBinding>(mutator: &mut Mutator<VM>, _tls: VMWorkerThread) {
    #[cfg(feature = "thread_local_gc")]
    use crate::util::alloc::LargeObjectAllocator;
    let allocators: &mut Allocators<VM> = mutator.allocators.borrow_mut();

    let immix_allocator = unsafe {
        allocators.get_allocator_mut(mutator.config.allocator_mapping[AllocationSemantics::Default])
    }
    .downcast_mut::<ImmixAllocator<VM>>()
    .unwrap();
    immix_allocator.reset();

    #[cfg(feature = "thread_local_gc")]
    {
        // For a thread local gc, it needs to sweep blocks
        // A global gc has dedicated work packets(SweepChunk) of sweeping blocks,
        // so no need to do it here
        immix_allocator.release();
        let los_allocator: &mut LargeObjectAllocator<VM> = unsafe {
            allocators.get_allocator_mut(mutator.config.allocator_mapping[AllocationSemantics::Los])
        }
        .downcast_mut::<LargeObjectAllocator<VM>>()
        .unwrap();
        los_allocator.release();
    }
}

#[cfg(feature = "thread_local_gc")]
pub fn immix_mutator_thread_local_prepare<VM: VMBinding>(mutator: &mut Mutator<VM>) {
    use crate::util::alloc::LargeObjectAllocator;
    let allocators: &mut Allocators<VM> = mutator.allocators.borrow_mut();

    let immix_allocator = unsafe {
        allocators.get_allocator_mut(mutator.config.allocator_mapping[AllocationSemantics::Default])
    }
    .downcast_mut::<ImmixAllocator<VM>>()
    .unwrap();
    immix_allocator.reset();
    immix_allocator.thread_local_prepare();
    let los_allocator: &mut LargeObjectAllocator<VM> = unsafe {
        allocators.get_allocator_mut(mutator.config.allocator_mapping[AllocationSemantics::Los])
    }
    .downcast_mut::<LargeObjectAllocator<VM>>()
    .unwrap();
    los_allocator.thread_local_prepare();
}

#[cfg(feature = "thread_local_gc")]
pub fn immix_mutator_thread_local_release<VM: VMBinding>(mutator: &mut Mutator<VM>) {
    use crate::util::alloc::LargeObjectAllocator;
    let allocators: &mut Allocators<VM> = mutator.allocators.borrow_mut();

    let immix_allocator = unsafe {
        allocators.get_allocator_mut(mutator.config.allocator_mapping[AllocationSemantics::Default])
    }
    .downcast_mut::<ImmixAllocator<VM>>()
    .unwrap();
    immix_allocator.reset();
    immix_allocator.thread_local_release();
    let los_allocator: &mut LargeObjectAllocator<VM> = unsafe {
        allocators.get_allocator_mut(mutator.config.allocator_mapping[AllocationSemantics::Los])
    }
    .downcast_mut::<LargeObjectAllocator<VM>>()
    .unwrap();
    los_allocator.thread_local_release();
}

#[cfg(feature = "thread_local_gc")]
pub fn immix_mutator_thread_local_alloc_copy<VM: VMBinding>(
    mutator: &mut Mutator<VM>,
    bytes: usize,
    align: usize,
    offset: usize,
) -> crate::util::Address {
    // {
    //     if crate::util::public_bit::is_public::<VM>(_original) {
    //         let result = self
    //             .public_object_allocator
    //             .alloc_as_collector(bytes, align, offset);
    //         result
    //     } else {
    //         // This branch will only be taken in a local gc and private objects
    //         // can only live in a block of the same owner

    //         self.allocator.local_line_mark_state = self.local_line_mark_state.unwrap();
    //         self.allocator.local_unavailable_line_mark_state =
    //             self.local_unavailable_line_mark_state.unwrap();

    //         #[cfg(debug_assertions)]
    //         {
    //             let block = Block::containing::<VM>(_original);
    //             let mutator_id = block.owner();
    //             debug_assert!(
    //                 mutator_id == self.allocator.get_mutator(),
    //                 "mutator_id: {}, allocator.mutator_id: {}",
    //                 mutator_id,
    //                 self.allocator.get_mutator()
    //             );
    //         }
    //         self.allocator
    //             .alloc_as_mutator(self.allocator.get_mutator(), bytes, align, offset)
    //     }
    // }

    use crate::util::alloc::Allocator;
    let allocators: &mut Allocators<VM> = mutator.allocators.borrow_mut();

    let immix_allocator: &mut ImmixAllocator<VM> = unsafe {
        allocators.get_allocator_mut(mutator.config.allocator_mapping[AllocationSemantics::Default])
    }
    .downcast_mut::<ImmixAllocator<VM>>()
    .unwrap();
    immix_allocator.alloc(bytes, align, offset)
}

#[cfg(feature = "thread_local_gc")]
fn immix_mutator_thread_local_post_copy<VM: VMBinding>(
    mutator: &mut Mutator<VM>,
    obj: crate::util::ObjectReference,
    bytes: usize,
) {
    let allocators: &mut Allocators<VM> = mutator.allocators.borrow_mut();

    let immix_allocator: &mut ImmixAllocator<VM> = unsafe {
        allocators.get_allocator_mut(mutator.config.allocator_mapping[AllocationSemantics::Default])
    }
    .downcast_mut::<ImmixAllocator<VM>>()
    .unwrap();
    immix_allocator.immix_space().post_copy(obj, bytes)
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
        #[cfg(feature = "thread_local_gc")]
        thread_local_prepare_func: &immix_mutator_thread_local_prepare,
        #[cfg(feature = "thread_local_gc")]
        thread_local_release_func: &immix_mutator_thread_local_release,
        #[cfg(feature = "thread_local_gc")]
        thread_local_alloc_copy_func: &immix_mutator_thread_local_alloc_copy,
        #[cfg(feature = "thread_local_gc")]
        thread_local_post_copy_func: &immix_mutator_thread_local_post_copy,
    };
    let mutator_id = crate::util::MUTATOR_ID_GENERATOR.fetch_add(1, atomic::Ordering::SeqCst);
    #[cfg(feature = "public_bit")]
    let barrier = Box::new(PublicObjectMarkingBarrier::new(
        PublicObjectMarkingBarrierSemantics::new(mmtk),
    ));
    #[cfg(not(feature = "public_bit"))]
    let barrier = Box::new(NoBarrier);
    Mutator {
        allocators: Allocators::<VM>::new(mutator_tls, mutator_id, plan, &config.space_mapping),
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
        #[cfg(all(feature = "thread_local_gc", feature = "debug_publish_object"))]
        request_id: 0,
        #[cfg(feature = "public_object_analysis")]
        global_request_id: 0,
    }
}
