use std::borrow::BorrowMut;

use super::Immix;
#[cfg(feature = "public_bit")]
use crate::plan::barriers::{PublicObjectMarkingBarrier, PublicObjectMarkingBarrierSemantics};
use crate::plan::mutator_context::create_allocator_mapping;
use crate::plan::mutator_context::create_space_mapping;
use crate::plan::mutator_context::Mutator;
use crate::plan::mutator_context::MutatorBuilder;
use crate::plan::mutator_context::MutatorConfig;
use crate::plan::mutator_context::ReservedAllocators;
use crate::plan::AllocationSemantics;
use crate::util::alloc::allocators::AllocatorSelector;
use crate::util::alloc::allocators::Allocators;
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
        // For a thread local gc, it needs to sweep blocks from its local block list
        // so need to do it here
        immix_allocator.release();
        let los_allocator: &mut LargeObjectAllocator<VM> = unsafe {
            allocators.get_allocator_mut(mutator.config.allocator_mapping[AllocationSemantics::Los])
        }
        .downcast_mut::<LargeObjectAllocator<VM>>()
        .unwrap();
        los_allocator.release();
        // Force a local gc in the next polling
        mutator.local_allocation_size = u32::MAX as usize;
        mutator.thread_local_gc_status = 0;
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

#[cfg(feature = "thread_local_gc_copying")]
pub fn immix_mutator_thread_local_alloc_copy<VM: VMBinding>(
    mutator: &mut Mutator<VM>,
    bytes: usize,
    align: usize,
    offset: usize,
) -> crate::util::Address {
    let allocators: &mut Allocators<VM> = mutator.allocators.borrow_mut();

    let immix_allocator: &mut ImmixAllocator<VM> = unsafe {
        allocators.get_allocator_mut(mutator.config.allocator_mapping[AllocationSemantics::Default])
    }
    .downcast_mut::<ImmixAllocator<VM>>()
    .unwrap();
    immix_allocator.alloc_copy(bytes, align, offset)
}

#[cfg(feature = "thread_local_gc_copying")]
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
    immix_allocator
        .immix_space()
        .thread_local_post_copy(mutator, obj, bytes)
}

#[cfg(feature = "thread_local_gc")]
pub fn immix_mutator_thread_local_defrag_prepare<VM: VMBinding>(mutator: &mut Mutator<VM>) {
    let allocators: &mut Allocators<VM> = mutator.allocators.borrow_mut();

    let immix_allocator = unsafe {
        allocators.get_allocator_mut(mutator.config.allocator_mapping[AllocationSemantics::Default])
    }
    .downcast_mut::<ImmixAllocator<VM>>()
    .unwrap();

    immix_allocator.thread_local_defrag_prepare();
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
    let immix = mmtk.get_plan().downcast_ref::<Immix<VM>>().unwrap();
    let config = MutatorConfig {
        allocator_mapping: &ALLOCATOR_MAPPING,
        space_mapping: Box::new({
            let mut vec = create_space_mapping(RESERVED_ALLOCATORS, true, immix);
            vec.push((AllocatorSelector::Immix(0), &immix.immix_space));
            vec
        }),
        prepare_func: &immix_mutator_prepare,
        release_func: &immix_mutator_release,
        #[cfg(feature = "thread_local_gc")]
        thread_local_prepare_func: &immix_mutator_thread_local_prepare,
        #[cfg(feature = "thread_local_gc")]
        thread_local_release_func: &immix_mutator_thread_local_release,
        #[cfg(feature = "thread_local_gc_copying")]
        thread_local_alloc_copy_func: &immix_mutator_thread_local_alloc_copy,
        #[cfg(feature = "thread_local_gc_copying")]
        thread_local_post_copy_func: &immix_mutator_thread_local_post_copy,
        #[cfg(feature = "thread_local_gc_copying")]
        thread_local_defrag_prepare_func: &immix_mutator_thread_local_defrag_prepare,
    };
    let mutator_id = crate::util::MUTATOR_ID_GENERATOR.fetch_add(1, atomic::Ordering::SeqCst);
    #[cfg(feature = "public_bit")]
    let barrier = Box::new(PublicObjectMarkingBarrier::new(
        PublicObjectMarkingBarrierSemantics::new(
            mmtk,
            #[cfg(feature = "debug_publish_object")]
            mutator_id,
            #[cfg(feature = "debug_thread_local_gc_copying")]
            mutator_tls,
        ),
    ));
    #[cfg(not(feature = "public_bit"))]
    let barrier = Box::new(crate::plan::barriers::NoBarrier);
    let builder = MutatorBuilder::new(mutator_tls, mmtk, config);
    if cfg!(feature = "thread_local_gc") {
        builder.barrier(barrier).mutator_id(mutator_id).build()
    } else {
        builder.build()
    }
}
