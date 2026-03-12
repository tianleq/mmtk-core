use super::gc_work::ImmixGCWorkContext;
use super::mutator::ALLOCATOR_MAPPING;
use crate::plan::barriers::BarrierSelector;
use crate::plan::global::BasePlan;
use crate::plan::global::CommonPlan;
use crate::plan::global::CreateGeneralPlanArgs;
use crate::plan::global::CreateSpecificPlanArgs;
use crate::plan::immix::Pause;
use crate::plan::AllocationSemantics;
use crate::plan::Plan;
use crate::plan::PlanConstraints;
#[cfg(feature = "thread_local_gc")]
use crate::plan::PlanThreadlocalTraceObject;
#[cfg(feature = "thread_local_gc")]
use crate::plan::ThreadlocalTracedObjectType;
#[cfg(feature = "thread_local_gc")]
use crate::policy::gc_work::PolicyThreadlocalTraceObject;
#[cfg(feature = "thread_local_gc")]
use crate::policy::gc_work::TraceKind;
use crate::policy::gc_work::TRACE_KIND_PUBLIC;
#[cfg(feature = "thread_local_gc")]
use crate::policy::immix::block::Block;
use crate::policy::immix::ImmixSpaceArgs;
#[cfg(not(feature = "thread_local_gc"))]
use crate::policy::immix::{TRACE_KIND_DEFRAG, TRACE_KIND_FAST};
use crate::policy::space::Space;
use crate::scheduler::gc_work::Prepare;
use crate::scheduler::gc_work::Release;
use crate::scheduler::gc_work::ScanStackSemantic;
use crate::scheduler::gc_work::StopMutators;
#[cfg(feature = "thread_local_gc")]
use crate::scheduler::thread_local_gc_work::ThreadlocalPrepare;
#[cfg(feature = "thread_local_gc")]
use crate::scheduler::thread_local_gc_work::ACTIVE_LOCAL_GC_COUNTER;
use crate::scheduler::*;
use crate::util::alloc::allocators::AllocatorSelector;
use crate::util::copy::*;
use crate::util::heap::gc_trigger::SpaceStats;
use crate::util::heap::VMRequest;
use crate::util::metadata::side_metadata::SideMetadataContext;
#[cfg(any(feature = "debug_publish_object", feature = "thread_local_gc"))]
use crate::util::ObjectReference;
#[cfg(feature = "thread_local_gc")]
use crate::util::VMMutatorThread;
use crate::vm::VMBinding;
#[cfg(feature = "thread_local_gc")]
use crate::Mutator;
use crate::{policy::immix::ImmixSpace, util::opaque_pointer::VMWorkerThread};
use std::sync::atomic::AtomicBool;

use atomic::Atomic;
use atomic::Ordering;
use enum_map::EnumMap;

use mmtk_macros::{HasSpaces, PlanTraceObject};
use portable_atomic::AtomicUsize;

#[derive(HasSpaces, PlanTraceObject)]
pub struct Immix<VM: VMBinding> {
    #[post_scan]
    #[space]
    #[copy_semantics(CopySemantics::DefaultCopy)]
    pub immix_space: ImmixSpace<VM>,
    #[parent]
    pub common: CommonPlan<VM>,
    last_gc_was_defrag: AtomicBool,
    defrag_mutator: AtomicUsize,
    current_pause: Atomic<Option<Pause>>,
    full_heap_gc_pending: AtomicBool,
}

/// The plan constraints for the immix plan.
pub const IMMIX_CONSTRAINTS: PlanConstraints = PlanConstraints {
    // If we disable moving in Immix, this is a non-moving plan.
    moves_objects: !cfg!(feature = "immix_non_moving"),
    // Max immix object size is half of a block.
    max_non_los_default_alloc_bytes: crate::policy::immix::MAX_IMMIX_OBJECT_SIZE,
    barrier: BarrierSelector::PublicObjectMarkingBarrier,
    needs_prepare_mutator: true,
    ..PlanConstraints::default()
};

impl<VM: VMBinding> Plan for Immix<VM> {
    fn collection_required(&self, space_full: bool, _space: Option<SpaceStats<Self::VM>>) -> bool {
        self.base().collection_required(self, space_full)
    }

    #[cfg(feature = "thread_local_gc")]
    fn thread_local_collection_required(
        &self,
        _space_full: bool,
        _space: Option<SpaceStats<VM>>,
        _tls: VMMutatorThread,
    ) -> bool {
        // {
        //     let total_pages = self.get_total_pages();

        //     let thread_local_copy_reserve_pages = self.get_thread_local_collection_reserved_pages();
        //     let red_zone_pages = thread_local_copy_reserve_pages;

        //     let required = self.get_used_pages() + thread_local_copy_reserve_pages + red_zone_pages
        //         >= total_pages;

        //     required
        // }

        use crate::{scheduler::thread_local_gc_work::THREAD_LOCAL_GC_PENDING, vm::ActivePlan};

        let mutator = VM::VMActivePlan::mutator(_tls);

        mutator.thread_local_gc_status == THREAD_LOCAL_GC_PENDING
            || mutator.local_allocation_size >= self.options().get_thread_local_heap_size()
    }

    fn last_collection_was_exhaustive(&self) -> bool {
        self.immix_space
            .is_last_gc_exhaustive(self.last_gc_was_defrag.load(Ordering::Relaxed))
    }

    fn constraints(&self) -> &'static PlanConstraints {
        &IMMIX_CONSTRAINTS
    }

    fn create_copy_config(&'static self) -> CopyConfig<Self::VM> {
        use enum_map::enum_map;
        CopyConfig {
            copy_mapping: enum_map! {
                CopySemantics::DefaultCopy => CopySelector::Immix(0),
                _ => CopySelector::Unused,
            },
            space_mapping: vec![(CopySelector::Immix(0), &self.immix_space)],
            constraints: &IMMIX_CONSTRAINTS,
        }
    }

    fn schedule_collection(&'static self, scheduler: &GCWorkScheduler<VM>) {
        // if self.full_heap_gc_pending.load(Ordering::Acquire) {
        //     self.current_pause
        //         .store(Some(Pause::Full), Ordering::SeqCst);
        //     Self::schedule_immix_full_heap_collection::<
        //         Immix<VM>,
        //         ImmixGCWorkContext<VM, TRACE_KIND_FAST>,
        //         ImmixGCWorkContext<VM, TRACE_KIND_DEFRAG>,
        //     >(self, &self.immix_space, scheduler);
        // } else {
        //     self.current_pause
        //         .store(Some(Pause::Public), Ordering::SeqCst);
        //     Self::schedule_immix_public_collection::<
        //         Immix<VM>,
        //         ImmixGCWorkContext<VM, TRACE_KIND_PUBLIC>,
        //     >(self, &self.immix_space, scheduler);
        // }

        self.current_pause
            .store(Some(Pause::Public), Ordering::SeqCst);
        Self::schedule_immix_public_collection::<
            Immix<VM>,
            ImmixGCWorkContext<VM, TRACE_KIND_PUBLIC>,
        >(self, &self.immix_space, scheduler);
    }

    #[cfg(feature = "thread_local_gc")]
    fn do_thread_local_collection(
        &'static self,
        tls: VMMutatorThread,
        mmtk: &'static crate::MMTK<VM>,
    ) {
        use crate::policy::immix::{TRACE_KIND_THREAD_LOCAL_COPY, TRACE_KIND_THREAD_LOCAL_FAST};

        if cfg!(feature = "thread_local_gc_copying") {
            Self::do_immix_thread_local_collection_impl::<Immix<VM>, TRACE_KIND_THREAD_LOCAL_COPY>(
                tls, self, mmtk,
            )
        } else {
            Self::do_immix_thread_local_collection_impl::<Immix<VM>, TRACE_KIND_THREAD_LOCAL_FAST>(
                tls, self, mmtk,
            )
        }
    }

    #[cfg(feature = "thread_local_gc")]
    fn do_thread_local_defrag(
        &'static self,
        _tls: VMMutatorThread,
        _mmtk: &'static crate::MMTK<VM>,
        _worker: &mut GCWorker<VM>,
    ) {
        #[cfg(feature = "thread_local_gc_copying")]
        {
            use thread_local_gc_work::{
                PlanThreadlocalObjectGraphTraversalClosure, ScanMutator, ThreadlocalDefragPrepare,
            };

            ThreadlocalDefragPrepare::<VM>::new(_tls).execute();

            //Scan mutator
            ScanMutator::<
                VM,
                PlanThreadlocalObjectGraphTraversalClosure<
                    VM,
                    Immix<VM>,
                    { crate::policy::immix::TRACE_KIND_THREAD_LOCAL_DEFRAG },
                >,
                {
                    use crate::policy::gc_work::DEFAULT_TRACE;
                    DEFAULT_TRACE
                },
            >::new(_tls, _mmtk, Some(_worker))
            .execute();
            // cannot do release since finalizer has not been executed yet
            // objects may be resurrected
        }
    }

    #[cfg(feature = "thread_local_gc_copying")]
    fn defrag_mutator_required(
        &self,
        mmtk: &'static crate::MMTK<VM>,
        tls: VMMutatorThread,
    ) -> bool {
        use crate::vm::ActivePlan;
        use std::borrow::Borrow;
        let number_of_workers = mmtk.scheduler.worker_group.worker_count();
        let max_defrag_mutators = if *mmtk.options.max_concurrent_defrag_mutator != 0 {
            usize::try_from(*mmtk.options.max_concurrent_defrag_mutator).unwrap()
        } else {
            number_of_workers
        };
        if self.defrag_mutator.load(Ordering::Acquire) >= number_of_workers {
            return false;
        }
        let mutator = VM::VMActivePlan::mutator(tls);
        let allocators: &crate::util::alloc::allocators::Allocators<VM> =
            mutator.allocators.borrow();

        let immix_allocator = unsafe {
            allocators.get_allocator(mutator.config.allocator_mapping[AllocationSemantics::Default])
        }
        .downcast_ref::<crate::util::alloc::ImmixAllocator<VM>>()
        .unwrap();
        if immix_allocator.local_reusable_blocks.len()
            >= thread_local_gc_work::DEFRAG_MUTATOR_THRESHOLD
        {
            let count = self.defrag_mutator.fetch_add(1, Ordering::SeqCst);
            if count < max_defrag_mutators {
                true
            } else {
                self.defrag_mutator.fetch_sub(1, Ordering::SeqCst);
                false
            }

            // true
        } else {
            false
        }
    }

    #[cfg(feature = "thread_local_gc")]
    fn do_thread_local_marking(
        &'static self,
        tls: VMMutatorThread,
        _mmtk: &'static crate::MMTK<VM>,
    ) {
        use crate::{policy::immix::line::Line, vm::ActivePlan};
        use std::borrow::Borrow;

        let mutator = VM::VMActivePlan::mutator(tls);
        let allocators: &crate::util::alloc::allocators::Allocators<VM> =
            mutator.allocators.borrow();
        let immix_allocator = unsafe {
            allocators.get_allocator(mutator.config.allocator_mapping[AllocationSemantics::Default])
        }
        .downcast_ref::<crate::util::alloc::ImmixAllocator<VM>>()
        .unwrap();

        let current_state = self.immix_space.line_mark_state.load(Ordering::Acquire);
        let new_state = if current_state + 1 > Line::MAX_MARK_STATE {
            Line::RESET_MARK_STATE
        } else {
            current_state + 1
        };
        // debug_assert_eq!(current_state, mutator.state);
        immix_allocator.mark_local_heap(current_state, new_state);

        // local state needs to be updated so that future eager marking will
        // not use a stale state
        mutator.state = new_state;
    }

    fn get_allocator_mapping(&self) -> &'static EnumMap<AllocationSemantics, AllocatorSelector> {
        &ALLOCATOR_MAPPING
    }

    fn prepare(&mut self, tls: VMWorkerThread) {
        let pause = self.current_pause().unwrap();
        match pause {
            Pause::InitialMark => {
                unimplemented!()
            }
            Pause::FinalMark => {
                unimplemented!()
            }
            Pause::Full | Pause::Public => {
                self.common.prepare(tls, true, pause);
                self.immix_space.prepare(
                    true,
                    Some(crate::policy::immix::defrag::StatsForDefrag::new(self)),
                );
                self.defrag_mutator.store(0, Ordering::Release);
            }
        }
        // #[cfg(debug_assertions)]
        // {
        //     use crate::policy::{PRIVATE_OBJECTS_IN_CURRENT_GC, PRIVATE_OBJECTS_IN_PREV_GC};

        //     PRIVATE_OBJECTS_IN_PREV_GC
        //         .lock()
        //         .unwrap()
        //         .extend(PRIVATE_OBJECTS_IN_CURRENT_GC.lock().unwrap().drain());
        // }
    }

    fn release(&mut self, tls: VMWorkerThread) {
        let pause = self.current_pause().unwrap();
        match pause {
            Pause::InitialMark => {
                unimplemented!()
            }
            Pause::FinalMark => {
                unimplemented!()
            }
            Pause::Full | Pause::Public => {
                self.common.release(tls, true, pause);
                // release the collected region
                self.immix_space.release(true, pause);
            }
        }
    }

    fn end_of_gc(&mut self, tls: VMWorkerThread) {
        self.last_gc_was_defrag
            .store(self.immix_space.end_of_gc(), Ordering::Relaxed);
        #[cfg(feature = "thread_local_gc")]
        ACTIVE_LOCAL_GC_COUNTER.store(0, Ordering::Relaxed);
        self.common.end_of_gc(tls);
        let full_pending = self.get_reserved_pages() >= (self.get_total_pages() * 90 / 100);
        self.full_heap_gc_pending
            .store(full_pending, Ordering::Release);
        #[cfg(debug_assertions)]
        {
            // use crate::policy::{
            //     GLOBAL_OBJECTS, GLOBAL_OBJECTS_CONSERVATIVE, GLOBAL_ROOTS_COUNTER,
            //     GLOBAL_ROOTS_COUNTER_CONSERVATIVE, PAGES_FREED_IN_LOCAL_GC, PRIVATE_OBJECTS,
            // };

            let mut conservative = crate::policy::GLOBAL_OBJECTS_CONSERVATIVE.lock().unwrap();
            // let mut precise = GLOBAL_OBJECTS.lock().unwrap();
            // let mut private = PRIVATE_OBJECTS.lock().unwrap();
            // let mut remset = REMSET_OBJECTS.lock().unwrap();
            // let stack_slots = STACK_ROOTS.lock().unwrap();
            // let mut stack_slots_sanity = STACK_ROOTS_SANITY.lock().unwrap();

            // println!(
            //     "{} vs {} at the end of a global GC",
            //     self.common
            //         .base
            //         .global_state
            //         .global_objects_count
            //         .load(Ordering::Acquire),
            //     self.common
            //         .base
            //         .global_state
            //         .global_objects_precise_count
            //         .load(Ordering::Acquire)
            // );

            // println!(
            //     "{} vs {} at the end of global GC, {} pages freed in local GC, {} vs {}",
            //     conservative.len(),
            //     precise.len(),
            //     PAGES_FREED_IN_LOCAL_GC.load(Ordering::Acquire),
            //     GLOBAL_ROOTS_COUNTER_CONSERVATIVE.load(Ordering::Acquire),
            //     GLOBAL_ROOTS_COUNTER.load(Ordering::Acquire)
            // );
            // for s in stack_slots.difference(&stack_slots_sanity) {
            //     println!("local GC stack slot: {:?}", s);
            // }
            // for s in stack_slots_sanity.difference(&stack_slots) {
            //     println!("sanity GC stack slots: {:?}", s);
            // }

            // for o in remset.difference(&private) {
            //     println!("private object: {:?} is kept alive", o);
            // }
            // if conservative.len() - precise.len() > 10000 {
            //     use std::fs::OpenOptions;

            //     let mut file = OpenOptions::new()
            //         .write(true)
            //         .create(true)
            //         .truncate(true)
            //         .open("/home/tianleq/objects-graph.txt")
            //         .unwrap();
            //     for v in conservative.iter() {
            //         use std::io::Write;

            //         file.write_all(format!("{} --> {}\n", v.1, v.0).as_bytes())
            //             .unwrap();
            //     }
            //     {
            //         let mut file = OpenOptions::new()
            //             .write(true)
            //             .create(true)
            //             .truncate(true)
            //             .open("/home/tianleq/extra-objects.txt")
            //             .unwrap();
            //         let mut extra = OpenOptions::new()
            //             .write(true)
            //             .create(true)
            //             .truncate(true)
            //             .open("/home/tianleq/unknown.txt")
            //             .unwrap();
            //         for v in conservative.iter() {
            //             use std::io::Write;

            //             use crate::util::metadata::public_bit::is_public;
            //             if precise.contains_key(v.0) {
            //                 continue;
            //             }
            //             file.write_all(format!("{} --> {}\n", v.1, v.0).as_bytes())
            //                 .unwrap();
            //             if !is_public(*v.1) {
            //                 extra
            //                     .write_all(format!("{} --> {}\n", v.1, v.0).as_bytes())
            //                     .unwrap();
            //             }
            //         }
            //     }
            //     panic!("retention rate too high");
            // }
            conservative.clear();
            // precise.clear();
            // private.clear();
            // remset.clear();
            // GLOBAL_ROOTS_COUNTER.store(0, Ordering::Release);
            // GLOBAL_ROOTS_COUNTER_CONSERVATIVE.store(0, Ordering::Release);
            // stack_slots_sanity.clear();
            self.common()
                .base
                .global_state
                .objects
                .lock()
                .unwrap()
                .clear();
            self.common
                .base
                .global_state
                .global_objects_count
                .store(0, Ordering::Release);
            self.common
                .base
                .global_state
                .global_objects_precise_count
                .store(0, Ordering::Release);

            // {
            //     use std::collections::HashSet;

            //     let mut objects = self.immix_space.common().objects.lock().unwrap();
            //     *objects = HashSet::new();
            // }
        }

        {
            use crate::vm::ActivePlan;

            // At the end of each GC, accumulate allocation bytes in the global varaible.
            for mutator in VM::VMActivePlan::mutators() {
                let allocation_bytes = mutator.allocation_bytes;
                mutator.allocation_bytes = 0;
                self.common
                    .base
                    .global_state
                    .total_allocation_bytes
                    .fetch_add(allocation_bytes, Ordering::SeqCst);
            }
        }
        // #[cfg(debug_assertions)]
        // {
        //     use crate::policy::{
        //         immix::{DEBUG_PUBLIC_OBJECT_FORWARDING, DEBUG_PUBLIC_OBJECT_LEFT_IN_PLACE},
        //         PRIVATE_OBJECTS_IN_PREV_GC,
        //     };

        //     DEBUG_PUBLIC_OBJECT_FORWARDING.lock().unwrap().clear();
        //     DEBUG_PUBLIC_OBJECT_LEFT_IN_PLACE.lock().unwrap().clear();
        //     PRIVATE_OBJECTS_IN_PREV_GC.lock().unwrap().clear();
        // }
    }

    fn current_gc_may_move_object(&self) -> bool {
        self.immix_space.in_defrag()
    }

    fn get_collection_reserved_pages(&self) -> usize {
        let defrag_headroom = self.immix_space.defrag_headroom_pages();
        let mut reserved = defrag_headroom;
        #[cfg(feature = "thread_local_gc_copying")]
        {
            let options = self.options();
            reserved += std::cmp::max(
                options.get_thread_local_heap_size() / crate::util::constants::BYTES_IN_PAGE,
                options.get_max_concurrent_local_gc() as usize
                    * options.get_max_local_copy_reserve() as usize
                    * Block::PAGES,
            );
        }
        reserved
    }

    fn get_used_pages(&self) -> usize {
        self.immix_space.reserved_pages() + self.common.get_used_pages()
    }

    fn base(&self) -> &BasePlan<VM> {
        &self.common.base
    }

    fn base_mut(&mut self) -> &mut BasePlan<Self::VM> {
        &mut self.common.base
    }

    fn common(&self) -> &CommonPlan<VM> {
        &self.common
    }

    #[cfg(feature = "thread_local_gc")]
    fn publish_object(
        &self,
        object: ObjectReference,
        #[cfg(feature = "debug_thread_local_gc_copying")] _tls: crate::util::VMMutatorThread,
    ) {
        if self.immix_space.in_space(object) {
            self.immix_space.publish_object(
                object,
                #[cfg(feature = "debug_thread_local_gc_copying")]
                _tls,
            );
        } else {
            self.common().publish_object(
                object,
                #[cfg(feature = "debug_thread_local_gc_copying")]
                _tls,
            );
        }
    }

    #[cfg(feature = "thread_local_gc")]
    fn publish_runtime_object(&self, object: ObjectReference) {
        if self.immix_space.in_space(object) {
            self.immix_space.publish_runtime_object(object);
        } else {
            self.common().publish_runtime_object(
                object,
                #[cfg(feature = "debug_thread_local_gc_copying")]
                crate::util::VMThread::VMThread::UNINITIALIZED,
            );
        }
    }

    #[cfg(feature = "thread_local_gc")]
    fn get_number_of_reusable_blocks(&self) -> usize {
        self.immix_space.reusable_blocks.len()
    }

    #[cfg(all(feature = "thread_local_gc", debug_assertions))]
    fn get_object_owner(&self, object: ObjectReference) -> u32 {
        if self.immix_space.in_space(object) {
            self.immix_space.get_object_owner(object)
        } else {
            self.common.get_los().get_object_owner(object)
        }
    }

    #[cfg(feature = "debug_publish_object")]
    fn is_object_published(&self, object: ObjectReference) -> bool {
        if self.immix_space.in_space(object) {
            self.immix_space.is_object_published(object)
        } else {
            // the object is not in immix space, it will not be moved
            // so simply check if the object has been published or not
            crate::util::metadata::public_bit::is_public::<VM>(object)
        }
    }

    #[cfg(feature = "debug_thread_local_gc_copying")]
    fn collect_gc_stats(&self) {
        use crate::util::GLOBAL_GC_STATISTICS;
        use crate::vm::ActivePlan;

        let mut number_of_live_public_blocks = 0;
        let mut number_of_live_blocks = 0;
        let mut number_of_local_reusable_blocks = 0;

        for chunk in self.immix_space.chunk_map.all_chunks().filter(|c| {
            self.immix_space.chunk_map.get(*c)
                == crate::util::heap::chunk_map::ChunkState::Allocated
        }) {
            for block in chunk.iter_region::<crate::policy::immix::block::Block>() {
                let state = block.get_state();
                // Skip unallocated blocks.
                if state == crate::policy::immix::block::BlockState::Unallocated {
                    continue;
                }
                let is_public = block.is_block_published();
                if is_public {
                    number_of_live_public_blocks += 1;
                }
                number_of_live_blocks += 1;
            }
        }
        // mutators have been stopped, so it is safe/sound to iterate through all mutators
        for mutator in VM::VMActivePlan::mutators() {
            let allocator = unsafe {
                mutator
                    .allocators
                    .get_allocator(mutator.config.allocator_mapping[AllocationSemantics::Default])
                    .downcast_ref::<crate::util::alloc::ImmixAllocator<VM>>()
                    .unwrap()
            };
            number_of_local_reusable_blocks += allocator.local_reusable_blocks_size();
        }
        let mut guard = GLOBAL_GC_STATISTICS.lock().unwrap();
        guard.number_of_global_reusable_blocks = self.immix_space.reusable_blocks.len();
        guard.number_of_live_blocks = number_of_live_blocks;
        guard.number_of_live_public_blocks = number_of_live_public_blocks;
        guard.number_of_local_reusable_blocks = number_of_local_reusable_blocks;
    }

    #[cfg(feature = "debug_thread_local_gc_copying")]
    fn collect_local_gc_stats(&self, _tls: VMMutatorThread) {
        use crate::vm::ActivePlan;

        let mutator = VM::VMActivePlan::mutator(_tls);
        let immix_allocator = unsafe {
            mutator
                .allocators
                .get_allocator(mutator.config.allocator_mapping[AllocationSemantics::Default])
                .downcast_ref::<crate::util::alloc::ImmixAllocator<VM>>()
                .unwrap()
        };
        immix_allocator.collect_thread_local_heap_stats();
    }
}

impl<VM: VMBinding> Immix<VM> {
    pub fn new(args: CreateGeneralPlanArgs<VM>) -> Self {
        #[cfg(feature = "thread_local_gc_copying")]
        let max_local_copy_reserve = args.options.get_max_local_copy_reserve();
        let plan_args = CreateSpecificPlanArgs {
            global_args: args,
            constraints: &IMMIX_CONSTRAINTS,
            global_side_metadata_specs: SideMetadataContext::new_global_specs(&[]),
        };

        Self::new_with_args(
            plan_args,
            ImmixSpaceArgs {
                unlog_object_when_traced: false,
                #[cfg(feature = "vo_bit")]
                mixed_age: false,
                #[cfg(feature = "thread_local_gc_copying")]
                max_local_copy_reserve,
                never_move_objects: false,
            },
        )
    }

    pub fn new_with_args(
        mut plan_args: CreateSpecificPlanArgs<VM>,
        space_args: ImmixSpaceArgs,
    ) -> Self {
        let immix = Immix {
            immix_space: ImmixSpace::new(
                plan_args.get_space_args("immix", true, false, VMRequest::discontiguous()),
                space_args,
            ),
            common: CommonPlan::new(plan_args),
            last_gc_was_defrag: AtomicBool::new(false),
            defrag_mutator: AtomicUsize::new(0),
            current_pause: Atomic::new(None),
            full_heap_gc_pending: AtomicBool::new(false),
        };

        immix.verify_side_metadata_sanity();

        immix
    }

    fn current_pause(&self) -> Option<Pause> {
        self.current_pause.load(Ordering::SeqCst)
    }

    /// Schedule a full heap immix collection. This method is used by immix/genimmix/stickyimmix
    /// to schedule a full heap collection. A plan must call set_collection_kind and set_gc_status before this method.
    pub(crate) fn schedule_immix_public_collection<
        PlanType: Plan<VM = VM>,
        Context: GCWorkContext<VM = VM, PlanType = PlanType>,
    >(
        plan: &'static PlanType,
        _immix_space: &ImmixSpace<VM>,
        scheduler: &GCWorkScheduler<VM>,
    ) {
        use crate::plan::immix::gc_work::CreateProcessRemsetWork;

        // Stop mutators

        // #[cfg(debug_assertions)]
        {
            use crate::scheduler::thread_local_gc_work::ScheduleExecuteThreadlocalCollectionWork;
            scheduler.work_buckets[WorkBucketStage::Unconstrained].add(
                StopMutators::<Context>::new_with_args(ScanStackSemantic::RootsOnly),
            );
            #[cfg(debug_assertions)]
            debug_assert!(plan.base().global_state.objects.lock().unwrap().is_empty());
            // mutators have not reahced safepoint yet, so one cannot iterate through mutators here
            // Instead, craete a work packet in Local bucket and do it there. All mutators are guaranteed
            // to be safe at that point.
            scheduler.work_buckets[WorkBucketStage::Local]
                .set_sentinel(Box::new(ScheduleExecuteThreadlocalCollectionWork));
        }

        // Prepare global/collectors/mutators
        scheduler.work_buckets[WorkBucketStage::Prepare].add(Prepare::<Context>::new(plan));
        // Scan thread-local remember set
        // cannnot create now as the remember set has not been updated yet
        scheduler.work_buckets[WorkBucketStage::Prepare]
            .add(CreateProcessRemsetWork::<VM, Self>::new());

        #[cfg(debug_assertions)]
        {
            use crate::scheduler::single_thread_gc_work::STTrace;
            // The following is for debug purpose
            scheduler.work_buckets[WorkBucketStage::SecondRoots].add(STTrace::<
                VM,
                Self,
                { crate::policy::gc_work::TRACE_KIND_VERIFY_PUBLIC },
            >::new());
        }

        // PUblic GC can only release LOS objects
        // Release global/collectors/mutators
        scheduler.work_buckets[WorkBucketStage::Release].add(Release::<Context>::new(plan));
    }

    /// Schedule a full heap immix collection. This method is used by immix/genimmix/stickyimmix
    /// to schedule a full heap collection. A plan must call set_collection_kind and set_gc_status before this method.
    pub(crate) fn schedule_immix_full_heap_collection<
        PlanType: Plan<VM = VM>,
        FastContext: GCWorkContext<VM = VM, PlanType = PlanType>,
        DefragContext: GCWorkContext<VM = VM, PlanType = PlanType>,
    >(
        plan: &'static PlanType,
        immix_space: &ImmixSpace<VM>,
        scheduler: &GCWorkScheduler<VM>,
    ) {
        let in_defrag = immix_space.decide_whether_to_defrag(
            plan.base().global_state.is_emergency_collection(),
            true,
            plan.base()
                .global_state
                .cur_collection_attempts
                .load(Ordering::SeqCst),
            plan.base().global_state.is_user_triggered_collection(),
            *plan.base().options.full_heap_system_gc,
        );

        if in_defrag {
            scheduler.schedule_common_work::<DefragContext>(plan);
        } else {
            scheduler.schedule_common_work::<FastContext>(plan);
        }

        #[cfg(debug_assertions)]
        {
            use crate::scheduler::single_thread_gc_work::STTrace;
            // The following is for debug purpose
            scheduler.work_buckets[WorkBucketStage::SecondRoots].add(STTrace::<
                VM,
                Self,
                { crate::policy::gc_work::TRACE_KIND_VERIFY },
            >::new());
        }
    }

    pub(in crate::plan) fn set_last_gc_was_defrag(&self, defrag: bool, order: Ordering) {
        self.last_gc_was_defrag.store(defrag, order)
    }

    #[cfg(feature = "thread_local_gc")]
    fn do_immix_thread_local_collection_impl<PlanType: Plan<VM = VM>, const KIND: TraceKind>(
        tls: VMMutatorThread,
        plan: &'static PlanType,
        mmtk: &'static crate::MMTK<VM>,
    ) {
        use crate::scheduler::thread_local_gc_work::{
            EndOfThreadLocalGC, PlanThreadlocalObjectGraphTraversalClosure, ScanMutator,
            ThreadlocalFinalization, ThreadlocalRelease,
        };

        #[cfg(debug_assertions)]
        {
            use crate::policy::immix::TRACE_KIND_THREAD_LOCAL_COPY;
            #[cfg(not(feature = "thread_local_gc_copying"))]
            {
                use crate::policy::immix::TRACE_KIND_THREAD_LOCAL_COPY;
                debug_assert_eq!(KIND, TRACE_KIND_THREAD_LOCAL_FAST);
            }

            #[cfg(feature = "thread_local_gc_copying")]
            debug_assert!(KIND == TRACE_KIND_THREAD_LOCAL_COPY);
        }

        {
            use crate::{policy::immix::line::Line, vm::ActivePlan};

            let plan = mmtk
                .get_plan()
                .downcast_ref::<crate::plan::immix::Immix<VM>>()
                .unwrap();
            let mutator = VM::VMActivePlan::mutator(tls);
            let line_mark_state = plan.immix_space.line_mark_state.load(Ordering::SeqCst);
            mutator.state = if 1 + line_mark_state > Line::MAX_MARK_STATE {
                Line::RESET_MARK_STATE
            } else {
                1 + line_mark_state
            };
        }

        // Prepare global/collectors/mutators
        ThreadlocalPrepare::<VM>::new(tls).execute();

        //Scan mutator
        ScanMutator::<
            VM,
            PlanThreadlocalObjectGraphTraversalClosure<VM, Immix<VM>, KIND>,
            KIND,
        >::new(tls, mmtk, None)
        .execute();

        // Finalization has to be done before Release as it may resurrect objects
        if !*plan.base().options.no_finalizer {
            // finalization
            ThreadlocalFinalization::<
                VM,
                PlanThreadlocalObjectGraphTraversalClosure<VM, Immix<VM>, KIND>,
            >::new(tls, mmtk)
            .do_finalization();
        }

        ThreadlocalRelease::<VM>::new(tls).execute();
        let mut end_of_thread_local_gc = EndOfThreadLocalGC { _tls: tls };

        end_of_thread_local_gc.execute(mmtk);
    }
}

#[cfg(feature = "thread_local_gc")]
impl<VM: VMBinding> PlanThreadlocalTraceObject<VM> for Immix<VM> {
    fn thread_local_post_scan_object<const KIND: crate::policy::gc_work::TraceKind>(
        &self,
        mutator: &Mutator<VM>,
        object: ObjectReference,
    ) {
        if self.immix_space.in_space(object) {
            <ImmixSpace<VM> as PolicyThreadlocalTraceObject<VM>>::thread_local_post_scan_object::<
                KIND,
            >(&self.immix_space, mutator, object);
            return;
        }
        <CommonPlan<VM> as PlanThreadlocalTraceObject<VM>>::thread_local_post_scan_object::<KIND>(
            &self.common,
            mutator,
            object,
        )
    }

    fn thread_local_may_move_objects<const KIND: crate::policy::gc_work::TraceKind>() -> bool {
        <ImmixSpace<VM> as PolicyThreadlocalTraceObject<VM>>::thread_local_may_move_objects::<KIND>(
        ) || <CommonPlan<VM> as PlanThreadlocalTraceObject<VM>>::thread_local_may_move_objects::<KIND>(
        )
    }

    fn thread_local_trace_object<const KIND: crate::policy::gc_work::TraceKind>(
        &self,
        mutator: &mut Mutator<VM>,
        source: ObjectReference,
        slot: Option<VM::VMSlot>,
        object: ObjectReference,
        worker: Option<*mut GCWorker<VM>>,
    ) -> ThreadlocalTracedObjectType {
        if self.immix_space.in_space(object) {
            return <ImmixSpace<VM> as PolicyThreadlocalTraceObject<VM>>::thread_local_trace_object::<
                KIND,
            >(
                &self.immix_space,
                mutator,
                source,
                slot,
                object,
                worker,
                Some(CopySemantics::DefaultCopy),
            );
        }
        <CommonPlan<VM> as PlanThreadlocalTraceObject<VM>>::thread_local_trace_object::<KIND>(
            &self.common,
            mutator,
            source,
            slot,
            object,
            worker,
        )
    }
}
