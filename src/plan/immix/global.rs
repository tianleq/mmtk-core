use super::gc_work::ImmixGCWorkContext;
use super::mutator::ALLOCATOR_MAPPING;
use crate::plan::barriers::BarrierSelector;
use crate::plan::global::BasePlan;
use crate::plan::global::CommonPlan;
use crate::plan::global::CreateGeneralPlanArgs;
use crate::plan::global::CreateSpecificPlanArgs;
use crate::plan::AllocationSemantics;
use crate::plan::Plan;
use crate::plan::PlanConstraints;
#[cfg(feature = "thread_local_gc")]
use crate::plan::PlanThreadlocalTraceObject;
#[cfg(feature = "thread_local_gc")]
use crate::plan::ThreadlocalTracedObjectType;
#[cfg(feature = "thread_local_gc")]
use crate::policy::gc_work::PolicyThreadlocalTraceObject;
use crate::policy::immix::ImmixSpaceArgs;
use crate::policy::immix::{TRACE_KIND_DEFRAG, TRACE_KIND_FAST};
use crate::policy::space::Space;
#[cfg(feature = "thread_local_gc")]
use crate::scheduler::thread_local_gc_work::ThreadlocalPrepare;
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

use atomic::Ordering;
use enum_map::EnumMap;

use mmtk_macros::{HasSpaces, PlanTraceObject};

#[derive(HasSpaces, PlanTraceObject)]
pub struct Immix<VM: VMBinding> {
    #[post_scan]
    #[space]
    #[copy_semantics(CopySemantics::DefaultCopy)]
    pub immix_space: ImmixSpace<VM>,
    #[parent]
    pub common: CommonPlan<VM>,
    last_gc_was_defrag: AtomicBool,
}

/// The plan constraints for the immix plan.
pub const IMMIX_CONSTRAINTS: PlanConstraints = PlanConstraints {
    moves_objects: crate::policy::immix::DEFRAG,
    // Max immix object size is half of a block.
    max_non_los_default_alloc_bytes: crate::policy::immix::MAX_IMMIX_OBJECT_SIZE,
    barrier: BarrierSelector::PublicObjectMarkingBarrier,
    ..PlanConstraints::default()
};

pub const DEFRAG_MUTATOR_THRESHOLD: usize = 32;

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

        use crate::vm::ActivePlan;

        let mutator = VM::VMActivePlan::mutator(_tls);

        mutator.local_allocation_size >= self.options().get_thread_local_heap_size()
    }

    fn last_collection_was_exhaustive(&self) -> bool {
        ImmixSpace::<VM>::is_last_gc_exhaustive(self.last_gc_was_defrag.load(Ordering::Relaxed))
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
        Self::schedule_immix_full_heap_collection::<
            Immix<VM>,
            ImmixGCWorkContext<VM, TRACE_KIND_FAST>,
            ImmixGCWorkContext<VM, TRACE_KIND_DEFRAG>,
        >(self, &self.immix_space, scheduler)
    }

    #[cfg(feature = "thread_local_gc")]
    fn do_thread_local_collection(
        &'static self,
        tls: VMMutatorThread,
        mmtk: &'static crate::MMTK<VM>,
    ) {
        use crate::policy::immix::{TRACE_THREAD_LOCAL_COPY, TRACE_THREAD_LOCAL_FAST};

        Self::schedule_and_do_immix_thread_local_collection::<
            Immix<VM>,
            ImmixGCWorkContext<VM, TRACE_THREAD_LOCAL_FAST>,
            ImmixGCWorkContext<VM, TRACE_THREAD_LOCAL_COPY>,
        >(tls, self, &self.immix_space, mmtk)
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
                    { crate::policy::immix::TRACE_THREAD_LOCAL_DEFRAG },
                >,
            >::new(_tls, _mmtk, Some(_worker))
            .execute();
            // cannot do release since finalizer has not been executed yet
            // objects may be resurrected
        }
    }

    #[cfg(feature = "thread_local_gc_copying")]
    fn defrag_mutator_required(&self, tls: VMMutatorThread) -> bool {
        use crate::vm::ActivePlan;
        use std::borrow::Borrow;

        let mutator = VM::VMActivePlan::mutator(tls);
        let allocators: &crate::util::alloc::allocators::Allocators<VM> =
            mutator.allocators.borrow();

        let immix_allocator = unsafe {
            allocators.get_allocator(mutator.config.allocator_mapping[AllocationSemantics::Default])
        }
        .downcast_ref::<crate::util::alloc::ImmixAllocator<VM>>()
        .unwrap();
        if immix_allocator.local_reusable_blocks.len() >= DEFRAG_MUTATOR_THRESHOLD {
            println!(
                "mutator: {}, local reusable blocks: {}",
                mutator.mutator_id,
                immix_allocator.local_reusable_blocks.len()
            );
            true
        } else {
            false
        }
    }

    fn get_allocator_mapping(&self) -> &'static EnumMap<AllocationSemantics, AllocatorSelector> {
        &ALLOCATOR_MAPPING
    }

    fn prepare(&mut self, tls: VMWorkerThread) {
        self.common.prepare(tls, true);
        self.immix_space.prepare(
            true,
            crate::policy::immix::defrag::StatsForDefrag::new(self),
        );
    }

    fn release(&mut self, tls: VMWorkerThread) {
        self.common.release(tls, true);
        // release the collected region
        self.immix_space.release(true);
    }

    fn end_of_gc(&mut self, _tls: VMWorkerThread) {
        self.last_gc_was_defrag
            .store(self.immix_space.end_of_gc(), Ordering::Relaxed);
    }

    fn current_gc_may_move_object(&self) -> bool {
        self.immix_space.in_defrag()
    }

    fn get_collection_reserved_pages(&self) -> usize {
        let defrag_headroom = self.immix_space.defrag_headroom_pages();
        let mut reserved = defrag_headroom;
        #[cfg(feature = "thread_local_gc_copying")]
        {
            reserved +=
                self.options().get_thread_local_heap_size() / crate::util::constants::BYTES_IN_PAGE;
        }
        return reserved;
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
    fn get_number_of_reusable_blocks(&self) -> usize {
        self.immix_space.reusable_blocks.len()
    }

    #[cfg(all(feature = "thread_local_gc", feature = "debug_publish_object"))]
    fn get_object_owner(&self, _object: ObjectReference) -> Option<u32> {
        if self.immix_space.in_space(_object) {
            return Some(self.immix_space.get_object_owner(_object));
        }
        if self.common.get_los().in_space(_object) {
            return Some(self.common.get_los().get_object_owner(_object));
        }
        None
    }

    #[cfg(feature = "debug_publish_object")]
    fn is_object_published(&self, object: ObjectReference) -> bool {
        debug_assert!(object.is_null() == false, "object is null");
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

    #[cfg(feature = "thread_local_gc_copying_stats")]
    fn print_local_heap_stats(&self, mutator: &mut crate::Mutator<VM>) {
        use std::{fs::OpenOptions, io::Write};

        use crate::{
            policy::immix::TOTAL_IMMIX_ALLOCATION_BYTES,
            util::{alloc::ImmixAllocator, GLOBAL_REQUEST_ID},
        };

        let result = self.immix_space.print_immixspace_stats();
        let live_los_pages = self.common().print_heap_stats();
        let allocator = unsafe {
            mutator
                .allocators
                .get_allocator_mut(mutator.config.allocator_mapping[AllocationSemantics::Default])
                .downcast_mut::<ImmixAllocator<VM>>()
                .unwrap()
        };
        let local_blocks_count = allocator.local_blocks.len()
            + allocator.local_free_blocks.len()
            + allocator.local_reusable_blocks.len();
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open("/home/tianleq/misc/local-gc-heap-stats.log")
            .unwrap();
        writeln!(
            file,
            "{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}",
            TOTAL_IMMIX_ALLOCATION_BYTES.load(Ordering::SeqCst),
            GLOBAL_REQUEST_ID.load(Ordering::SeqCst),
            mutator.mutator_id,
            result.0,
            result.1,
            result.2,
            result.3,
            result.4,
            result.5,
            live_los_pages,
            local_blocks_count
        )
        .unwrap();
    }

    #[cfg(feature = "thread_local_gc_copying_stats")]
    fn print_global_heap_stats(&self) {
        use crate::util::alloc::ImmixAllocator;
        use crate::vm::ActivePlan;
        use crate::{policy::immix::TOTAL_IMMIX_ALLOCATION_BYTES, util::GLOBAL_REQUEST_ID};
        use std::{fs::OpenOptions, io::Write};

        let (
            reusable_block,
            global_reusable_block,
            allocated_block,
            published_block,
            available_lines,
            sparse_block,
        ) = self.immix_space.print_immixspace_stats();
        let free_pages = self.get_collection_reserved_pages();
        let mut local_resuable_blocks = 0;
        for mutator in VM::VMActivePlan::mutators() {
            let allocator = unsafe {
                mutator
                    .allocators
                    .get_allocator_mut(
                        mutator.config.allocator_mapping[AllocationSemantics::Default],
                    )
                    .downcast_mut::<ImmixAllocator<VM>>()
                    .unwrap()
            };
            local_resuable_blocks += allocator.local_reusable_blocks.len()
        }

        let live_los_pages = self.common().print_heap_stats();

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open("/home/tianleq/misc/global-gc-heap-stats.log")
            .unwrap();
        writeln!(
            file,
            "{}, {}, {}, {}, {}, {}, {}, {}, {}, {}|{}",
            TOTAL_IMMIX_ALLOCATION_BYTES.load(Ordering::SeqCst),
            GLOBAL_REQUEST_ID.load(Ordering::SeqCst),
            free_pages,
            reusable_block,
            global_reusable_block,
            allocated_block,
            published_block,
            available_lines,
            sparse_block,
            live_los_pages,
            local_resuable_blocks
        )
        .unwrap();
    }
}

impl<VM: VMBinding> Immix<VM> {
    pub fn new(args: CreateGeneralPlanArgs<VM>) -> Self {
        let plan_args = CreateSpecificPlanArgs {
            global_args: args,
            constraints: &IMMIX_CONSTRAINTS,
            global_side_metadata_specs: SideMetadataContext::new_global_specs(&[]),
        };
        Self::new_with_args(
            plan_args,
            ImmixSpaceArgs {
                reset_log_bit_in_major_gc: false,
                unlog_object_when_traced: false,
                #[cfg(feature = "vo_bit")]
                mixed_age: false,
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
        };

        immix.verify_side_metadata_sanity();

        immix
    }

    /// Schedule a full heap immix collection. This method is used by immix/genimmix/stickyimmix
    /// to schedule a full heap collection. A plan must call set_collection_kind and set_gc_status before this method.
    pub(crate) fn schedule_immix_full_heap_collection<
        PlanType: Plan<VM = VM>,
        FastContext: GCWorkContext<VM = VM, PlanType = PlanType>,
        DefragContext: GCWorkContext<VM = VM, PlanType = PlanType>,
    >(
        plan: &'static DefragContext::PlanType,
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
    }

    pub(in crate::plan) fn set_last_gc_was_defrag(&self, defrag: bool, order: Ordering) {
        self.last_gc_was_defrag.store(defrag, order)
    }

    #[cfg(feature = "thread_local_gc")]
    pub(crate) fn schedule_and_do_immix_thread_local_collection<
        PlanType: Plan<VM = VM>,
        FastContext: 'static + GCWorkContext<VM = VM, PlanType = PlanType>,
        DefragContext: 'static + GCWorkContext<VM = VM, PlanType = PlanType>,
    >(
        tls: VMMutatorThread,
        plan: &'static PlanType,
        _immix_space: &ImmixSpace<VM>,
        mmtk: &'static crate::MMTK<VM>,
    ) {
        use crate::policy::immix::{TRACE_THREAD_LOCAL_COPY, TRACE_THREAD_LOCAL_FAST};
        use crate::scheduler::thread_local_gc_work::{
            EndOfThreadLocalGC, PlanThreadlocalObjectGraphTraversalClosure, ScanMutator,
            ThreadlocalFinalization, ThreadlocalRelease,
        };

        #[cfg(not(feature = "thread_local_gc_copying"))]
        let in_defrag = false;
        #[cfg(feature = "thread_local_gc_copying")]
        let in_defrag = true;

        if in_defrag {
            // Prepare global/collectors/mutators
            ThreadlocalPrepare::<VM>::new(tls).execute();

            //Scan mutator
            ScanMutator::<
                VM,
                PlanThreadlocalObjectGraphTraversalClosure<VM, Immix<VM>, TRACE_THREAD_LOCAL_COPY>,
            >::new(tls, mmtk, None)
            .execute();

            // Finalization has to be done before Release as it may resurrect objects
            if !*plan.base().options.no_finalizer {
                // finalization
                ThreadlocalFinalization::<
                    VM,
                    PlanThreadlocalObjectGraphTraversalClosure<
                        VM,
                        Immix<VM>,
                        TRACE_THREAD_LOCAL_COPY,
                    >,
                >::new(tls, mmtk)
                .do_finalization();
            }

            ThreadlocalRelease::<VM>::new(tls).execute();
            let mut end_of_thread_local_gc = EndOfThreadLocalGC { tls };

            end_of_thread_local_gc.execute(mmtk);
        } else {
            // Prepare global/collectors/mutators
            ThreadlocalPrepare::<VM>::new(tls).execute();

            //Scan mutator
            ScanMutator::<
                VM,
                PlanThreadlocalObjectGraphTraversalClosure<VM, Immix<VM>, TRACE_THREAD_LOCAL_FAST>,
            >::new(tls, mmtk, None)
            .execute();

            // Finalization and then do release
            if !*plan.base().options.no_finalizer {
                // finalization
                ThreadlocalFinalization::<
                    VM,
                    PlanThreadlocalObjectGraphTraversalClosure<
                        VM,
                        Immix<VM>,
                        TRACE_THREAD_LOCAL_FAST,
                    >,
                >::new(tls, mmtk)
                .do_finalization();
            }
            ThreadlocalRelease::<VM>::new(tls).execute();
            let mut end_of_thread_local_gc = EndOfThreadLocalGC { tls };

            end_of_thread_local_gc.execute(mmtk);
        }
    }

    // #[cfg(feature = "thread_local_gc")]
    // fn get_thread_local_collection_reserved_pages(&self) -> usize {
    //     #[cfg(feature = "thread_local_gc_copying")]
    //     return self.immix_space.thread_local_gc_copy_reserve_pages();
    //     #[cfg(not(feature = "thread_local_gc_copying"))]
    //     return 0;
    // }
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
        false
            || <ImmixSpace<VM> as PolicyThreadlocalTraceObject<VM>>::thread_local_may_move_objects::<
                KIND,
            >()
            || <CommonPlan<VM> as PlanThreadlocalTraceObject<VM>>::thread_local_may_move_objects::<
                KIND,
            >()
    }

    #[cfg(not(feature = "debug_publish_object"))]
    fn thread_local_trace_object<const KIND: crate::policy::gc_work::TraceKind>(
        &self,
        mutator: &mut Mutator<VM>,
        object: ObjectReference,
        worker: Option<*mut GCWorker<VM>>,
    ) -> ThreadlocalTracedObjectType {
        if self.immix_space.in_space(object) {
            return <ImmixSpace<VM> as PolicyThreadlocalTraceObject<VM>>::thread_local_trace_object::<
                KIND,
            >(
                &self.immix_space,
                mutator,
                object,
                worker,
                Some(CopySemantics::DefaultCopy),
            );
        }
        <CommonPlan<VM> as PlanThreadlocalTraceObject<VM>>::thread_local_trace_object::<KIND>(
            &self.common,
            mutator,
            object,
            worker,
        )
    }

    #[cfg(feature = "debug_publish_object")]
    fn thread_local_trace_object<const KIND: crate::policy::gc_work::TraceKind>(
        &self,
        mutator: &mut Mutator<VM>,
        source: ObjectReference,
        slot: VM::VMEdge,
        object: ObjectReference,
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
                Some(CopySemantics::DefaultCopy),
            );
        }
        <CommonPlan<VM> as PlanThreadlocalTraceObject<VM>>::thread_local_trace_object::<KIND>(
            &self.common,
            mutator,
            source,
            slot,
            object,
        )
    }
}
