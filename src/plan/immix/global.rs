use super::gc_work::ImmixGCWorkContext;
use super::mutator::ALLOCATOR_MAPPING;
use crate::plan::barriers::BarrierSelector;
use crate::plan::global::BasePlan;
use crate::plan::global::CommonPlan;
use crate::plan::global::CreateGeneralPlanArgs;
use crate::plan::global::CreateSpecificPlanArgs;
use crate::plan::global::GcStatus;
use crate::plan::AllocationSemantics;
use crate::plan::Plan;
use crate::plan::PlanConstraints;
use crate::plan::PlanThreadlocalTraceObject;
use crate::policy::gc_work::PolicyThreadlocalTraceObject;
use crate::policy::immix::ImmixSpaceArgs;
use crate::policy::immix::{
    TRACE_KIND_DEFRAG, TRACE_KIND_FAST, TRACE_THREAD_LOCAL_DEFRAG, TRACE_THREAD_LOCAL_FAST,
};
use crate::policy::space::Space;
use crate::scheduler::thread_local_gc_work::ScanMutator;
use crate::scheduler::thread_local_gc_work::ThreadlocalPrepare;
use crate::scheduler::thread_local_gc_work::ThreadlocalSentinel;
use crate::scheduler::*;
use crate::util::alloc::allocators::AllocatorSelector;
use crate::util::copy::*;
use crate::util::heap::VMRequest;
use crate::util::metadata::side_metadata::SideMetadataContext;
use crate::util::metadata::side_metadata::SideMetadataSanity;
use crate::util::ObjectReference;
use crate::util::VMMutatorThread;
use crate::vm::VMBinding;
use crate::{policy::immix::ImmixSpace, util::opaque_pointer::VMWorkerThread};
use std::sync::atomic::AtomicBool;

use atomic::Ordering;
use enum_map::EnumMap;

use mmtk_macros::PlanTraceObject;

#[derive(PlanTraceObject)]
pub struct Immix<VM: VMBinding> {
    #[post_scan]
    #[trace(CopySemantics::DefaultCopy)]
    pub immix_space: ImmixSpace<VM>,
    #[fallback_trace]
    pub common: CommonPlan<VM>,
    last_gc_was_defrag: AtomicBool,
}

pub const IMMIX_CONSTRAINTS: PlanConstraints = PlanConstraints {
    moves_objects: crate::policy::immix::DEFRAG,
    gc_header_bits: 2,
    gc_header_words: 0,
    num_specialized_scans: 1,
    /// Max immix object size is half of a block.
    max_non_los_default_alloc_bytes: crate::policy::immix::MAX_IMMIX_OBJECT_SIZE,
    barrier: BarrierSelector::PublicObjectMarkingBarrier,
    ..PlanConstraints::default()
};

impl<VM: VMBinding> Plan for Immix<VM> {
    type VM = VM;

    fn collection_required(&self, space_full: bool, _space: Option<&dyn Space<Self::VM>>) -> bool {
        self.base().collection_required(self, space_full)
    }

    #[cfg(feature = "thread_local_gc")]
    fn thread_local_collection_required(
        &self,
        _space_full: bool,
        _space: Option<&dyn Space<Self::VM>>,
    ) -> bool {
        false
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

    fn get_spaces(&self) -> Vec<&dyn Space<Self::VM>> {
        let mut ret = self.common.get_spaces();
        ret.push(&self.immix_space);
        ret
    }

    fn schedule_collection(&'static self, scheduler: &GCWorkScheduler<VM>) {
        self.base().set_collection_kind::<Self>(self);
        self.base().set_gc_status(GcStatus::GcPrepare);
        Self::schedule_immix_full_heap_collection::<
            Immix<VM>,
            ImmixGCWorkContext<VM, TRACE_KIND_FAST>,
            ImmixGCWorkContext<VM, TRACE_KIND_DEFRAG>,
        >(self, &self.immix_space, scheduler)
    }

    fn schedule_thread_local_collection(
        &'static self,
        tls: VMMutatorThread,
        worker: &mut GCWorker<Self::VM>,
    ) {
        self.base().set_collection_kind::<Self>(self);
        // self.base().set_gc_status(GcStatus::GcPrepare);
        Self::schedule_immix_thread_local_collection::<
            Immix<VM>,
            ImmixGCWorkContext<VM, TRACE_THREAD_LOCAL_FAST>,
            ImmixGCWorkContext<VM, TRACE_THREAD_LOCAL_DEFRAG>,
        >(tls, self, &self.immix_space, worker)
    }

    fn get_allocator_mapping(&self) -> &'static EnumMap<AllocationSemantics, AllocatorSelector> {
        &ALLOCATOR_MAPPING
    }

    fn prepare(&mut self, tls: VMWorkerThread) {
        self.common.prepare(tls, true);
        self.immix_space.prepare(true);
    }

    fn release(&mut self, tls: VMWorkerThread) {
        self.common.release(tls, true);
        // release the collected region
        self.last_gc_was_defrag
            .store(self.immix_space.release(true), Ordering::Relaxed);
    }

    fn get_collection_reserved_pages(&self) -> usize {
        self.immix_space.defrag_headroom_pages()
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

    fn publish_object(&self, object: ObjectReference) {
        if self.immix_space.in_space(object) {
            self.immix_space.publish_object(object);
        } else {
            self.common().publish_object(object);
        }
    }

    fn get_object_owner(&self, _object: ObjectReference) -> Option<u32> {
        if self.immix_space.in_space(_object) {
            return Some(self.immix_space.get_object_owner(_object));
        }
        Option::None
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
                plan_args.get_space_args("immix", true, VMRequest::discontiguous()),
                space_args,
            ),
            common: CommonPlan::new(plan_args),
            last_gc_was_defrag: AtomicBool::new(false),
        };

        {
            let mut side_metadata_sanity_checker = SideMetadataSanity::new();
            immix
                .common
                .verify_side_metadata_sanity(&mut side_metadata_sanity_checker);
            immix
                .immix_space
                .verify_side_metadata_sanity(&mut side_metadata_sanity_checker);
        }

        immix
    }

    /// Schedule a full heap immix collection. This method is used by immix/genimmix/stickyimmix
    /// to schedule a full heap collection. A plan must call set_collection_kind and set_gc_status before this method.
    pub(crate) fn schedule_immix_full_heap_collection<
        PlanType: Plan<VM = VM>,
        FastContext: 'static + GCWorkContext<VM = VM, PlanType = PlanType>,
        DefragContext: 'static + GCWorkContext<VM = VM, PlanType = PlanType>,
    >(
        plan: &'static DefragContext::PlanType,
        immix_space: &ImmixSpace<VM>,
        scheduler: &GCWorkScheduler<VM>,
    ) {
        let in_defrag = immix_space.decide_whether_to_defrag(
            plan.is_emergency_collection(),
            true,
            plan.base().cur_collection_attempts.load(Ordering::SeqCst),
            plan.base().is_user_triggered_collection(),
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

    pub(crate) fn schedule_immix_thread_local_collection<
        PlanType: Plan<VM = VM>,
        FastContext: 'static + GCWorkContext<VM = VM, PlanType = PlanType>,
        DefragContext: 'static + GCWorkContext<VM = VM, PlanType = PlanType>,
    >(
        tls: VMMutatorThread,
        plan: &'static PlanType,
        immix_space: &ImmixSpace<VM>,
        worker: &mut GCWorker<VM>,
    ) {
        let in_defrag = immix_space.decide_whether_to_defrag(
            plan.is_emergency_collection(),
            false,
            plan.base().cur_collection_attempts.load(Ordering::SeqCst),
            plan.base().is_user_triggered_collection(),
            *plan.base().options.full_heap_system_gc,
        );
        assert!(!in_defrag, "defrag has not been implemented");
        if in_defrag {
            //Scan mutator
            ScanMutator::<DefragContext::ThreadlocalProcessEdgesWorkType>::new(tls)
                .do_work(worker, worker.mmtk);
            // worker.scheduler().work_buckets[WorkBucketStage::Unconstrained].add(ScanMutator::<
            //     DefragContext::ProcessEdgesWorkType,
            // >::new(
            //     mutator_id
            // ));

            // Prepare global/collectors/mutators
            ThreadlocalPrepare::<DefragContext>::new(plan, tls).do_work(worker, worker.mmtk);
            // worker.scheduler().work_buckets[WorkBucketStage::Prepare].add(ThreadlocalPrepare::<
            //     DefragContext,
            // >::new(
            //     plan, mutator_id
            // ));

            // Release global/collectors/mutators
            worker.scheduler().work_buckets[WorkBucketStage::Unconstrained].set_local_sentinel(
                Box::new(ThreadlocalSentinel::<DefragContext>::new(plan, tls)),
            );
            // worker.scheduler().work_buckets[WorkBucketStage::Release].add(ThreadlocalRelease::<
            //     DefragContext,
            // >::new(
            //     plan, mutator_id
            // ));
        } else {
            //Scan mutator
            ScanMutator::<FastContext::ThreadlocalProcessEdgesWorkType>::new(tls)
                .do_work(worker, worker.mmtk);
            // Prepare global/collectors/mutators
            ThreadlocalPrepare::<FastContext>::new(plan, tls).do_work(worker, worker.mmtk);

            // Release global/collectors/mutators
            worker.scheduler().work_buckets[WorkBucketStage::Unconstrained]
                .set_local_sentinel(Box::new(ThreadlocalSentinel::<FastContext>::new(plan, tls)));
        }
    }
}

impl<VM: VMBinding> PlanThreadlocalTraceObject<VM> for Immix<VM> {
    fn thread_local_post_scan_object(&self, object: ObjectReference) {
        if self.immix_space.in_space(object) {
            <ImmixSpace<VM> as PolicyThreadlocalTraceObject<VM>>::thread_local_post_scan_object(
                &self.immix_space,
                object,
            );
            return;
        }
        <CommonPlan<VM> as PlanThreadlocalTraceObject<VM>>::thread_local_post_scan_object(
            &self.common,
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

    fn thread_local_trace_object<
        Q: crate::ObjectQueue,
        const KIND: crate::policy::gc_work::TraceKind,
    >(
        &self,
        queue: &mut Q,
        object: ObjectReference,
        mutator_id: u32,
        worker: &mut GCWorker<VM>,
    ) -> ObjectReference {
        if self.immix_space.in_space(object) {
            <ImmixSpace<VM> as PolicyThreadlocalTraceObject<VM>>::thread_local_trace_object::<
                Q,
                KIND,
            >(
                &self.immix_space,
                queue,
                object,
                Some(CopySemantics::DefaultCopy),
                mutator_id,
                worker,
            );
        }
        <CommonPlan<VM> as PlanThreadlocalTraceObject<VM>>::thread_local_trace_object::<Q, KIND>(
            &self.common,
            queue,
            object,
            mutator_id,
            worker,
        )
    }
}
