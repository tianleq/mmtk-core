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
use crate::policy::immix::ImmixSpaceArgs;
use crate::policy::immix::{
    TRACE_KIND_DEFRAG, TRACE_KIND_FAST, TRACE_THREAD_LOCAL_DEFRAG, TRACE_THREAD_LOCAL_FAST,
};
use crate::policy::space::Space;
use crate::scheduler::thread_local_gc_work::ThreadLocalPrepare;
use crate::scheduler::thread_local_gc_work::ThreadLocalRelease;
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
        scheduler: &GCWorkScheduler<Self::VM>,
    ) {
        self.base().set_collection_kind::<Self>(self);
        self.base().set_gc_status(GcStatus::GcPrepare);
        Self::schedule_immix_thread_local_collection::<
            Immix<VM>,
            ImmixGCWorkContext<VM, TRACE_THREAD_LOCAL_FAST>,
            ImmixGCWorkContext<VM, TRACE_THREAD_LOCAL_DEFRAG>,
        >(tls, self, &self.immix_space, scheduler)
    }

    fn get_allocator_mapping(&self) -> &'static EnumMap<AllocationSemantics, AllocatorSelector> {
        &ALLOCATOR_MAPPING
    }

    fn prepare(&mut self, tls: VMWorkerThread) {
        self.common.prepare(tls, true);
        self.immix_space.prepare(true);
    }

    fn thread_local_prepare(&mut self, tls: VMMutatorThread) {
        self.common.thread_local_prepare(tls);
        self.immix_space.thread_local_prepare(tls);
    }

    fn thread_local_release(&mut self, tls: VMMutatorThread) {
        // at the moment, thread-local gc only reclaiming immix space
        self.common.thread_local_release(tls);
        self.immix_space.thread_local_release(tls);
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
        scheduler: &GCWorkScheduler<VM>,
    ) {
        use crate::scheduler::gc_work::*;
        let in_defrag = immix_space.decide_whether_to_defrag(
            plan.is_emergency_collection(),
            false,
            plan.base().cur_collection_attempts.load(Ordering::SeqCst),
            plan.base().is_user_triggered_collection(),
            *plan.base().options.full_heap_system_gc,
        );

        if in_defrag {
            //Scan mutator
            scheduler.work_buckets[WorkBucketStage::Unconstrained]
                .add(ScanMutator::<DefragContext::ProcessEdgesWorkType>::new(tls));
            // Prepare global/collectors/mutators
            scheduler.work_buckets[WorkBucketStage::Prepare]
                .add(ThreadLocalPrepare::<DefragContext>::new(plan, tls));

            // Release global/collectors/mutators
            scheduler.work_buckets[WorkBucketStage::Release]
                .add(ThreadLocalRelease::<DefragContext>::new(plan, tls));
        } else {
            //Scan mutator
            scheduler.work_buckets[WorkBucketStage::Unconstrained]
                .add(ScanMutator::<FastContext::ProcessEdgesWorkType>::new(tls));
            // Prepare global/collectors/mutators
            scheduler.work_buckets[WorkBucketStage::Prepare]
                .add(ThreadLocalPrepare::<FastContext>::new(plan, tls));

            // Release global/collectors/mutators
            scheduler.work_buckets[WorkBucketStage::Release]
                .add(ThreadLocalRelease::<FastContext>::new(plan, tls));
        }
    }
}
