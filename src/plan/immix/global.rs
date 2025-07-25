use super::mutator::ALLOCATOR_MAPPING;
#[cfg(feature = "satb")]
use super::Pause;
use crate::plan::global::BasePlan;
use crate::plan::global::CommonPlan;
use crate::plan::global::CreateGeneralPlanArgs;
use crate::plan::global::CreateSpecificPlanArgs;
use crate::plan::immix::gc_work::ImmixGCWorkContext;
use crate::plan::AllocationSemantics;
use crate::plan::Plan;
use crate::plan::PlanConstraints;
use crate::policy::immix::ImmixSpaceArgs;
use crate::policy::immix::TRACE_KIND_DEFRAG;
use crate::policy::immix::TRACE_KIND_FAST;
use crate::policy::space::Space;
use crate::scheduler::*;
use crate::util::alloc::allocators::AllocatorSelector;
use crate::util::copy::*;
use crate::util::heap::gc_trigger::SpaceStats;
use crate::util::heap::VMRequest;
use crate::util::metadata::side_metadata::SideMetadataContext;
use crate::vm::VMBinding;
use crate::{policy::immix::ImmixSpace, util::opaque_pointer::VMWorkerThread};
use std::sync::atomic::AtomicBool;

use atomic::Atomic;
use atomic::Ordering;
use enum_map::EnumMap;

use mmtk_macros::{HasSpaces, PlanTraceObject};

#[cfg(feature = "satb")]
#[derive(Debug, Clone, Copy, bytemuck::NoUninit, PartialEq, Eq)]
#[repr(u8)]
enum GCCause {
    Unknown,
    FullHeap,
    InitialMark,
    FinalMark,
}

#[derive(HasSpaces, PlanTraceObject)]
pub struct Immix<VM: VMBinding> {
    #[post_scan]
    #[space]
    #[copy_semantics(CopySemantics::DefaultCopy)]
    pub immix_space: ImmixSpace<VM>,
    #[parent]
    pub common: CommonPlan<VM>,
    last_gc_was_defrag: AtomicBool,
    #[cfg(feature = "satb")]
    current_pause: Atomic<Option<Pause>>,
    #[cfg(feature = "satb")]
    previous_pause: Atomic<Option<Pause>>,
    #[cfg(feature = "satb")]
    gc_cause: Atomic<GCCause>,
}

/// The plan constraints for the immix plan.
pub const IMMIX_CONSTRAINTS: PlanConstraints = PlanConstraints {
    // If we disable moving in Immix, this is a non-moving plan.
    moves_objects: !cfg!(feature = "immix_non_moving"),
    // Max immix object size is half of a block.
    max_non_los_default_alloc_bytes: crate::policy::immix::MAX_IMMIX_OBJECT_SIZE,
    needs_prepare_mutator: false,
    #[cfg(feature = "satb")]
    needs_log_bit: true,
    #[cfg(feature = "satb")]
    barrier: crate::BarrierSelector::SATBBarrier,
    ..PlanConstraints::default()
};

impl<VM: VMBinding> Plan for Immix<VM> {
    #[cfg(not(feature = "satb"))]
    fn collection_required(&self, space_full: bool, _space: Option<SpaceStats<Self::VM>>) -> bool {
        self.base().collection_required(self, space_full)
    }

    #[cfg(feature = "satb")]
    fn collection_required(&self, space_full: bool, _space: Option<SpaceStats<Self::VM>>) -> bool {
        if self.base().collection_required(self, space_full) {
            self.gc_cause.store(GCCause::FullHeap, Ordering::Release);
            return true;
        }

        let concurrent_marking_in_progress = self.concurrent_marking_in_progress();

        if concurrent_marking_in_progress && crate::concurrent_marking_packets_drained() {
            self.gc_cause.store(GCCause::FinalMark, Ordering::Release);
            return true;
        }
        let threshold = self.get_total_pages() >> 1;
        let concurrent_marking_threshold = self
            .common
            .base
            .global_state
            .concurrent_marking_threshold
            .load(Ordering::Acquire);
        if !concurrent_marking_in_progress && concurrent_marking_threshold > threshold {
            debug_assert!(crate::concurrent_marking_packets_drained());
            debug_assert!(!self.concurrent_marking_in_progress());
            let prev_pause = self.previous_pause();
            debug_assert!(prev_pause.is_none() || prev_pause.unwrap() != Pause::InitialMark);
            self.gc_cause.store(GCCause::InitialMark, Ordering::Release);
            return true;
        }
        false
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
        #[cfg(feature = "satb")]
        {
            let defrag = Self::is_defrag_collection::<
                Immix<VM>,
                ImmixGCWorkContext<VM, TRACE_KIND_DEFRAG>,
            >(self, &self.immix_space);
            self.current_pause.store(
                if defrag {
                    Some(Pause::FullDefrag)
                } else {
                    Some(Pause::Full)
                },
                Ordering::SeqCst,
            );
        }
        Self::schedule_immix_full_heap_collection::<
            Immix<VM>,
            ImmixGCWorkContext<VM, TRACE_KIND_FAST>,
            ImmixGCWorkContext<VM, TRACE_KIND_DEFRAG>,
        >(self, &self.immix_space, scheduler);
    }

    #[cfg(feature = "satb")]
    fn schedule_concurrent_collection(&'static self, scheduler: &GCWorkScheduler<Self::VM>) {
        let pause = self.select_collection_kind();
        if pause == Pause::Full {
            use crate::{
                plan::immix::gc_work::ImmixGCWorkContext,
                policy::immix::{TRACE_KIND_DEFRAG, TRACE_KIND_FAST},
            };
            let defrag = Self::is_defrag_collection::<
                Immix<VM>,
                ImmixGCWorkContext<VM, TRACE_KIND_DEFRAG>,
            >(self, &self.immix_space);
            self.current_pause.store(
                if defrag {
                    Some(Pause::FullDefrag)
                } else {
                    Some(Pause::Full)
                },
                Ordering::SeqCst,
            );

            Self::schedule_immix_full_heap_collection::<
                Immix<VM>,
                ImmixGCWorkContext<VM, TRACE_KIND_FAST>,
                ImmixGCWorkContext<VM, TRACE_KIND_DEFRAG>,
            >(self, &self.immix_space, scheduler);
        } else {
            // Set current pause kind
            self.current_pause.store(Some(pause), Ordering::SeqCst);
            // Schedule work
            match pause {
                Pause::InitialMark => self.schedule_concurrent_marking_initial_pause(scheduler),
                Pause::FinalMark => self.schedule_concurrent_marking_final_pause(scheduler),
                _ => unreachable!(),
            }
        }
    }

    fn get_allocator_mapping(&self) -> &'static EnumMap<AllocationSemantics, AllocatorSelector> {
        &ALLOCATOR_MAPPING
    }

    fn prepare(&mut self, tls: VMWorkerThread) {
        #[cfg(feature = "satb")]
        {
            let pause = self.current_pause().unwrap();
            match pause {
                Pause::Full | Pause::FullDefrag => {
                    self.common.prepare(tls, true);
                    self.immix_space.prepare(
                        true,
                        crate::policy::immix::defrag::StatsForDefrag::new(self),
                        pause,
                    );
                }
                Pause::InitialMark => {
                    // init prepare has to be executed first, otherwise, los objects will not be
                    // dealt with properly
                    self.common.initial_pause_prepare();
                    self.immix_space.initial_pause_prepare();
                    self.common.prepare(tls, true);
                    self.immix_space.prepare(
                        true,
                        crate::policy::immix::defrag::StatsForDefrag::new(self),
                        pause,
                    );
                }
                Pause::FinalMark => (),
            }
        }
        #[cfg(not(feature = "satb"))]
        {
            self.common.prepare(tls, true);
            self.immix_space.prepare(
                true,
                crate::policy::immix::defrag::StatsForDefrag::new(self),
                #[cfg(feature = "satb")]
                Pause::Full,
            );
        }
    }

    fn release(&mut self, tls: VMWorkerThread) {
        #[cfg(feature = "satb")]
        {
            let pause = self.current_pause().unwrap();
            match pause {
                Pause::Full | Pause::FullDefrag => {
                    self.common.release(tls, true);
                    // release the collected region
                    self.immix_space.release(true);
                }
                Pause::InitialMark => (),
                Pause::FinalMark => {
                    self.immix_space.final_pause_release();
                    self.common.final_pause_release();
                    self.common.release(tls, true);
                    // release the collected region
                    self.immix_space.release(true);
                }
            }
            // reset the concurrent marking page counting
            self.common()
                .base
                .global_state
                .concurrent_marking_threshold
                .store(0, Ordering::Release);
        }
        #[cfg(not(feature = "satb"))]
        {
            self.common.release(tls, true);
            // release the collected region
            self.immix_space.release(true);
        }
    }

    fn end_of_gc(&mut self, _tls: VMWorkerThread) {
        self.last_gc_was_defrag
            .store(self.immix_space.end_of_gc(), Ordering::Relaxed);
    }

    fn current_gc_may_move_object(&self) -> bool {
        self.immix_space.in_defrag()
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

    #[cfg(feature = "satb")]
    fn gc_pause_start(&self, _scheduler: &GCWorkScheduler<VM>) {
        use crate::vm::ActivePlan;
        let pause = self.current_pause().unwrap();
        match pause {
            Pause::Full | Pause::FullDefrag => {
                self.set_concurrent_marking_state(false);
            }
            Pause::InitialMark => {
                debug_assert!(
                    !self.concurrent_marking_in_progress(),
                    "prev pause: {:?}",
                    self.previous_pause().unwrap()
                );
            }
            Pause::FinalMark => {
                debug_assert!(self.concurrent_marking_in_progress());
                // Flush barrier buffers
                for mutator in <VM as VMBinding>::VMActivePlan::mutators() {
                    mutator.barrier.flush();
                }
                self.set_concurrent_marking_state(false);
            }
        }
        // #[cfg(debug_assertions)]
        // println!("pause {:?} start", pause);
    }

    #[cfg(feature = "satb")]
    fn gc_pause_end(&self) {
        // self.immix_space.flush_page_resource();
        let pause = self.current_pause().unwrap();
        if pause == Pause::InitialMark {
            self.set_concurrent_marking_state(true);
        }
        self.previous_pause.store(Some(pause), Ordering::SeqCst);
        self.current_pause.store(None, Ordering::SeqCst);
        // #[cfg(debug_assertions)]
        // println!("pause {:?} end", pause);
    }
}

impl<VM: VMBinding> Immix<VM> {
    pub fn new(args: CreateGeneralPlanArgs<VM>) -> Self {
        let spec = if cfg!(feature = "satb") {
            use crate::vm::ObjectModel;
            crate::util::metadata::extract_side_metadata(&[*VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC])
        } else {
            vec![]
        };

        let plan_args = CreateSpecificPlanArgs {
            global_args: args,
            constraints: &IMMIX_CONSTRAINTS,
            global_side_metadata_specs: SideMetadataContext::new_global_specs(&spec),
        };
        Self::new_with_args(
            plan_args,
            ImmixSpaceArgs {
                unlog_object_when_traced: false,
                #[cfg(feature = "vo_bit")]
                mixed_age: false,
                #[cfg(feature = "satb")]
                never_move_objects: true,
                #[cfg(not(feature = "satb"))]
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
            #[cfg(feature = "satb")]
            current_pause: Atomic::new(None),
            #[cfg(feature = "satb")]
            previous_pause: Atomic::new(None),
            #[cfg(feature = "satb")]
            gc_cause: Atomic::new(GCCause::Unknown),
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
    ) -> bool {
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
        in_defrag
    }

    pub(crate) fn is_defrag_collection<
        PlanType: Plan<VM = VM>,
        DefragContext: GCWorkContext<VM = VM, PlanType = PlanType>,
    >(
        plan: &'static DefragContext::PlanType,
        immix_space: &ImmixSpace<VM>,
    ) -> bool {
        immix_space.decide_whether_to_defrag(
            plan.base().global_state.is_emergency_collection(),
            true,
            plan.base()
                .global_state
                .cur_collection_attempts
                .load(Ordering::SeqCst),
            plan.base().global_state.is_user_triggered_collection(),
            *plan.base().options.full_heap_system_gc,
        )
    }

    #[cfg(feature = "satb")]
    fn select_collection_kind(&self) -> Pause {
        let emergency = self.base().global_state.is_emergency_collection();
        let user_triggered = self.base().global_state.is_user_triggered_collection();
        let concurrent_marking_in_progress = self.concurrent_marking_in_progress();
        let concurrent_marking_packets_drained = crate::concurrent_marking_packets_drained();

        if emergency || user_triggered {
            return Pause::Full;
        } else if !concurrent_marking_in_progress && concurrent_marking_packets_drained {
            return Pause::InitialMark;
        } else if concurrent_marking_in_progress && concurrent_marking_packets_drained {
            return Pause::FinalMark;
        }

        Pause::Full
    }

    #[cfg(feature = "satb")]
    fn disable_unnecessary_buckets(&'static self, scheduler: &GCWorkScheduler<VM>, pause: Pause) {
        if pause == Pause::InitialMark {
            scheduler.work_buckets[WorkBucketStage::Closure].set_as_disabled();
            scheduler.work_buckets[WorkBucketStage::WeakRefClosure].set_as_disabled();
            scheduler.work_buckets[WorkBucketStage::FinalRefClosure].set_as_disabled();
            scheduler.work_buckets[WorkBucketStage::PhantomRefClosure].set_as_disabled();
        }
        scheduler.work_buckets[WorkBucketStage::TPinningClosure].set_as_disabled();
        scheduler.work_buckets[WorkBucketStage::PinningRootsTrace].set_as_disabled();
        scheduler.work_buckets[WorkBucketStage::VMRefClosure].set_as_disabled();
        scheduler.work_buckets[WorkBucketStage::VMRefForwarding].set_as_disabled();
        scheduler.work_buckets[WorkBucketStage::SoftRefClosure].set_as_disabled();
        scheduler.work_buckets[WorkBucketStage::CalculateForwarding].set_as_disabled();
        scheduler.work_buckets[WorkBucketStage::SecondRoots].set_as_disabled();
        scheduler.work_buckets[WorkBucketStage::RefForwarding].set_as_disabled();
        scheduler.work_buckets[WorkBucketStage::FinalizableForwarding].set_as_disabled();
        scheduler.work_buckets[WorkBucketStage::Compact].set_as_disabled();
    }

    #[cfg(feature = "satb")]
    pub(crate) fn schedule_concurrent_marking_initial_pause(
        &'static self,
        scheduler: &GCWorkScheduler<VM>,
    ) {
        use crate::{
            plan::immix::{
                concurrent_marking::ProcessRootSlots, gc_work::ConcurrentImmixGCWorkContext,
            },
            scheduler::gc_work::{Prepare, StopMutators, UnsupportedProcessEdges},
        };

        self.disable_unnecessary_buckets(scheduler, Pause::InitialMark);

        scheduler.work_buckets[WorkBucketStage::Unconstrained].add_prioritized(Box::new(
            StopMutators::<ConcurrentImmixGCWorkContext<ProcessRootSlots<VM>>>::new_args(
                Pause::InitialMark,
            ),
        ));
        scheduler.work_buckets[WorkBucketStage::Prepare].add(Prepare::<
            ConcurrentImmixGCWorkContext<UnsupportedProcessEdges<VM>>,
        >::new(self));
    }

    #[cfg(feature = "satb")]
    fn schedule_concurrent_marking_final_pause(&'static self, scheduler: &GCWorkScheduler<VM>) {
        use super::gc_work::ConcurrentImmixGCWorkContext;
        use crate::{
            plan::immix::concurrent_marking::ProcessRootSlots,
            scheduler::{
                gc_work::{Release, StopMutators, UnsupportedProcessEdges},
                // single_thread_gc_work::STTrace,
            },
        };

        self.disable_unnecessary_buckets(scheduler, Pause::FinalMark);

        scheduler.work_buckets[WorkBucketStage::Unconstrained].add_prioritized(Box::new(
            StopMutators::<ConcurrentImmixGCWorkContext<ProcessRootSlots<VM>>>::new_args(
                Pause::FinalMark,
            ),
        ));

        // scheduler.work_buckets[WorkBucketStage::SecondRoots].add(STTrace::<
        //     VM,
        //     <crate::plan::immix::gc_work::ConcurrentImmixGCWorkContext<
        //         crate::scheduler::gc_work::UnsupportedProcessEdges<VM>,
        //     > as crate::scheduler::work::GCWorkContext>::PlanType,
        // >::new());
        scheduler.work_buckets[WorkBucketStage::Release].add(Release::<
            ConcurrentImmixGCWorkContext<UnsupportedProcessEdges<VM>>,
        >::new(self));
    }

    pub(in crate::plan) fn set_last_gc_was_defrag(&self, defrag: bool, order: Ordering) {
        self.last_gc_was_defrag.store(defrag, order)
    }

    #[cfg(feature = "satb")]
    pub fn concurrent_marking_in_progress(&self) -> bool {
        self.common()
            .base
            .global_state
            .concurrent_marking_active
            .load(Ordering::Acquire)
    }

    #[cfg(feature = "satb")]
    fn set_concurrent_marking_state(&self, active: bool) {
        use crate::vm::Collection;

        <VM as VMBinding>::VMCollection::set_concurrent_marking_state(active);
        self.common()
            .base
            .global_state
            .concurrent_marking_active
            .store(active, Ordering::SeqCst);
    }

    #[cfg(feature = "satb")]
    pub fn current_pause(&self) -> Option<Pause> {
        self.current_pause.load(Ordering::SeqCst)
    }

    #[cfg(feature = "satb")]
    pub fn previous_pause(&self) -> Option<Pause> {
        self.previous_pause.load(Ordering::SeqCst)
    }

    #[cfg(feature = "satb")]
    pub fn is_marked(&self, o: crate::util::ObjectReference) -> bool {
        if self.immix_space.in_space(o) {
            self.immix_space.is_marked(o)
        } else {
            self.common.los.is_marked(o)
        }
    }
}
