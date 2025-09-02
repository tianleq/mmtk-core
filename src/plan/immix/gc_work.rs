use super::global::Immix;
use crate::policy::gc_work::TraceKind;
use crate::policy::gc_work::TRACE_KIND_TRANSITIVE_PIN;
use crate::scheduler::gc_work::PlanProcessEdges;
use std::marker::PhantomData;

// #[cfg(feature = "thread_local_gc")]
// use crate::scheduler::thread_local_gc_work::PlanThreadlocalProcessEdges;
use crate::vm::VMBinding;

pub(super) struct ImmixGCWorkContext<VM: VMBinding, const KIND: TraceKind>(
    std::marker::PhantomData<VM>,
);
impl<VM: VMBinding, const KIND: TraceKind> crate::scheduler::GCWorkContext
    for ImmixGCWorkContext<VM, KIND>
{
    type VM = VM;
    type PlanType = Immix<VM>;
    type DefaultProcessEdges = PlanProcessEdges<VM, Immix<VM>, KIND>;
    type PinningProcessEdges = PlanProcessEdges<VM, Immix<VM>, TRACE_KIND_TRANSITIVE_PIN>;
}

pub(super) struct CreateProcessModBufWork<VM, P>
where
    VM: VMBinding,
    P: crate::Plan<VM = VM> + crate::plan::PlanTraceObject<VM>,
{
    _p: PhantomData<(VM, P)>,
}

impl<VM, P> CreateProcessModBufWork<VM, P>
where
    VM: VMBinding,
    P: crate::Plan<VM = VM> + crate::plan::PlanTraceObject<VM>,
{
    pub fn new() -> Self {
        Self { _p: PhantomData }
    }
}

unsafe impl<VM, P> Send for CreateProcessModBufWork<VM, P>
where
    VM: VMBinding,
    P: crate::Plan<VM = VM> + crate::plan::PlanTraceObject<VM>,
{
}

impl<VM, P> crate::scheduler::GCWork<VM> for CreateProcessModBufWork<VM, P>
where
    VM: VMBinding,
    P: crate::Plan<VM = VM> + crate::plan::PlanTraceObject<VM>,
{
    fn do_work(
        &mut self,
        worker: &mut crate::scheduler::GCWorker<VM>,
        _mmtk: &'static crate::MMTK<VM>,
    ) {
        use crate::plan::immix::concurrent_gc_work::ProcessModBufSATB;
        use crate::scheduler::WorkBucketStage;
        use crate::vm::ActivePlan;

        for mutator in <VM as VMBinding>::VMActivePlan::mutators() {
            worker.scheduler().work_buckets[WorkBucketStage::Closure].add(
                ProcessModBufSATB::<VM, P>::new(*mutator.remember_set.clone()),
            );
        }
    }
}
