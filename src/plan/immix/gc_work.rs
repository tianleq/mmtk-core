use itertools::Itertools;

use super::global::Immix;
use crate::plan::immix::concurrent_gc_work::ProcessObjectRemset;
use crate::policy::gc_work::TraceKind;
use crate::policy::gc_work::TRACE_KIND_TRANSITIVE_PIN;
use crate::scheduler::gc_work::PlanProcessEdges;
use crate::scheduler::thread_local_gc_work::ThreadStackWalker;
use crate::scheduler::GCWork;
use crate::scheduler::GCWorker;
use crate::Mutator;
use crate::MMTK;
use std::marker::PhantomData;

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

pub(super) struct CreateProcessRemsetWork<VM, P>
where
    VM: VMBinding,
    P: crate::Plan<VM = VM> + crate::plan::PlanTraceObject<VM>,
{
    _p: PhantomData<(VM, P)>,
}

impl<VM, P> CreateProcessRemsetWork<VM, P>
where
    VM: VMBinding,
    P: crate::Plan<VM = VM> + crate::plan::PlanTraceObject<VM>,
{
    pub fn new() -> Self {
        Self { _p: PhantomData }
    }
}

unsafe impl<VM, P> Send for CreateProcessRemsetWork<VM, P>
where
    VM: VMBinding,
    P: crate::Plan<VM = VM> + crate::plan::PlanTraceObject<VM>,
{
}

impl<VM, P> crate::scheduler::GCWork<VM> for CreateProcessRemsetWork<VM, P>
where
    VM: VMBinding,
    P: crate::Plan<VM = VM> + crate::plan::PlanTraceObject<VM>,
{
    fn do_work(
        &mut self,
        worker: &mut crate::scheduler::GCWorker<VM>,
        _mmtk: &'static crate::MMTK<VM>,
    ) {
        use crate::plan::immix::concurrent_gc_work::ProcessSlotRemset;
        use crate::scheduler::WorkBucketStage;
        use crate::vm::ActivePlan;

        for mutator in <VM as VMBinding>::VMActivePlan::mutators() {
            worker.scheduler().work_buckets[WorkBucketStage::Closure].add(
                ProcessSlotRemset::<VM, P>::new(
                    mutator.slot_remset.iter().unique().copied().collect_vec(),
                    #[cfg(debug_assertions)]
                    mutator.mutator_id,
                    _mmtk,
                ),
            );

            // stack slots do not carry over
            worker.scheduler().work_buckets[WorkBucketStage::Closure].add(
                ProcessSlotRemset::<VM, P>::new(
                    mutator.stack_slots.drain(..).collect_vec(),
                    #[cfg(debug_assertions)]
                    mutator.mutator_id,
                    _mmtk,
                ),
            );

            #[cfg(debug_assertions)]
            {
                use crate::util::metadata::public_bit::is_public;
                debug_assert!(mutator
                    .object_remset
                    .iter()
                    .all(|o| crate::memory_manager::is_pinned(*o) && is_public(*o)));
            }

            worker.scheduler().work_buckets[WorkBucketStage::Closure].add(ProcessObjectRemset::<
                VM,
                P,
            >::new(
                mutator.object_remset.iter().copied().collect_vec(),
                #[cfg(debug_assertions)]
                mutator.mutator_id,
                _mmtk,
            ));
        }
    }
}

pub struct CollectMutatorRoots<VM>
where
    VM: VMBinding,
{
    pub mutator: &'static mut Mutator<VM>,
}

unsafe impl<VM> Send for CollectMutatorRoots<VM> where VM: VMBinding {}

impl<VM> CollectMutatorRoots<VM>
where
    VM: VMBinding,
{
    pub fn new(mutator: &'static mut Mutator<VM>) -> Self {
        Self { mutator }
    }
}

impl<VM> GCWork<VM> for CollectMutatorRoots<VM>
where
    VM: VMBinding,
{
    // This work packet should be executed before `CreateProcessRemsetWork`
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        use crate::vm::Collection;

        trace!(
            "CollectMutatorRoots for mutator {:?}",
            self.mutator.mutator_tls
        );

        let object_graph_traversal = ThreadStackWalker::<VM>::new(self.mutator.mutator_tls);
        <VM as VMBinding>::VMCollection::scan_mutator(
            self.mutator.mutator_tls,
            object_graph_traversal,
        );
    }
}
