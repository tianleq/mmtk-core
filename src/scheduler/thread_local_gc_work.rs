use std::marker::PhantomData;

use crate::{
    plan::{GcStatus, PlanThreadlocalTraceObject, ThreadlocalObjectsClosure, VectorObjectQueue},
    policy::gc_work::TraceKind,
    scheduler::{
        gc_work::{PrepareCollector, PrepareMutator, ReleaseCollector, ReleaseMutator},
        WorkBucketStage,
    },
    util::{ObjectReference, VMMutatorThread},
    vm::edge_shape::Edge,
    vm::ActivePlan,
    vm::Collection,
    vm::VMBinding,
    Mutator, Plan, MMTK,
};

use super::{CoordinatorWork, GCWork, GCWorkContext, GCWorker};

/// The thread-local GC Preparation Work
/// This work packet invokes prepare() for the plan (which will invoke prepare() for each space), and
/// pushes work packets for preparing mutators and collectors.
/// We should only have one such work packet per GC, before any actual GC work starts.
/// We assume this work packet is the only running work packet that accesses plan, and there should
/// be no other concurrent work packet that accesses plan (read or write). Otherwise, there may
/// be a race condition.
pub struct ThreadLocalPrepare<C: GCWorkContext> {
    pub plan: &'static C::PlanType,
    tls: VMMutatorThread,
}

impl<C: GCWorkContext> ThreadLocalPrepare<C> {
    pub fn new(plan: &'static C::PlanType, tls: VMMutatorThread) -> Self {
        Self { plan, tls }
    }
}

impl<C: GCWorkContext + 'static> GCWork<C::VM> for ThreadLocalPrepare<C> {
    fn do_work(&mut self, _worker: &mut GCWorker<C::VM>, mmtk: &'static MMTK<C::VM>) {
        trace!("Prepare Global");
        // We assume this is the only running work packet that accesses plan at the point of execution
        #[allow(clippy::cast_ref_to_mut)]
        let plan_mut: &mut C::PlanType = unsafe { &mut *(self.plan as *const _ as *mut _) };
        plan_mut.thread_local_prepare(self.tls);

        let mutator = <C::VM as VMBinding>::VMActivePlan::mutator(self.tls);
        mmtk.scheduler.work_buckets[WorkBucketStage::Prepare]
            .add(PrepareMutator::<C::VM>::new(mutator));

        for w in &mmtk.scheduler.worker_group.workers_shared {
            let result = w.designated_work.push(Box::new(PrepareCollector));
            debug_assert!(result.is_ok());
        }
    }
}

/// The global GC release Work
/// This work packet invokes release() for the plan (which will invoke release() for each space), and
/// pushes work packets for releasing mutators and collectors.
/// We should only have one such work packet per GC, after all actual GC work ends.
/// We assume this work packet is the only running work packet that accesses plan, and there should
/// be no other concurrent work packet that accesses plan (read or write). Otherwise, there may
/// be a race condition.
pub struct ThreadLocalRelease<C: GCWorkContext> {
    pub plan: &'static C::PlanType,
    tls: VMMutatorThread,
}

impl<C: GCWorkContext> ThreadLocalRelease<C> {
    pub fn new(plan: &'static C::PlanType, tls: VMMutatorThread) -> Self {
        Self { plan, tls }
    }
}

impl<C: GCWorkContext + 'static> GCWork<C::VM> for ThreadLocalRelease<C> {
    fn do_work(&mut self, _worker: &mut GCWorker<C::VM>, mmtk: &'static MMTK<C::VM>) {
        trace!("Release Thread-loal");

        self.plan.base().gc_trigger.policy.on_gc_release(mmtk);

        // We assume this is the only running work packet that accesses plan at the point of execution
        #[allow(clippy::cast_ref_to_mut)]
        let plan_mut: &mut C::PlanType = unsafe { &mut *(self.plan as *const _ as *mut _) };
        plan_mut.thread_local_release(self.tls);

        let mutator = <C::VM as VMBinding>::VMActivePlan::mutator(self.tls);
        mmtk.scheduler.work_buckets[WorkBucketStage::Release]
            .add(ReleaseMutator::<C::VM>::new(mutator));

        for w in &mmtk.scheduler.worker_group.workers_shared {
            let result = w.designated_work.push(Box::new(ReleaseCollector));
            debug_assert!(result.is_ok());
        }
    }
}

#[derive(Default)]
pub struct EndOfThreadLocalGC {
    pub elapsed: std::time::Duration,
    pub tls: VMMutatorThread,
}

impl<VM: VMBinding> GCWork<VM> for EndOfThreadLocalGC {
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        info!(
            "End of Thread local GC ({}/{} pages, took {} ms)",
            mmtk.plan.get_reserved_pages(),
            mmtk.plan.get_total_pages(),
            self.elapsed.as_millis()
        );

        // We assume this is the only running work packet that accesses plan at the point of execution
        #[allow(clippy::cast_ref_to_mut)]
        let plan_mut: &mut dyn Plan<VM = VM> = unsafe { &mut *(&*mmtk.plan as *const _ as *mut _) };
        plan_mut.end_of_gc(worker.tls);

        #[cfg(feature = "extreme_assertions")]
        if crate::util::edge_logger::should_check_duplicate_edges(&*mmtk.plan) {
            // reset the logging info at the end of each GC
            mmtk.edge_logger.reset();
        }

        if <VM as VMBinding>::VMCollection::COORDINATOR_ONLY_STW {
            assert!(worker.is_coordinator(),
                    "VM only allows coordinator to resume mutators, but the current worker is not the coordinator.");
        }

        mmtk.plan.base().set_gc_status(GcStatus::NotInGC);

        // Reset the triggering information.
        mmtk.plan.base().reset_collection_trigger();

        <VM as VMBinding>::VMCollection::resume_from_thread_local_gc(self.tls);
        // <VM as VMBinding>::VMCollection::resume_mutators(worker.tls);
    }
}

impl<VM: VMBinding> CoordinatorWork<VM> for EndOfThreadLocalGC {}

/// Scan & update a list of object slots
pub trait ThreadlocalProcessEdges<VM: VMBinding>: 'static + Sized {
    fn process_edges(&mut self, edges: Vec<VM::VMEdge>);
}

pub struct PlanThreadlocalProcessEdges<
    VM: VMBinding,
    P: Plan<VM = VM> + PlanThreadlocalTraceObject<VM>,
    const KIND: TraceKind,
> {
    mutator: Mutator<VM>,
    plan: &'static P,
}

impl<VM: VMBinding, P: PlanThreadlocalTraceObject<VM> + Plan<VM = VM>, const KIND: TraceKind>
    ThreadlocalProcessEdges<VM> for PlanThreadlocalProcessEdges<VM, P, KIND>
{
    fn process_edges(&mut self, edges: Vec<VM::VMEdge>) {
        for i in 0..edges.len() {
            self.process_edge(edges[i])
        }
    }
}

impl<VM: VMBinding, P: PlanThreadlocalTraceObject<VM> + Plan<VM = VM>, const KIND: TraceKind>
    PlanThreadlocalProcessEdges<VM, P, KIND>
{
    fn new(mutator: Mutator<VM>, mmtk: &'static MMTK<VM>) -> Self {
        let plan = mmtk.plan.downcast_ref::<P>().unwrap();
        Self { plan, mutator }
    }

    fn process_edge(&mut self, slot: VM::VMEdge) {
        let mut closure: ThreadlocalObjectsClosure<VM, P, KIND> =
            ThreadlocalObjectsClosure::new(&mut self.mutator, self.plan, slot);
        closure.do_closure();
    }

    // fn trace_object(&mut self, object: ObjectReference) -> ObjectReference {
    //     if object.is_null() {
    //         return object;
    //     }
    //     VM::VMScanning::scan_object(
    //         VMWorkerThread(VMThread::UNINITIALIZED),
    //         object,
    //         &mut closure,
    //     );
    //     // We cannot borrow `self` twice in a call, so we extract `worker` as a local variable.
    //     self.plan
    //         .thread_local_trace_object::<VectorObjectQueue, KIND>(&mut self.base.nodes, object)
    // }
}
