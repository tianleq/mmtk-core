use super::gc_work::EdgeOf;
use super::gc_work::ProcessEdgesBase;
use super::gc_work::ScanObjectsWork;
use super::work_bucket::WorkBucketStage;
use super::*;
use crate::plan::GcStatus;
use crate::plan::PlanTraceObject;
use crate::plan::VectorObjectQueue;
use crate::policy::gc_work::TraceKind;
use crate::scheduler::gc_work::PrepareCollector;
use crate::scheduler::gc_work::PrepareMutator;
use crate::scheduler::gc_work::ReleaseCollector;
use crate::scheduler::gc_work::ReleaseMutator;
use crate::util::*;
use crate::vm::edge_shape::Edge;
use crate::vm::*;
use crate::*;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};

pub struct ScheduleSingleThreadCollection;

impl<VM: VMBinding> GCWork<VM> for ScheduleSingleThreadCollection {
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        mmtk.plan
            .schedule_single_thread_collection(worker.scheduler());

        // Tell GC trigger that GC started.
        // We now know what kind of GC this is (e.g. nursery vs mature in gen copy, defrag vs fast in Immix)
        // TODO: Depending on the OS scheduling, other workers can run so fast that they can finish
        // everything in the `Unconstrained` and the `Prepare` buckets before we execute the next
        // statement. Consider if there is a better place to call `on_gc_start`.
        mmtk.plan.base().gc_trigger.policy.on_gc_start(mmtk);
    }
}

/// The global GC Preparation Work
/// This work packet invokes prepare() for the plan (which will invoke prepare() for each space), and
/// pushes work packets for preparing mutators and collectors.
/// We should only have one such work packet per GC, before any actual GC work starts.
/// We assume this work packet is the only running work packet that accesses plan, and there should
/// be no other concurrent work packet that accesses plan (read or write). Otherwise, there may
/// be a race condition.
pub struct SingleThreadPrepare<C: GCWorkContext> {
    pub plan: &'static C::PlanType,
}

impl<C: GCWorkContext> SingleThreadPrepare<C> {
    pub fn new(plan: &'static C::PlanType) -> Self {
        Self { plan }
    }
}

impl<C: GCWorkContext + 'static> GCWork<C::VM> for SingleThreadPrepare<C> {
    fn do_work(&mut self, worker: &mut GCWorker<C::VM>, mmtk: &'static MMTK<C::VM>) {
        trace!("SingleThreadPrepare Global");
        // We assume this is the only running work packet that accesses plan at the point of execution
        #[allow(clippy::cast_ref_to_mut)]
        let plan_mut: &mut C::PlanType = unsafe { &mut *(self.plan as *const _ as *mut _) };
        plan_mut.prepare(worker.tls);

        for mutator in <C::VM as VMBinding>::VMActivePlan::mutators() {
            mmtk.scheduler.work_buckets[WorkBucketStage::Prepare]
                .add_local(PrepareMutator::<C::VM>::new(mutator));
        }
        for w in &mmtk.scheduler.worker_group.workers_shared {
            let result = w.designated_work.push(Box::new(PrepareCollector));
            debug_assert!(result.is_ok());
        }
    }
}

struct SingleThreadProcessEdgesWorkRootsWorkFactory<E: ProcessEdgesWork> {
    mmtk: &'static MMTK<E::VM>,
}

impl<E: ProcessEdgesWork> Clone for SingleThreadProcessEdgesWorkRootsWorkFactory<E> {
    fn clone(&self) -> Self {
        Self { mmtk: self.mmtk }
    }
}

impl<E: ProcessEdgesWork> RootsWorkFactory<EdgeOf<E>>
    for SingleThreadProcessEdgesWorkRootsWorkFactory<E>
{
    fn create_process_edge_roots_work(&mut self, edges: Vec<EdgeOf<E>>) {
        crate::memory_manager::add_local_work_packet(
            self.mmtk,
            WorkBucketStage::Closure,
            E::new(edges, true, self.mmtk),
        );
    }

    fn create_process_node_roots_work(&mut self, nodes: Vec<ObjectReference>) {
        // We want to use E::create_scan_work.
        let process_edges_work = E::new(vec![], true, self.mmtk);
        let work = process_edges_work.create_scan_work(nodes, true);
        crate::memory_manager::add_local_work_packet(self.mmtk, WorkBucketStage::Closure, work);
    }
}

impl<E: ProcessEdgesWork> SingleThreadProcessEdgesWorkRootsWorkFactory<E> {
    fn new(mmtk: &'static MMTK<E::VM>) -> Self {
        Self { mmtk }
    }
}

pub struct SingleThreadScanStackRoot<Edges: ProcessEdgesWork>(pub &'static mut Mutator<Edges::VM>);

impl<E: ProcessEdgesWork> GCWork<E::VM> for SingleThreadScanStackRoot<E> {
    fn do_work(&mut self, worker: &mut GCWorker<E::VM>, mmtk: &'static MMTK<E::VM>) {
        trace!(
            "SingleThreadScanStackRoot for mutator {:?}",
            self.0.get_tls()
        );
        let base = &mmtk.plan.base();
        let mutators = <E::VM as VMBinding>::VMActivePlan::number_of_mutators();
        let factory = SingleThreadProcessEdgesWorkRootsWorkFactory::<E>::new(mmtk);
        <E::VM as VMBinding>::VMScanning::scan_thread_root(
            worker.tls,
            unsafe { &mut *(self.0 as *mut _) },
            factory,
        );
        self.0.flush();

        if mmtk.plan.base().inform_stack_scanned(mutators) {
            <E::VM as VMBinding>::VMScanning::notify_initial_thread_scan_complete(
                false, worker.tls,
            );
            base.set_gc_status(GcStatus::GcProper);
        }
    }
}

#[derive(Default)]
pub struct SingleThreadScanStackRoots<Edges: ProcessEdgesWork>(PhantomData<Edges>);

impl<E: ProcessEdgesWork> SingleThreadScanStackRoots<E> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<E: ProcessEdgesWork> GCWork<E::VM> for SingleThreadScanStackRoots<E> {
    fn do_work(&mut self, worker: &mut GCWorker<E::VM>, mmtk: &'static MMTK<E::VM>) {
        trace!("SingleThreadScanStackRoots");
        let factory = SingleThreadProcessEdgesWorkRootsWorkFactory::<E>::new(mmtk);
        <E::VM as VMBinding>::VMScanning::scan_thread_roots(worker.tls, factory);
        <E::VM as VMBinding>::VMScanning::notify_initial_thread_scan_complete(false, worker.tls);
        for mutator in <E::VM as VMBinding>::VMActivePlan::mutators() {
            mutator.flush();
        }
        mmtk.plan.common().base.set_gc_status(GcStatus::GcProper);
    }
}

#[derive(Default)]
pub struct SingleThreadScanVMSpecificRoots<Edges: ProcessEdgesWork>(PhantomData<Edges>);

impl<E: ProcessEdgesWork> SingleThreadScanVMSpecificRoots<E> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<E: ProcessEdgesWork> GCWork<E::VM> for SingleThreadScanVMSpecificRoots<E> {
    fn do_work(&mut self, worker: &mut GCWorker<E::VM>, mmtk: &'static MMTK<E::VM>) {
        trace!("ScanStaticRoots");
        let factory = SingleThreadProcessEdgesWorkRootsWorkFactory::<E>::new(mmtk);
        <E::VM as VMBinding>::VMScanning::single_thread_scan_vm_specific_roots(worker.tls, factory);
    }
}

/// Stop all mutators
///
/// Schedule a `ScanStackRoots` immediately after a mutator is paused
///
/// TODO: Smaller work granularity
#[derive(Default)]
pub struct SingleThreadStopMutators<ScanEdges: ProcessEdgesWork>(PhantomData<ScanEdges>);

impl<ScanEdges: ProcessEdgesWork> SingleThreadStopMutators<ScanEdges> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<E: ProcessEdgesWork> GCWork<E::VM> for SingleThreadStopMutators<E> {
    fn do_work(&mut self, worker: &mut GCWorker<E::VM>, mmtk: &'static MMTK<E::VM>) {
        trace!("single thread stop_all_mutators start");
        mmtk.plan.base().prepare_for_stack_scanning();
        <E::VM as VMBinding>::VMCollection::stop_all_mutators(worker.tls, |mutator| {
            mmtk.scheduler.work_buckets[WorkBucketStage::Prepare]
                .add_local(SingleThreadScanStackRoot::<E>(mutator));
        });
        trace!("single thread stop_all_mutators end");
        mmtk.scheduler.notify_mutators_paused(mmtk);
        if <E::VM as VMBinding>::VMScanning::SCAN_MUTATORS_IN_SAFEPOINT {
            // Prepare mutators if necessary
            // FIXME: This test is probably redundant. JikesRVM requires to call `prepare_mutator` once after mutators are paused
            if !mmtk.plan.base().stacks_prepared() {
                for mutator in <E::VM as VMBinding>::VMActivePlan::mutators() {
                    <E::VM as VMBinding>::VMCollection::prepare_mutator(
                        worker.tls,
                        mutator.get_tls(),
                        mutator,
                    );
                }
            }
            // Scan mutators
            if <E::VM as VMBinding>::VMScanning::SINGLE_THREAD_MUTATOR_SCANNING {
                mmtk.scheduler.work_buckets[WorkBucketStage::Prepare]
                    .add_local(SingleThreadScanStackRoots::<E>::new());
            } else {
                for mutator in <E::VM as VMBinding>::VMActivePlan::mutators() {
                    mmtk.scheduler.work_buckets[WorkBucketStage::Prepare]
                        .add_local(SingleThreadScanStackRoot::<E>(mutator));
                }
            }
        }
        mmtk.scheduler.work_buckets[WorkBucketStage::Prepare]
            .add_local(SingleThreadScanVMSpecificRoots::<E>::new());
    }
}

/// The global GC release Work
/// This work packet invokes release() for the plan (which will invoke release() for each space), and
/// pushes work packets for releasing mutators and collectors.
/// We should only have one such work packet per GC, after all actual GC work ends.
/// We assume this work packet is the only running work packet that accesses plan, and there should
/// be no other concurrent work packet that accesses plan (read or write). Otherwise, there may
/// be a race condition.
pub struct SingleThreadRelease<C: GCWorkContext> {
    pub plan: &'static C::PlanType,
}

impl<C: GCWorkContext> SingleThreadRelease<C> {
    pub fn new(plan: &'static C::PlanType) -> Self {
        Self { plan }
    }
}

impl<C: GCWorkContext + 'static> GCWork<C::VM> for SingleThreadRelease<C> {
    fn do_work(&mut self, worker: &mut GCWorker<C::VM>, mmtk: &'static MMTK<C::VM>) {
        trace!("Release Global");

        self.plan.base().gc_trigger.policy.on_gc_release(mmtk);

        // We assume this is the only running work packet that accesses plan at the point of execution
        #[allow(clippy::cast_ref_to_mut)]
        let plan_mut: &mut C::PlanType = unsafe { &mut *(self.plan as *const _ as *mut _) };
        plan_mut.release(worker.tls);

        for mutator in <C::VM as VMBinding>::VMActivePlan::mutators() {
            mmtk.scheduler.work_buckets[WorkBucketStage::Release]
                .add_local(ReleaseMutator::<C::VM>::new(mutator));
        }
        for w in &mmtk.scheduler.worker_group.workers_shared {
            let result = w.designated_work.push(Box::new(ReleaseCollector));
            debug_assert!(result.is_ok());
        }
    }
}

/// This provides an implementation of [`crate::scheduler::gc_work::ProcessEdgesWork`]. A plan that implements
/// `PlanTraceObject` can use this work packet for tracing objects.
pub struct SingleThreadPlanProcessEdges<
    VM: VMBinding,
    P: Plan<VM = VM> + PlanTraceObject<VM>,
    const KIND: TraceKind,
> {
    plan: &'static P,
    base: ProcessEdgesBase<VM>,
}

impl<VM: VMBinding, P: PlanTraceObject<VM> + Plan<VM = VM>, const KIND: TraceKind> ProcessEdgesWork
    for SingleThreadPlanProcessEdges<VM, P, KIND>
{
    type VM = VM;
    type ScanObjectsWorkType = SingleThreadPlanScanObjects<Self, P>;

    fn new(edges: Vec<EdgeOf<Self>>, roots: bool, mmtk: &'static MMTK<VM>) -> Self {
        let base = ProcessEdgesBase::new(edges, roots, mmtk);
        let plan = base.plan().downcast_ref::<P>().unwrap();
        Self { plan, base }
    }

    fn create_scan_work(
        &self,
        nodes: Vec<ObjectReference>,
        roots: bool,
    ) -> Self::ScanObjectsWorkType {
        SingleThreadPlanScanObjects::<Self, P>::new(self.plan, nodes, false, roots)
    }

    fn trace_object(&mut self, object: ObjectReference) -> ObjectReference {
        if object.is_null() {
            return object;
        }
        // We cannot borrow `self` twice in a call, so we extract `worker` as a local variable.
        let worker = self.worker();
        self.plan
            .trace_object::<VectorObjectQueue, KIND>(&mut self.base.nodes, object, worker)
    }

    fn process_edge(&mut self, slot: EdgeOf<Self>) {
        let object = slot.load();
        let new_object = self.trace_object(object);
        if P::may_move_objects::<KIND>() {
            slot.store(new_object);
        }
    }
}

// Impl Deref/DerefMut to ProcessEdgesBase for PlanProcessEdges
impl<VM: VMBinding, P: PlanTraceObject<VM> + Plan<VM = VM>, const KIND: TraceKind> Deref
    for SingleThreadPlanProcessEdges<VM, P, KIND>
{
    type Target = ProcessEdgesBase<VM>;
    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl<VM: VMBinding, P: PlanTraceObject<VM> + Plan<VM = VM>, const KIND: TraceKind> DerefMut
    for SingleThreadPlanProcessEdges<VM, P, KIND>
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}

/// This is an alternative to `ScanObjects` that calls the `post_scan_object` of the policy
/// selected by the plan.  It is applicable to plans that derive `PlanTraceObject`.
pub struct SingleThreadPlanScanObjects<
    E: ProcessEdgesWork,
    P: Plan<VM = E::VM> + PlanTraceObject<E::VM>,
> {
    plan: &'static P,
    buffer: Vec<ObjectReference>,
    #[allow(dead_code)]
    concurrent: bool,
    roots: bool,
    phantom: PhantomData<E>,
}

impl<E: ProcessEdgesWork, P: Plan<VM = E::VM> + PlanTraceObject<E::VM>>
    SingleThreadPlanScanObjects<E, P>
{
    pub fn new(
        plan: &'static P,
        buffer: Vec<ObjectReference>,
        concurrent: bool,
        roots: bool,
    ) -> Self {
        Self {
            plan,
            buffer,
            concurrent,
            roots,
            phantom: PhantomData,
        }
    }
}

impl<E: ProcessEdgesWork, P: Plan<VM = E::VM> + PlanTraceObject<E::VM>> ScanObjectsWork<E::VM>
    for SingleThreadPlanScanObjects<E, P>
{
    type E = E;

    fn roots(&self) -> bool {
        self.roots
    }

    fn post_scan_object(&self, object: ObjectReference) {
        self.plan.post_scan_object(object);
    }

    fn make_another(&self, buffer: Vec<ObjectReference>) -> Self {
        Self::new(self.plan, buffer, self.concurrent, false)
    }
}

impl<E: ProcessEdgesWork, P: Plan<VM = E::VM> + PlanTraceObject<E::VM>> GCWork<E::VM>
    for SingleThreadPlanScanObjects<E, P>
{
    fn do_work(&mut self, worker: &mut GCWorker<E::VM>, mmtk: &'static MMTK<E::VM>) {
        trace!("PlanScanObjects");
        self.do_work_common(&self.buffer, worker, mmtk, true);
        trace!("PlanScanObjects End");
    }
}
