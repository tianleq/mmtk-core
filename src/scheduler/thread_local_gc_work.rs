use super::gc_work::EdgeOf;
use super::gc_work::ProcessEdgesBase;
use super::gc_work::ScanObjectsWork;
use super::work_bucket::WorkBucketStage;
use super::*;
use crate::plan::GcStatus;
use crate::plan::PlanThreadlocalTraceObject;
use crate::plan::VectorObjectQueue;
use crate::policy::gc_work::TraceKind;
// use crate::scheduler::gc_work::PrepareCollector;
use crate::scheduler::gc_work::PrepareMutator;
// use crate::scheduler::gc_work::ReleaseCollector;
use crate::util::*;
use crate::vm::edge_shape::Edge;
use crate::vm::*;
use crate::*;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};

pub struct ScheduleThreadlocalCollection(pub VMMutatorThread, pub std::time::Instant);

impl<VM: VMBinding> GCWork<VM> for ScheduleThreadlocalCollection {
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        let mutator = VM::VMActivePlan::mutator(self.0);
        worker.gc_start = self.1;
        info!(
            "ScheduleThreadlocalCollection {:?} executed by GC Thread {}",
            mutator.mutator_id,
            crate::scheduler::worker::current_worker_ordinal().unwrap()
        );

        // Tell GC trigger that GC started.
        // We now know what kind of GC this is (e.g. nursery vs mature in gen copy, defrag vs fast in Immix)
        // TODO: Depending on the OS scheduling, other workers can run so fast that they can finish
        // everything in the `Unconstrained` and the `Prepare` buckets before we execute the next
        // statement. Consider if there is a better place to call `on_gc_start`.
        mmtk.plan.base().gc_trigger.policy.on_gc_start(mmtk);
        // When this gc thread is executing the local gc, it cannot steal work from others' bucket/queue
        worker.disable_steal();
        VM::VMCollection::publish_vm_specific_roots(mutator);
        mmtk.plan.schedule_thread_local_collection(self.0, worker);
    }
}

/// The thread-local GC Preparation Work
/// This work packet invokes prepare() for the plan (which will invoke prepare() for each space), and
/// pushes work packets for preparing mutators and collectors.
/// We should only have one such work packet per GC, before any actual GC work starts.
/// We assume this work packet is the only running work packet that accesses plan, and there should
/// be no other concurrent work packet that accesses plan (read or write). Otherwise, there may
/// be a race condition.
pub struct ThreadlocalPrepare<C: GCWorkContext> {
    pub plan: &'static C::PlanType,
    tls: VMMutatorThread,
}

impl<C: GCWorkContext> ThreadlocalPrepare<C> {
    pub fn new(plan: &'static C::PlanType, tls: VMMutatorThread) -> Self {
        Self { plan, tls }
    }
}

impl<C: GCWorkContext + 'static> GCWork<C::VM> for ThreadlocalPrepare<C> {
    fn do_work(&mut self, worker: &mut GCWorker<C::VM>, mmtk: &'static MMTK<C::VM>) {
        trace!("Prepare Global");
        let mutator = <C::VM as VMBinding>::VMActivePlan::mutator(self.tls);

        PrepareMutator::<C::VM>::new(mutator).do_work(worker, mmtk);

        // PrepareCollector.do_work(worker, mmtk);
        trace!("Prepare Collector");
        worker.get_copy_context_mut().thread_local_prepare();
        mmtk.plan.prepare_worker(worker);
    }
}

/// The thread local GC release Work
/// This work packet invokes release() for the plan (which will invoke release() for each space), and
/// pushes work packets for releasing mutators and collectors.
/// We should only have one such work packet per GC, after all actual GC work ends.
/// We assume this work packet is the only running work packet that accesses plan, and there should
/// be no other concurrent work packet that accesses plan (read or write). Otherwise, there may
/// be a race condition.
pub struct ThreadlocalRelease<C: GCWorkContext> {
    pub plan: &'static C::PlanType,
    tls: VMMutatorThread,
}

impl<C: GCWorkContext> ThreadlocalRelease<C> {
    pub fn new(plan: &'static C::PlanType, tls: VMMutatorThread) -> Self {
        Self { plan, tls }
    }
}

impl<C: GCWorkContext + 'static> GCWork<C::VM> for ThreadlocalRelease<C> {
    fn do_work(&mut self, worker: &mut GCWorker<C::VM>, _mmtk: &'static MMTK<C::VM>) {
        trace!("Thread local Release");
        let mutator = <C::VM as VMBinding>::VMActivePlan::mutator(self.tls);
        // self.plan.base().gc_trigger.policy.on_gc_release(mmtk);

        trace!("Release Mutator");
        mutator.release(worker.tls);
        // Mutators need to be aware of all memory allocated by the collector

        trace!("Release Collector");
        worker.get_copy_context_mut().thread_local_release(mutator);
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

        #[cfg(feature = "extreme_assertions")]
        if crate::util::edge_logger::should_check_duplicate_edges(&*mmtk.plan) {
            // reset the logging info at the end of each GC
            mmtk.edge_logger.reset();
        }

        // mmtk.plan.base().set_gc_status(GcStatus::NotInGC);

        // Reset the triggering information.
        mmtk.plan.base().reset_collection_trigger();
        worker.enable_steal();
        <VM as VMBinding>::VMCollection::resume_from_thread_local_gc(self.tls);
    }
}

/// Scan a specific mutator
///
/// Schedule a `ThreadlocalScanStackRoot`
///
#[derive(Default)]
pub struct ScanMutator<ScanEdges: ProcessEdgesWork> {
    tls: VMMutatorThread,
    phantom: PhantomData<ScanEdges>,
}

impl<ScanEdges: ProcessEdgesWork> ScanMutator<ScanEdges> {
    pub fn new(tls: VMMutatorThread) -> Self {
        Self {
            tls,
            phantom: PhantomData,
        }
    }
}

impl<E: ProcessEdgesWork> GCWork<E::VM> for ScanMutator<E> {
    fn do_work(&mut self, _worker: &mut GCWorker<E::VM>, mmtk: &'static MMTK<E::VM>) {
        trace!("scan_mutator start");
        mmtk.plan.base().prepare_for_stack_scanning();
        <E::VM as VMBinding>::VMCollection::scan_mutator(self.tls, |mutator| {
            mmtk.scheduler.work_buckets[WorkBucketStage::Unconstrained]
                .add_local(ThreadlocalScanStackRoot::<E>(mutator));
        });
        trace!("scan_mutator end");
        // mmtk.scheduler.notify_mutators_paused(mmtk);
    }
}

struct ThreadlocalProcessEdgesWorkRootsWorkFactory<E: ProcessEdgesWork> {
    mmtk: &'static MMTK<E::VM>,
    tls: VMMutatorThread,
}

impl<E: ProcessEdgesWork> Clone for ThreadlocalProcessEdgesWorkRootsWorkFactory<E> {
    fn clone(&self) -> Self {
        Self {
            mmtk: self.mmtk,
            tls: self.tls,
        }
    }
}

impl<E: ProcessEdgesWork> RootsWorkFactory<EdgeOf<E>>
    for ThreadlocalProcessEdgesWorkRootsWorkFactory<E>
{
    fn create_process_edge_roots_work(&mut self, edges: Vec<EdgeOf<E>>) {
        #[cfg(not(feature = "debug_publish_object"))]
        crate::memory_manager::add_local_work_packet(
            self.mmtk,
            WorkBucketStage::Unconstrained,
            E::new(edges, true, self.mmtk, Some(self.tls)),
        );
        #[cfg(feature = "debug_publish_object")]
        crate::memory_manager::add_local_work_packet(
            self.mmtk,
            WorkBucketStage::Unconstrained,
            E::new(
                edges.iter().map(|&edge| edge.load()).collect(),
                edges,
                true,
                self.mmtk,
                Some(self.tls),
            ),
        );
    }

    fn create_process_node_roots_work(&mut self, nodes: Vec<ObjectReference>) {
        // We want to use E::create_scan_work.
        #[cfg(not(feature = "debug_publish_object"))]
        let process_edges_work = E::new(vec![], true, self.mmtk, Some(self.tls));
        #[cfg(feature = "debug_publish_object")]
        let process_edges_work = E::new(vec![], vec![], true, self.mmtk, Some(self.tls));
        let work = process_edges_work.create_scan_work(nodes, true);
        crate::memory_manager::add_local_work_packet(
            self.mmtk,
            WorkBucketStage::Unconstrained,
            work,
        );
    }
}

impl<E: ProcessEdgesWork> ThreadlocalProcessEdgesWorkRootsWorkFactory<E> {
    fn new(mmtk: &'static MMTK<E::VM>, tls: VMMutatorThread) -> Self {
        Self { mmtk, tls }
    }
}

pub struct ThreadlocalScanStackRoot<Edges: ProcessEdgesWork>(pub &'static mut Mutator<Edges::VM>);

impl<E: ProcessEdgesWork> GCWork<E::VM> for ThreadlocalScanStackRoot<E> {
    fn do_work(&mut self, worker: &mut GCWorker<E::VM>, mmtk: &'static MMTK<E::VM>) {
        trace!(
            "ThreadlocalScanStackRoot for mutator {:?}",
            self.0.get_tls()
        );

        info!(
            "ThreadlocalScanStackRoot executed by GC Thread {}",
            crate::scheduler::worker::current_worker_ordinal().unwrap()
        );

        // let base = &mmtk.plan.base();
        let factory =
            ThreadlocalProcessEdgesWorkRootsWorkFactory::<E>::new(mmtk, self.0.mutator_tls);
        <E::VM as VMBinding>::VMScanning::thread_local_scan_roots_of_mutator_threads(
            worker.tls,
            unsafe { &mut *(self.0 as *mut _) },
            factory,
        );
        self.0.flush();

        // base.set_gc_status(GcStatus::GcProper);
    }
}

pub struct ThreadlocalSentinel<C: GCWorkContext> {
    pub plan: &'static C::PlanType,
    tls: VMMutatorThread,
}

impl<C: GCWorkContext> ThreadlocalSentinel<C> {
    pub fn new(plan: &'static C::PlanType, tls: VMMutatorThread) -> Self {
        Self { plan, tls }
    }
}

impl<C: GCWorkContext + 'static> GCWork<C::VM> for ThreadlocalSentinel<C> {
    fn do_work(&mut self, worker: &mut GCWorker<C::VM>, mmtk: &'static MMTK<C::VM>) {
        trace!("Thread local Sentinel");
        #[cfg(debug_assertions)]
        info!(
            "ThreadlocalSentinel executed by GC Thread {}",
            crate::scheduler::worker::current_worker_ordinal().unwrap()
        );

        // TODO support weak reference and finalization properly

        // Finalization and then do release
        if !*self.plan.base().options.no_finalizer {
            use crate::util::finalizable_processor::{Finalization, ForwardFinalization};
            // finalization
            Finalization::<C::ThreadlocalProcessEdgesWorkType>::new(Option::Some(self.tls))
                .do_work(worker, mmtk);
            // forward refs
            if self.plan.constraints().needs_forward_after_liveness {
                ForwardFinalization::<C::ThreadlocalProcessEdgesWorkType>::new(Option::Some(
                    self.tls,
                ))
                .do_work(worker, mmtk);
            }
        }
        ThreadlocalRelease::<C>::new(self.plan, self.tls).do_work(worker, mmtk);
        let mut end_of_thread_local_gc = EndOfThreadLocalGC {
            elapsed: worker.gc_start.elapsed(),
            tls: self.tls,
        };

        end_of_thread_local_gc.do_work_with_stat(worker, mmtk);
    }
}

/// This provides an implementation of [`crate::scheduler::gc_work::ProcessEdgesWork`]. A plan that implements
/// `PlanTraceObject` can use this work packet for tracing objects.
pub struct PlanThreadlocalProcessEdges<
    VM: VMBinding,
    P: Plan<VM = VM> + PlanThreadlocalTraceObject<VM>,
    const KIND: TraceKind,
> {
    plan: &'static P,
    base: ProcessEdgesBase<VM>,
    tls: VMMutatorThread,
    mutator_id: u32,
}

impl<VM: VMBinding, P: PlanThreadlocalTraceObject<VM> + Plan<VM = VM>, const KIND: TraceKind>
    ProcessEdgesWork for PlanThreadlocalProcessEdges<VM, P, KIND>
{
    type VM = VM;
    type ScanObjectsWorkType = PlanThreadlocalScanObjects<Self, P>;

    #[cfg(not(feature = "debug_publish_object"))]
    fn new(
        edges: Vec<EdgeOf<Self>>,
        roots: bool,
        mmtk: &'static MMTK<VM>,
        tls: Option<VMMutatorThread>,
    ) -> Self {
        let base = ProcessEdgesBase::new(edges, roots, mmtk);
        let plan = base.plan().downcast_ref::<P>().unwrap();
        Self {
            plan,
            base,
            tls: tls.unwrap(),
            mutator_id: 0,
        }
    }
    #[cfg(feature = "debug_publish_object")]
    fn new(
        sources: Vec<ObjectReference>,
        edges: Vec<EdgeOf<Self>>,
        roots: bool,
        mmtk: &'static MMTK<VM>,
        tls: Option<VMMutatorThread>,
    ) -> Self {
        let base = ProcessEdgesBase::new(sources, edges, roots, mmtk);
        let plan = base.plan().downcast_ref::<P>().unwrap();
        let mutator = VM::VMActivePlan::mutator(tls.unwrap());
        Self {
            plan,
            base,
            tls: tls.unwrap(),
            mutator_id: mutator.mutator_id,
        }
    }

    /// Start the a scan work packet. If SCAN_OBJECTS_IMMEDIATELY, the work packet will be executed immediately, in this method.
    /// Otherwise, the work packet will be added the Closure work bucket and will be dispatched later by the scheduler.
    fn start_or_dispatch_scan_work(&mut self, work_packet: impl GCWork<Self::VM>) {
        if Self::SCAN_OBJECTS_IMMEDIATELY {
            // We execute this `scan_objects_work` immediately.
            // This is expected to be a useful optimization because,
            // say for _pmd_ with 200M heap, we're likely to have 50000~60000 `ScanObjects` work packets
            // being dispatched (similar amount to `ProcessEdgesWork`).
            // Executing these work packets now can remarkably reduce the global synchronization time.
            self.worker().do_work(work_packet);
        } else {
            self.mmtk().scheduler.work_buckets[WorkBucketStage::Unconstrained]
                .add_local(work_packet);
        }
    }

    fn create_scan_work(
        &self,
        nodes: Vec<ObjectReference>,
        roots: bool,
    ) -> Self::ScanObjectsWorkType {
        PlanThreadlocalScanObjects::<Self, P>::new(self.plan, nodes, false, roots, self.tls)
    }

    fn trace_object(&mut self, object: ObjectReference) -> ObjectReference {
        if object.is_null() {
            return object;
        }
        // We cannot borrow `self` twice in a call, so we extract `worker` as a local variable.
        let worker = self.worker();
        // let mutator = VM::VMActivePlan::mutator(self.tls);
        let mutator_id = self.mutator_id;
        self.plan
            .thread_local_trace_object::<VectorObjectQueue, KIND>(
                &mut self.base.nodes,
                object,
                mutator_id,
                worker,
            )
    }

    fn process_edges(&mut self) {
        for i in 0..self.edges.len() {
            #[cfg(not(feature = "debug_publish_object"))]
            self.process_edge(self.edges[i]);
            #[cfg(feature = "debug_publish_object")]
            self._process_edge(self.sources[i], self.edges[i]);
        }
    }

    fn process_edge(&mut self, slot: EdgeOf<Self>) {
        let object = slot.load();
        let new_object = self.trace_object(object);
        if P::thread_local_may_move_objects::<KIND>() {
            slot.store(new_object);
        }
    }
}

impl<VM: VMBinding, P: PlanThreadlocalTraceObject<VM> + Plan<VM = VM>, const KIND: TraceKind>
    PlanThreadlocalProcessEdges<VM, P, KIND>
{
    fn _process_edge(&mut self, _source: ObjectReference, slot: EdgeOf<Self>) {
        let object = slot.load();
        let new_object = self.trace_object(object);
        #[cfg(feature = "extra_header")]
        {
            if !object.is_null() && !crate::util::public_bit::is_public::<VM>(object) {
                let m1 = crate::util::object_extra_header_metadata::get_extra_header_metadata::<
                    VM,
                    usize,
                >(object);
                let m2 = crate::util::object_extra_header_metadata::get_extra_header_metadata::<
                    VM,
                    usize,
                >(_source);
                debug_assert!(
                    (m1 & object_extra_header_metadata::BOTTOM_HALF_MASK)
                        == (m2 & object_extra_header_metadata::BOTTOM_HALF_MASK),
                    "object is not published properly."
                )
            }
        }
        if P::thread_local_may_move_objects::<KIND>() {
            slot.store(new_object);
        }
    }
}

// Impl Deref/DerefMut to ProcessEdgesBase for PlanProcessEdges
impl<VM: VMBinding, P: PlanThreadlocalTraceObject<VM> + Plan<VM = VM>, const KIND: TraceKind> Deref
    for PlanThreadlocalProcessEdges<VM, P, KIND>
{
    type Target = ProcessEdgesBase<VM>;
    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl<VM: VMBinding, P: PlanThreadlocalTraceObject<VM> + Plan<VM = VM>, const KIND: TraceKind>
    DerefMut for PlanThreadlocalProcessEdges<VM, P, KIND>
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}

/// This is an alternative to `ScanObjects` that calls the `post_scan_object` of the policy
/// selected by the plan.  It is applicable to plans that derive `PlanTraceObject`.
pub struct PlanThreadlocalScanObjects<
    E: ProcessEdgesWork,
    P: Plan<VM = E::VM> + PlanThreadlocalTraceObject<E::VM>,
> {
    plan: &'static P,
    buffer: Vec<ObjectReference>,
    #[allow(dead_code)]
    concurrent: bool,
    roots: bool,
    phantom: PhantomData<E>,
    tls: VMMutatorThread,
}

impl<E: ProcessEdgesWork, P: Plan<VM = E::VM> + PlanThreadlocalTraceObject<E::VM>>
    PlanThreadlocalScanObjects<E, P>
{
    pub fn new(
        plan: &'static P,
        buffer: Vec<ObjectReference>,
        concurrent: bool,
        roots: bool,
        tls: VMMutatorThread,
    ) -> Self {
        Self {
            plan,
            buffer,
            concurrent,
            roots,
            phantom: PhantomData,
            tls,
        }
    }
}

impl<E: ProcessEdgesWork, P: Plan<VM = E::VM> + PlanThreadlocalTraceObject<E::VM>>
    ScanObjectsWork<E::VM> for PlanThreadlocalScanObjects<E, P>
{
    type E = E;

    fn roots(&self) -> bool {
        self.roots
    }

    fn post_scan_object(&self, object: ObjectReference) {
        let mutator = <E::VM as VMBinding>::VMActivePlan::mutator(self.tls);
        self.plan.thread_local_post_scan_object(mutator, object);
    }

    fn make_another(&self, buffer: Vec<ObjectReference>) -> Self {
        Self::new(self.plan, buffer, self.concurrent, false, self.tls)
    }
}

impl<E: ProcessEdgesWork, P: Plan<VM = E::VM> + PlanThreadlocalTraceObject<E::VM>> GCWork<E::VM>
    for PlanThreadlocalScanObjects<E, P>
{
    fn do_work(&mut self, worker: &mut GCWorker<E::VM>, mmtk: &'static MMTK<E::VM>) {
        trace!("PlanScanObjects");

        self.do_work_common(&self.buffer, worker, mmtk, true, Some(self.tls));
        trace!("PlanScanObjects End");
    }
}
