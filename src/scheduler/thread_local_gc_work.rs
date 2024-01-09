// use super::gc_work::EdgeOf;
// use super::gc_work::ProcessEdgesBase;
// use super::gc_work::ScanObjectsWork;
// use super::work_bucket::WorkBucketStage;
use super::*;
use crate::plan::PlanThreadlocalTraceObject;
use crate::plan::ThreadlocalTracedObjectType::*;
use crate::policy::gc_work::TraceKind;
use crate::scheduler::gc_work::PrepareMutator;
use crate::util::*;
use crate::vm::edge_shape::Edge;
use crate::vm::*;
use crate::*;
use std::collections::VecDeque;
use std::marker::PhantomData;
// use std::ops::{Deref, DerefMut};

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
        mmtk.plan
            .base()
            .gc_trigger
            .policy
            .on_thread_local_gc_start(mmtk);
        // When this gc thread is executing the local gc, it cannot steal work from others' bucket/queue
        worker.disable_steal();

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
        trace!("Thread local Prepare Mutator");
        {
            let mutator = <C::VM as VMBinding>::VMActivePlan::mutator(self.tls);
            PrepareMutator::<C::VM>::new(mutator).do_work(worker, mmtk);
        }
        let mutator = <C::VM as VMBinding>::VMActivePlan::mutator(self.tls);
        // PrepareCollector.do_work(worker, mmtk);
        trace!("Thread local Prepare Collector");
        worker.get_copy_context_mut().thread_local_prepare(mutator);
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

        // Mutators need to be aware of all memory allocated by the collector
        // So collector must be released before mutator
        trace!("Thread local Release Collector");
        worker.get_copy_context_mut().thread_local_release(mutator);

        trace!("Thread local Release Mutator");
        mutator.release(worker.tls);
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

        mmtk.plan
            .base()
            .gc_trigger
            .policy
            .on_thread_local_gc_end(mmtk);

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
// #[derive(Default)]
// pub struct ScanMutator<ScanEdges: ProcessEdgesWork> {
//     tls: VMMutatorThread,
//     phantom: PhantomData<ScanEdges>,
// }

// impl<ScanEdges: ProcessEdgesWork> ScanMutator<ScanEdges> {
//     pub fn new(tls: VMMutatorThread) -> Self {
//         Self {
//             tls,
//             phantom: PhantomData,
//         }
//     }
// }

// impl<E: ProcessEdgesWork> GCWork<E::VM> for ScanMutator<E> {
//     fn do_work(&mut self, _worker: &mut GCWorker<E::VM>, mmtk: &'static MMTK<E::VM>) {
//         trace!("scan_mutator start");
//         mmtk.plan.base().prepare_for_stack_scanning();
//         <E::VM as VMBinding>::VMCollection::scan_mutator(self.tls, |mutator| {
//             mmtk.scheduler.work_buckets[WorkBucketStage::Unconstrained]
//                 .add_local(ThreadlocalScanStackRoot::<E>(mutator));
//         });
//         trace!("scan_mutator end");
//         // mmtk.scheduler.notify_mutators_paused(mmtk);
//     }
// }

/// Scan a specific mutator
#[derive(Default)]
pub struct ScanMutator<VM, Closure>
where
    VM: VMBinding,
    Closure: ThreadlocalObjectGraphTraversalClosure<VM>,
{
    tls: VMMutatorThread,
    phantom: PhantomData<(VM, Closure)>,
}

impl<VM, Closure> ScanMutator<VM, Closure>
where
    VM: VMBinding,
    Closure: ThreadlocalObjectGraphTraversalClosure<VM>,
{
    pub fn new(tls: VMMutatorThread) -> Self {
        Self {
            tls,
            phantom: PhantomData,
        }
    }

    pub fn scan_mutator(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        trace!("scan_mutator start");
        mmtk.plan.base().prepare_for_stack_scanning();
        let object_graph_traversal =
            ThreadlocalObjectGraphTraversal::<'_, VM, Closure>::new(mmtk, self.tls, worker);
        VM::VMCollection::scan_mutator(self.tls, object_graph_traversal);
        trace!("scan_mutator end");
    }
}

pub struct ThreadlocalObjectGraphTraversal<'a, VM, Closure>
where
    VM: VMBinding,
    Closure: ThreadlocalObjectGraphTraversalClosure<VM>,
{
    mmtk: &'static MMTK<VM>,
    tls: VMMutatorThread,
    // Use raw pointer for fast pointer dereferencing, instead of using `Option<&'static mut GCWorker<E::VM>>`.
    // Because a copying gc will dereference this pointer at least once for every object copy.
    worker: &'a mut GCWorker<VM>,
    phantom: PhantomData<Closure>,
}

impl<'a, VM, Closure> ObjectGraphTraversal<VM::VMEdge>
    for ThreadlocalObjectGraphTraversal<'a, VM, Closure>
where
    VM: VMBinding,
    Closure: ThreadlocalObjectGraphTraversalClosure<VM>,
{
    fn traverse_from_roots(&mut self, root_slots: Vec<VM::VMEdge>) {
        Closure::new(self.mmtk, self.tls, self.worker, Some(root_slots)).do_closure();
    }
}

impl<'a, VM, Closure> ThreadlocalObjectGraphTraversal<'a, VM, Closure>
where
    VM: VMBinding,
    Closure: ThreadlocalObjectGraphTraversalClosure<VM>,
{
    pub fn new(
        mmtk: &'static MMTK<VM>,
        tls: VMMutatorThread,
        worker: &'a mut GCWorker<VM>,
    ) -> Self {
        Self {
            mmtk,
            tls,
            worker,
            phantom: PhantomData,
        }
    }
}

// struct ThreadlocalProcessEdgesWorkRootsWorkFactory<E: ProcessEdgesWork> {
//     mmtk: &'static MMTK<E::VM>,
//     tls: VMMutatorThread,
//     process_edges_work_counter: Arc<AtomicUsize>,
// }

// impl<E: ProcessEdgesWork> Clone for ThreadlocalProcessEdgesWorkRootsWorkFactory<E> {
//     fn clone(&self) -> Self {
//         Self {
//             mmtk: self.mmtk,
//             tls: self.tls,
//             process_edges_work_counter: self.process_edges_work_counter.clone(),
//         }
//     }
// }

// impl<E: ProcessEdgesWork> RootsWorkFactory<EdgeOf<E>>
//     for ThreadlocalProcessEdgesWorkRootsWorkFactory<E>
// {
//     #[cfg(not(feature = "debug_publish_object"))]
//     fn create_process_edge_roots_work(&mut self, edges: Vec<EdgeOf<E>>) {
//         crate::memory_manager::add_local_work_packet(
//             self.mmtk,
//             WorkBucketStage::Unconstrained,
//             E::new(
//                 edges,
//                 true,
//                 self.mmtk,
//                 Some(self.tls),
//                 Some(self.process_edges_work_counter.clone()),
//             ),
//         );
//     }

//     #[cfg(feature = "debug_publish_object")]
//     fn create_process_edge_roots_work(&mut self, vm_roots_type: u8, edges: Vec<EdgeOf<E>>) {
//         self.process_edges_work_counter
//             .fetch_add(1, Ordering::SeqCst);
//         #[cfg(debug_assertions)]
//         {
//             let mutator = <E::VM as VMBinding>::VMActivePlan::mutator(self.tls);
//             let mut debug_info = LOCAL_TRANSITIVE_CLOSURE_WORK_DEBUG_INFO.lock().unwrap();
//             if let Some(val) = debug_info.get_mut(&mutator.mutator_id) {
//                 *val += 1;
//             } else {
//                 debug_info.insert(mutator.mutator_id, 1);
//             }
//         }
//         crate::memory_manager::add_local_work_packet(
//             self.mmtk,
//             WorkBucketStage::Unconstrained,
//             E::new(
//                 edges.iter().map(|&edge| edge.load()).collect(),
//                 edges,
//                 true,
//                 vm_roots_type,
//                 self.mmtk,
//                 Some(self.tls),
//                 Some(self.process_edges_work_counter.clone()),
//             ),
//         );
//     }

//     fn create_process_node_roots_work(&mut self, _nodes: Vec<ObjectReference>) {
//         panic!("thread local gc does not support node enqueuing");
//     }
// }

// impl<E: ProcessEdgesWork> ThreadlocalProcessEdgesWorkRootsWorkFactory<E> {
//     fn new(mmtk: &'static MMTK<E::VM>, tls: VMMutatorThread) -> Self {
//         Self {
//             mmtk,
//             tls,
//             process_edges_work_counter: Arc::new(AtomicUsize::new(0)),
//         }
//     }
// }

// pub struct ThreadlocalScanStackRoot<Edges: ProcessEdgesWork>(pub &'static mut Mutator<Edges::VM>);

// impl<E: ProcessEdgesWork> GCWork<E::VM> for ThreadlocalScanStackRoot<E> {
//     fn do_work(&mut self, worker: &mut GCWorker<E::VM>, mmtk: &'static MMTK<E::VM>) {
//         trace!(
//             "ThreadlocalScanStackRoot for mutator {:?}",
//             self.0.get_tls()
//         );

//         info!(
//             "ThreadlocalScanStackRoot executed by GC Thread {}",
//             crate::scheduler::worker::current_worker_ordinal().unwrap()
//         );

//         // let base = &mmtk.plan.base();
//         let factory =
//             ThreadlocalProcessEdgesWorkRootsWorkFactory::<E>::new(mmtk, self.0.mutator_tls);
//         <E::VM as VMBinding>::VMScanning::thread_local_scan_roots_of_mutator_threads(
//             worker.tls,
//             unsafe { &mut *(self.0 as *mut _) },
//             factory,
//         );
//         self.0.flush();

//         // base.set_gc_status(GcStatus::GcProper);
//     }
// }

// /// This provides an implementation of [`crate::scheduler::gc_work::ProcessEdgesWork`]. A plan that implements
// /// `PlanTraceObject` can use this work packet for tracing objects.
// pub struct PlanThreadlocalProcessEdges<VM: VMBinding, P, const KIND: TraceKind>
// where
//     VM: VMBinding,
//     P: Plan<VM = VM> + PlanThreadlocalTraceObject<VM>,
// {
//     plan: &'static P,
//     base: ProcessEdgesBase<VM>,
//     tls: VMMutatorThread,
//     mutator_id: u32,
// }

// impl<VM: VMBinding, P: PlanThreadlocalTraceObject<VM> + Plan<VM = VM>, const KIND: TraceKind>
//     ProcessEdgesWork for PlanThreadlocalProcessEdges<VM, P, KIND>
// {
//     type VM = VM;
//     type ScanObjectsWorkType = PlanThreadlocalScanObjects<Self, P>;

//     #[cfg(not(feature = "debug_publish_object"))]
//     fn new(
//         edges: Vec<EdgeOf<Self>>,
//         roots: bool,
//         mmtk: &'static MMTK<VM>,
//         tls: Option<VMMutatorThread>,
//     ) -> Self {
//         let base = ProcessEdgesBase::new(edges, roots, mmtk);
//         let plan = base.plan().downcast_ref::<P>().unwrap();
//         Self {
//             plan,
//             base,
//             tls: tls.unwrap(),
//             mutator_id: 0,
//             process_edges_work_counter: process_edges_work_counter.unwrap(),
//         }
//     }
//     #[cfg(feature = "debug_publish_object")]
//     fn new(
//         sources: Vec<ObjectReference>,
//         edges: Vec<EdgeOf<Self>>,
//         roots: bool,
//         vm_roots_type: u8,
//         mmtk: &'static MMTK<VM>,
//         tls: Option<VMMutatorThread>,
//     ) -> Self {
//         let base = ProcessEdgesBase::new(sources, edges, roots, vm_roots_type, mmtk);
//         let plan = base.plan().downcast_ref::<P>().unwrap();
//         let mutator = VM::VMActivePlan::mutator(tls.unwrap());

//         Self {
//             plan,
//             base,
//             tls: tls.unwrap(),
//             mutator_id: mutator.mutator_id,
//         }
//     }

//     /// Start the a scan work packet. If SCAN_OBJECTS_IMMEDIATELY, the work packet will be executed immediately, in this method.
//     /// Otherwise, the work packet will be added the Closure work bucket and will be dispatched later by the scheduler.
//     fn start_or_dispatch_scan_work(&mut self, work_packet: impl GCWork<Self::VM>) {
//         if Self::SCAN_OBJECTS_IMMEDIATELY {
//             // We execute this `scan_objects_work` immediately.
//             // This is expected to be a useful optimization because,
//             // say for _pmd_ with 200M heap, we're likely to have 50000~60000 `ScanObjects` work packets
//             // being dispatched (similar amount to `ProcessEdgesWork`).
//             // Executing these work packets now can remarkably reduce the global synchronization time.
//             self.worker().do_work(work_packet);
//         } else {
//             self.mmtk().scheduler.work_buckets[WorkBucketStage::Unconstrained]
//                 .add_local(work_packet);
//         }
//     }

//     fn create_scan_work(
//         &self,
//         nodes: Vec<ObjectReference>,
//         roots: bool,
//     ) -> Self::ScanObjectsWorkType {
//         PlanThreadlocalScanObjects::<Self, P>::new(self.plan, nodes, false, roots, self.tls)
//     }

//     fn trace_object(&mut self, object: ObjectReference) -> ObjectReference {
//         if object.is_null() {
//             return object;
//         }
//         // We cannot borrow `self` twice in a call, so we extract `worker` as a local variable.
//         let worker = self.worker();
//         // let mutator = VM::VMActivePlan::mutator(self.tls);
//         let mutator_id = self.mutator_id;
//         match self
//             .plan
//             .thread_local_trace_object::<KIND>(mutator_id, object, worker)
//         {
//             Scanned(object) => return object,
//             ToBeScanned(object) => {
//                 self.base.nodes.enqueue(object);
//                 return object;
//             }
//         }
//     }

//     fn process_edges(&mut self) {
//         for i in 0..self.edges.len() {
//             #[cfg(not(feature = "debug_publish_object"))]
//             self.process_edge(self.edges[i]);
//             #[cfg(feature = "debug_publish_object")]
//             self._process_edge(self.sources[i], self.edges[i]);
//         }
//     }

//     fn process_edge(&mut self, slot: EdgeOf<Self>) {
//         let object = slot.load();
//         let new_object = self.trace_object(object);
//         if P::thread_local_may_move_objects::<KIND>() {
//             slot.store(new_object);
//         }
//     }
// }

// impl<VM, P, const KIND: TraceKind> PlanThreadlocalProcessEdges<VM, P, KIND>
// where
//     VM: VMBinding,
//     P: PlanThreadlocalTraceObject<VM> + Plan<VM = VM>,
// {
//     fn _process_edge(&mut self, _source: ObjectReference, slot: EdgeOf<Self>) {
//         let object = slot.load();
//         let new_object = self.trace_object(object);
//         #[cfg(feature = "debug_publish_object")]
//         {
//             // in a local gc, public objects are not moved, so source is
//             // the exact object that needs to be looked at
//             if !_source.is_null() && crate::util::public_bit::is_public::<VM>(_source) {
//                 if !new_object.is_null() {
//                     debug_assert!(
//                         crate::util::public_bit::is_public::<VM>(new_object),
//                         "public object: {:?} {:?} points to private object: {:?} {:?}",
//                         _source,
//                         crate::util::object_extra_header_metadata::get_extra_header_metadata::<
//                             VM,
//                             usize,
//                         >(_source),
//                         new_object,
//                         crate::util::object_extra_header_metadata::get_extra_header_metadata::<
//                             VM,
//                             usize,
//                         >(new_object)
//                     );
//                 }
//             }
//         }
//         if P::thread_local_may_move_objects::<KIND>() {
//             slot.store(new_object);
//         }
//     }
// }

// // Impl Deref/DerefMut to ProcessEdgesBase for PlanProcessEdges
// impl<VM: VMBinding, P: PlanThreadlocalTraceObject<VM> + Plan<VM = VM>, const KIND: TraceKind> Deref
//     for PlanThreadlocalProcessEdges<VM, P, KIND>
// {
//     type Target = ProcessEdgesBase<VM>;
//     fn deref(&self) -> &Self::Target {
//         &self.base
//     }
// }

// impl<VM: VMBinding, P: PlanThreadlocalTraceObject<VM> + Plan<VM = VM>, const KIND: TraceKind>
//     DerefMut for PlanThreadlocalProcessEdges<VM, P, KIND>
// {
//     fn deref_mut(&mut self) -> &mut Self::Target {
//         &mut self.base
//     }
// }

// /// This is an alternative to `ScanObjects` that calls the `post_scan_object` of the policy
// /// selected by the plan.  It is applicable to plans that derive `PlanTraceObject`.
// pub struct PlanThreadlocalScanObjects<
//     E: ProcessEdgesWork,
//     P: Plan<VM = E::VM> + PlanThreadlocalTraceObject<E::VM>,
// > {
//     plan: &'static P,
//     buffer: Vec<ObjectReference>,
//     #[allow(dead_code)]
//     concurrent: bool,
//     roots: bool,
//     phantom: PhantomData<E>,
//     tls: VMMutatorThread,
// }

// impl<E: ProcessEdgesWork, P: Plan<VM = E::VM> + PlanThreadlocalTraceObject<E::VM>>
//     PlanThreadlocalScanObjects<E, P>
// {
//     pub fn new(
//         plan: &'static P,
//         buffer: Vec<ObjectReference>,
//         concurrent: bool,
//         roots: bool,
//         tls: VMMutatorThread,
//     ) -> Self {
//         Self {
//             plan,
//             buffer,
//             concurrent,
//             roots,
//             phantom: PhantomData,
//             tls,
//         }
//     }
// }

// impl<E: ProcessEdgesWork, P: Plan<VM = E::VM> + PlanThreadlocalTraceObject<E::VM>>
//     ScanObjectsWork<E::VM> for PlanThreadlocalScanObjects<E, P>
// {
//     type E = E;

//     fn roots(&self) -> bool {
//         self.roots
//     }

//     fn post_scan_object(&self, object: ObjectReference) {
//         let mutator = <E::VM as VMBinding>::VMActivePlan::mutator(self.tls);
//         self.plan.thread_local_post_scan_object(mutator, object);
//     }

//     fn make_another(&self, buffer: Vec<ObjectReference>) -> Self {
//         Self::new(self.plan, buffer, self.concurrent, false, self.tls)
//     }
// }

// impl<E: ProcessEdgesWork, P: Plan<VM = E::VM> + PlanThreadlocalTraceObject<E::VM>> GCWork<E::VM>
//     for PlanThreadlocalScanObjects<E, P>
// {
//     fn do_work(&mut self, worker: &mut GCWorker<E::VM>, mmtk: &'static MMTK<E::VM>) {
//         trace!("PlanScanObjects");

//         self.do_work_common(&self.buffer, worker, mmtk, true, Some(self.tls));
//         trace!("PlanScanObjects End");
//     }
// }

pub trait ThreadlocalObjectGraphTraversalClosure<VM: VMBinding>: EdgeVisitor<VM::VMEdge> {
    fn do_closure(&mut self);
    fn do_object_closure(&mut self, object: ObjectReference) -> ObjectReference;
    fn do_object_tracing(&mut self, object: ObjectReference) -> ObjectReference;
    fn new(
        mmtk: &'static MMTK<VM>,
        tls: VMMutatorThread,
        worker: &mut GCWorker<VM>,
        root_slots: Option<Vec<VM::VMEdge>>,
    ) -> Self;
}

pub struct PlanThreadlocalObjectGraphTraversalClosure<
    VM: VMBinding,
    P: Plan<VM = VM> + PlanThreadlocalTraceObject<VM>,
    const KIND: TraceKind,
> {
    plan: &'static P,
    tls: VMMutatorThread,
    edge_buffer: VecDeque<VM::VMEdge>,
    #[cfg(feature = "debug_publish_object")]
    source_buffer: VecDeque<ObjectReference>,
    // Use raw pointer for fast pointer dereferencing, instead of using `Option<&'static mut GCWorker<E::VM>>`.
    // Because a copying gc will dereference this pointer at least once for every object copy.
    worker: *mut GCWorker<VM>,
}

impl<VM: VMBinding, P: PlanThreadlocalTraceObject<VM> + Plan<VM = VM>, const KIND: TraceKind>
    ThreadlocalObjectGraphTraversalClosure<VM>
    for PlanThreadlocalObjectGraphTraversalClosure<VM, P, KIND>
{
    fn new(
        mmtk: &'static MMTK<VM>,
        tls: VMMutatorThread,
        worker: &mut GCWorker<VM>,
        root_slots: Option<Vec<VM::VMEdge>>,
    ) -> Self {
        let mut edge_buffer = VecDeque::new();
        #[cfg(feature = "debug_publish_object")]
        let mut source_buffer = VecDeque::new();
        if let Some(roots) = root_slots {
            #[cfg(feature = "debug_publish_object")]
            {
                source_buffer = VecDeque::with_capacity(roots.capacity());
                for slot in &roots {
                    source_buffer.push_back(slot.load());
                }
            }
            edge_buffer = VecDeque::from(roots);
        }

        Self {
            plan: mmtk.plan.downcast_ref::<P>().unwrap(),
            tls,
            edge_buffer,
            #[cfg(feature = "debug_publish_object")]
            source_buffer,
            worker,
        }
    }

    fn do_closure(&mut self) {
        #[cfg(feature = "debug_publish_object")]
        debug_assert!(
            self.edge_buffer.len() == self.source_buffer.len(),
            "slots len != object len"
        );
        let mutator = VM::VMActivePlan::mutator(self.tls);
        let mutator_id = mutator.mutator_id;
        while !self.edge_buffer.is_empty() {
            let slot = self.edge_buffer.pop_front().unwrap();
            #[cfg(feature = "debug_publish_object")]
            let _source = self.source_buffer.pop_front().unwrap();
            let object = slot.load();
            if object.is_null() {
                continue;
            }

            let new_object =
                match self
                    .plan
                    .thread_local_trace_object::<KIND>(mutator_id, object, self.worker())
                {
                    Scanned(new_object) => new_object,
                    ToBeScanned(new_object) => {
                        VM::VMScanning::scan_object(
                            crate::util::VMWorkerThread(crate::util::VMThread::UNINITIALIZED),
                            new_object,
                            self,
                        );
                        self.plan.thread_local_post_scan_object(mutator, new_object);
                        new_object
                    }
                };

            #[cfg(feature = "debug_publish_object")]
            {
                // in a local gc, public objects are not moved, so source is
                // the exact object that needs to be looked at
                if !_source.is_null() && crate::util::public_bit::is_public::<VM>(_source) {
                    debug_assert!(
                        crate::util::public_bit::is_public::<VM>(new_object),
                        "public object: {:?} {:?} points to private object: {:?} {:?}",
                        _source,
                        crate::util::object_extra_header_metadata::get_extra_header_metadata::<
                            VM,
                            usize,
                        >(_source),
                        new_object,
                        crate::util::object_extra_header_metadata::get_extra_header_metadata::<
                            VM,
                            usize,
                        >(new_object)
                    );
                }
            }
            if P::thread_local_may_move_objects::<KIND>() {
                slot.store(new_object);
            }
        }
    }

    fn do_object_closure(&mut self, object: ObjectReference) -> ObjectReference {
        debug_assert!(!object.is_null(), "object should not be null");
        let mutator = VM::VMActivePlan::mutator(self.tls);
        let mutator_id = mutator.mutator_id;

        let new_object =
            match self
                .plan
                .thread_local_trace_object::<KIND>(mutator_id, object, self.worker())
            {
                Scanned(new_object) => {
                    debug_assert!(
                        object.is_live(),
                        "object: {:?} is supposed to be alive.",
                        object
                    );
                    new_object
                }
                ToBeScanned(new_object) => {
                    VM::VMScanning::scan_object(self.worker().tls, new_object, self);
                    self.plan.thread_local_post_scan_object(mutator, new_object);
                    new_object
                }
            };

        self.do_closure();
        new_object
    }

    fn do_object_tracing(&mut self, object: ObjectReference) -> ObjectReference {
        let mutator = VM::VMActivePlan::mutator(self.tls);
        let mutator_id = mutator.mutator_id;
        let new_object =
            match self
                .plan
                .thread_local_trace_object::<KIND>(mutator_id, object, self.worker())
            {
                Scanned(new_object) => new_object,
                _ => {
                    panic!(
                        "live object: {:?} must have been traced/scanned already",
                        object
                    );
                }
            };
        new_object
    }
}

impl<VM: VMBinding, P: PlanThreadlocalTraceObject<VM> + Plan<VM = VM>, const KIND: TraceKind>
    EdgeVisitor<VM::VMEdge> for PlanThreadlocalObjectGraphTraversalClosure<VM, P, KIND>
{
    #[cfg(not(feature = "debug_publish_object"))]
    fn visit_edge(&mut self, edge: VM::VMEdge) {
        if !edge.load().is_null() {
            self.edge_buffer.push_back(edge);
        }
    }

    #[cfg(feature = "debug_publish_object")]
    fn visit_edge(&mut self, object: ObjectReference, edge: VM::VMEdge) {
        self.source_buffer.push_back(object);
        self.edge_buffer.push_back(edge);
    }
}

impl<VM: VMBinding, P: PlanThreadlocalTraceObject<VM> + Plan<VM = VM>, const KIND: TraceKind> Drop
    for PlanThreadlocalObjectGraphTraversalClosure<VM, P, KIND>
{
    #[inline(always)]
    fn drop(&mut self) {
        assert!(
            self.edge_buffer.is_empty(),
            "There are edges left over. Closure is not done correctly."
        );
    }
}

impl<VM: VMBinding, P: PlanThreadlocalTraceObject<VM> + Plan<VM = VM>, const KIND: TraceKind>
    PlanThreadlocalObjectGraphTraversalClosure<VM, P, KIND>
{
    pub fn worker(&self) -> &'static mut GCWorker<VM> {
        unsafe { &mut *self.worker }
    }
}

pub struct ThreadlocalFinalization<VM, Closure>
where
    VM: VMBinding,
    Closure: ThreadlocalObjectGraphTraversalClosure<VM>,
{
    mmtk: &'static MMTK<VM>,
    tls: VMMutatorThread,
    // Use raw pointer for fast pointer dereferencing, instead of using `Option<&'static mut GCWorker<E::VM>>`.
    // Because a copying gc will dereference this pointer at least once for every object copy.
    worker: *mut GCWorker<VM>,
    phantom: PhantomData<Closure>,
}

impl<VM, Closure> ThreadlocalFinalization<VM, Closure>
where
    VM: VMBinding,
    Closure: ThreadlocalObjectGraphTraversalClosure<VM>,
{
    pub fn new(mmtk: &'static MMTK<VM>, tls: VMMutatorThread, worker: &mut GCWorker<VM>) -> Self {
        Self {
            mmtk,
            tls,
            worker,
            phantom: PhantomData,
        }
    }

    pub fn worker(&self) -> &'static mut GCWorker<VM> {
        unsafe { &mut *self.worker }
    }

    pub fn do_finalization(&self) {
        let mutator = VM::VMActivePlan::mutator(self.tls);
        let mut closure = Closure::new(self.mmtk, self.tls, self.worker(), None);
        let mut ready_for_finalize = vec![];
        for mut f in mutator
            .finalizable_candidates
            .drain(0..)
            .collect::<Vec<<VM::VMReferenceGlue as ReferenceGlue<VM>>::FinalizableType>>()
        {
            let reff: ObjectReference = f.get_reference();

            trace!("Pop {:?} for finalization in local gc", reff);
            if crate::util::public_bit::is_public::<VM>(reff) {
                // public object is untouched, so nothing needs to be done
                continue;
            } else if reff.is_live() {
                // live object indicates that the object has already been traced/scanned during transitive closure phase
                // so no need to do the closure again
                let object = closure.do_object_tracing(f.get_reference());
                f.set_reference(object);
                trace!(
                    "{:?} is live, push {:?} back to local candidates buffer",
                    reff,
                    f
                );
                mutator.finalizable_candidates.push(f);
                continue;
            } else {
                // The object is private and dead, so it can be finalized
                ready_for_finalize.push(f);
            }
        }
        // Keep the finalizable objects alive.
        ready_for_finalize.iter_mut().for_each(|f| {
            let object = closure.do_object_closure(f.get_reference());
            f.set_reference(object)
        });
        // Now push all local ready for finalize objects to the global list
        let mut finalizable_processor = self.mmtk.finalizable_processor.lock().unwrap();
        finalizable_processor.add_ready_for_finalize_objects(ready_for_finalize);

        // Maybe should leave this to the global gc ???
        VM::VMCollection::schedule_finalization(self.worker().tls);
    }
}
