use crate::plan::PlanThreadlocalTraceObject;
use crate::plan::ThreadlocalTracedObjectType::*;
use crate::policy::gc_work::TraceKind;
use crate::util::*;
use crate::vm::edge_shape::Edge;
use crate::vm::*;
use crate::*;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::atomic::AtomicBool;

pub const THREAD_LOCAL_GC_ACTIVE: u32 = 1;
pub const THREAD_LOCAL_GC_INACTIVE: u32 = 0;

pub struct ExecuteThreadlocalCollection<VM: VMBinding> {
    pub mmtk: &'static MMTK<VM>,
    pub mutator_tls: VMMutatorThread,
    pub start_time: std::time::Instant,
}

impl<VM: VMBinding> ExecuteThreadlocalCollection<VM> {
    pub fn execute(&mut self) {
        let mutator = VM::VMActivePlan::mutator(self.mutator_tls);
        mutator.thread_local_gc_status = THREAD_LOCAL_GC_ACTIVE;
        info!("Start of Thread local GC {:?}", mutator.mutator_id,);

        // A hook of local gc, no-op at the moment
        self.mmtk
            .gc_trigger
            .policy
            .on_thread_local_gc_start(self.mmtk, mutator);

        self.mmtk
            .get_plan()
            .do_thread_local_collection(self.mutator_tls, self.mmtk);
        let elapsed = self.start_time.elapsed();
        mutator.thread_local_gc_status = THREAD_LOCAL_GC_INACTIVE;
        info!(
            "End of Thread local GC {} ({}/{} pages, took {} ms)",
            mutator.mutator_id,
            self.mmtk.get_plan().get_reserved_pages(),
            self.mmtk.get_plan().get_total_pages(),
            elapsed.as_millis()
        );
        self.mmtk
            .gc_trigger
            .policy
            .on_thread_local_gc_end(self.mmtk, mutator);
        #[cfg(feature = "debug_thread_local_gc_copying")]
        mutator.reset_stats();
        // local gc has finished,
        match LOCAL_GC_ACTIVE.compare_exchange(
            true,
            false,
            atomic::Ordering::SeqCst,
            atomic::Ordering::SeqCst,
        ) {
            Err(_) => panic!("LOCAL_GC_ACTIVE is broken"),
            _ => (),
        };
    }
}

/// The thread-local GC Preparation Work
/// This work packet invokes prepare() for the plan (which will invoke prepare() for each space), and
/// pushes work packets for preparing mutators and collectors.
/// We should only have one such work packet per GC, before any actual GC work starts.
/// We assume this work packet is the only running work packet that accesses plan, and there should
/// be no other concurrent work packet that accesses plan (read or write). Otherwise, there may
/// be a race condition.
pub struct ThreadlocalPrepare<VM: VMBinding> {
    tls: VMMutatorThread,
    phantom: PhantomData<VM>,
}

impl<VM: VMBinding> ThreadlocalPrepare<VM> {
    pub fn new(tls: VMMutatorThread) -> Self {
        Self {
            tls,
            phantom: PhantomData,
        }
    }

    pub fn execute(&mut self) {
        trace!("Thread local Prepare Mutator");
        let mutator = VM::VMActivePlan::mutator(self.tls);
        mutator.thread_local_prepare();
    }
}

/// The thread local GC release Work
/// This work packet invokes release() for the plan (which will invoke release() for each space), and
/// pushes work packets for releasing mutators and collectors.
/// We should only have one such work packet per GC, after all actual GC work ends.
/// We assume this work packet is the only running work packet that accesses plan, and there should
/// be no other concurrent work packet that accesses plan (read or write). Otherwise, there may
/// be a race condition.
pub struct ThreadlocalRelease<VM: VMBinding> {
    tls: VMMutatorThread,
    phantom: PhantomData<VM>,
}

impl<VM: VMBinding> ThreadlocalRelease<VM> {
    pub fn new(tls: VMMutatorThread) -> Self {
        Self {
            phantom: PhantomData,
            tls,
        }
    }

    pub fn execute(&mut self) {
        trace!("Thread local Release");
        let mutator = VM::VMActivePlan::mutator(self.tls);
        // self.plan.base().gc_trigger.policy.on_gc_release(mmtk);

        trace!("Thread local Release Mutator");
        mutator.thread_local_release();
    }
}

pub struct EndOfThreadLocalGC {
    pub tls: VMMutatorThread,
}

impl EndOfThreadLocalGC {
    pub fn execute<VM: VMBinding>(&mut self, _mmtk: &'static MMTK<VM>) {
        #[cfg(feature = "extreme_assertions")]
        if crate::util::edge_logger::should_check_duplicate_edges(&*mmtk.plan) {
            // reset the logging info at the end of each GC
            mmtk.edge_logger.reset();
        }
    }
}

/// Scan a specific mutator
pub struct ScanMutator<VM, Closure>
where
    VM: VMBinding,
    Closure: ThreadlocalObjectGraphTraversalClosure<VM>,
{
    tls: VMMutatorThread,
    mmtk: &'static MMTK<VM>,
    phantom: PhantomData<Closure>,
}

impl<VM, Closure> ScanMutator<VM, Closure>
where
    VM: VMBinding,
    Closure: ThreadlocalObjectGraphTraversalClosure<VM>,
{
    pub fn new(tls: VMMutatorThread, mmtk: &'static MMTK<VM>) -> Self {
        Self {
            tls,
            mmtk,
            phantom: PhantomData,
        }
    }

    pub fn execute(&mut self) {
        trace!("scan_mutator start");
        self.mmtk.state.prepare_for_stack_scanning();
        let object_graph_traversal =
            ThreadlocalObjectGraphTraversal::<VM, Closure>::new(self.mmtk, self.tls);
        VM::VMCollection::scan_mutator(self.tls, object_graph_traversal);
        trace!("scan_mutator end");
    }
}

pub struct ThreadlocalObjectGraphTraversal<VM, Closure>
where
    VM: VMBinding,
    Closure: ThreadlocalObjectGraphTraversalClosure<VM>,
{
    mmtk: &'static MMTK<VM>,
    tls: VMMutatorThread,
    phantom: PhantomData<Closure>,
}

impl<VM, Closure> ObjectGraphTraversal<VM::VMEdge> for ThreadlocalObjectGraphTraversal<VM, Closure>
where
    VM: VMBinding,
    Closure: ThreadlocalObjectGraphTraversalClosure<VM>,
{
    fn traverse_from_roots(&mut self, root_slots: Vec<VM::VMEdge>) {
        Closure::new(self.mmtk, self.tls, Some(root_slots)).do_closure();
    }
}

impl<VM, Closure> ThreadlocalObjectGraphTraversal<VM, Closure>
where
    VM: VMBinding,
    Closure: ThreadlocalObjectGraphTraversalClosure<VM>,
{
    pub fn new(mmtk: &'static MMTK<VM>, tls: VMMutatorThread) -> Self {
        Self {
            mmtk,
            tls,
            phantom: PhantomData,
        }
    }
}

pub trait ThreadlocalObjectGraphTraversalClosure<VM: VMBinding>: EdgeVisitor<VM::VMEdge> {
    fn do_closure(&mut self);
    fn do_object_closure(&mut self, object: ObjectReference) -> ObjectReference;
    fn do_object_tracing(&mut self, object: ObjectReference) -> ObjectReference;
    fn new(
        mmtk: &'static MMTK<VM>,
        tls: VMMutatorThread,
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
}

impl<VM, P, const KIND: TraceKind> ThreadlocalObjectGraphTraversalClosure<VM>
    for PlanThreadlocalObjectGraphTraversalClosure<VM, P, KIND>
where
    VM: VMBinding,
    P: PlanThreadlocalTraceObject<VM> + Plan<VM = VM>,
{
    fn new(
        mmtk: &'static MMTK<VM>,
        tls: VMMutatorThread,
        root_slots: Option<Vec<VM::VMEdge>>,
    ) -> Self {
        let mut edge_buffer = VecDeque::with_capacity(4096);
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
            plan: mmtk.get_plan().downcast_ref::<P>().unwrap(),
            tls,
            edge_buffer,
            #[cfg(feature = "debug_publish_object")]
            source_buffer,
        }
    }

    fn do_closure(&mut self) {
        #[cfg(feature = "debug_publish_object")]
        assert!(
            self.edge_buffer.len() == self.source_buffer.len(),
            "slots len != object len"
        );
        let mutator = VM::VMActivePlan::mutator(self.tls);

        while !self.edge_buffer.is_empty() {
            let slot = self.edge_buffer.pop_front().unwrap();
            #[cfg(feature = "debug_publish_object")]
            let _source = self.source_buffer.pop_front().unwrap();
            let object = slot.load();
            if object.is_null() {
                continue;
            }
            #[cfg(not(feature = "debug_publish_object"))]
            let new_object = match self.plan.thread_local_trace_object::<KIND>(mutator, object) {
                Scanned(new_object) => new_object,
                ToBeScanned(new_object) => {
                    VM::VMScanning::scan_object(
                        VMWorkerThread(VMThread::UNINITIALIZED),
                        new_object,
                        self,
                    );
                    self.plan.thread_local_post_scan_object(mutator, new_object);
                    new_object
                }
            };

            #[cfg(feature = "debug_publish_object")]
            let new_object = match self
                .plan
                .thread_local_trace_object::<KIND>(mutator, _source, slot, object)
            {
                Scanned(new_object) => {
                    if crate::util::metadata::public_bit::is_public::<VM>(object) {
                        assert!(
                            crate::util::metadata::public_bit::is_public::<VM>(new_object),
                            "public bit is corrupted. public obj: {} | private new_obj: {} ",
                            object,
                            new_object
                        );
                    }

                    new_object
                }
                ToBeScanned(new_object) => {
                    VM::VMScanning::scan_object(
                        VMWorkerThread(VMThread::UNINITIALIZED), // worker tls is not being used by the openjdk binding
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
                if !_source.is_null() && crate::util::metadata::public_bit::is_public::<VM>(_source)
                {
                    assert!(
                        crate::util::metadata::public_bit::is_public::<VM>(new_object),
                        "public object: {:?} {:?} points to private object: {:?} {:?}",
                        _source,
                        crate::util::object_extra_header_metadata::get_extra_header_metadata::<
                            VM,
                            usize,
                        >(_source)
                            & object_extra_header_metadata::BOTTOM_HALF_MASK,
                        new_object,
                        crate::util::object_extra_header_metadata::get_extra_header_metadata::<
                            VM,
                            usize,
                        >(new_object)
                            & object_extra_header_metadata::BOTTOM_HALF_MASK,
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

        #[cfg(not(feature = "debug_publish_object"))]
        let new_object = match self.plan.thread_local_trace_object::<KIND>(mutator, object) {
            Scanned(new_object) => {
                debug_assert!(
                    object.is_live(),
                    "object: {:?} is supposed to be alive.",
                    object
                );
                new_object
            }
            ToBeScanned(new_object) => {
                VM::VMScanning::scan_object(
                    VMWorkerThread(VMThread::UNINITIALIZED),
                    new_object,
                    self,
                );
                self.plan.thread_local_post_scan_object(mutator, new_object);
                new_object
            }
        };

        #[cfg(feature = "debug_publish_object")]
        let new_object = match self.plan.thread_local_trace_object::<KIND>(
            mutator,
            object,
            VM::VMObjectModel::null_slot(),
            object,
        ) {
            Scanned(new_object) => {
                debug_assert!(
                    object.is_live(),
                    "object: {:?} is supposed to be alive.",
                    object
                );
                new_object
            }
            ToBeScanned(new_object) => {
                VM::VMScanning::scan_object(
                    VMWorkerThread(VMThread::UNINITIALIZED),
                    new_object,
                    self,
                );
                self.plan.thread_local_post_scan_object(mutator, new_object);
                new_object
            }
        };

        self.do_closure();
        new_object
    }

    fn do_object_tracing(&mut self, object: ObjectReference) -> ObjectReference {
        let mutator = VM::VMActivePlan::mutator(self.tls);

        #[cfg(not(feature = "debug_publish_object"))]
        let new_object = match self.plan.thread_local_trace_object::<KIND>(mutator, object) {
            Scanned(new_object) => new_object,
            _ => {
                panic!(
                    "live object: {:?} must have been traced/scanned already",
                    object
                );
            }
        };

        #[cfg(feature = "debug_publish_object")]
        let new_object = match self.plan.thread_local_trace_object::<KIND>(
            mutator,
            object,
            VM::VMObjectModel::null_slot(),
            object,
        ) {
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

pub struct ThreadlocalFinalization<VM, Closure>
where
    VM: VMBinding,
    Closure: ThreadlocalObjectGraphTraversalClosure<VM>,
{
    mmtk: &'static MMTK<VM>,
    tls: VMMutatorThread,
    // // Use raw pointer for fast pointer dereferencing, instead of using `Option<&'static mut GCWorker<E::VM>>`.
    // // Because a copying gc will dereference this pointer at least once for every object copy.
    // worker: *mut GCWorker<VM>,
    phantom: PhantomData<Closure>,
}

impl<VM, Closure> ThreadlocalFinalization<VM, Closure>
where
    VM: VMBinding,
    Closure: ThreadlocalObjectGraphTraversalClosure<VM>,
{
    pub fn new(tls: VMMutatorThread, mmtk: &'static MMTK<VM>) -> Self {
        Self {
            mmtk,
            tls,
            phantom: PhantomData,
        }
    }

    pub fn do_finalization(&self) {
        let mutator = VM::VMActivePlan::mutator(self.tls);
        let mut closure = Closure::new(self.mmtk, self.tls, None);
        let mut ready_for_finalize = vec![];
        for mut f in mutator
            .finalizable_candidates
            .drain(0..)
            .collect::<Vec<<VM::VMReferenceGlue as ReferenceGlue<VM>>::FinalizableType>>()
        {
            let reff: ObjectReference = f.get_reference();

            trace!("Pop {:?} for finalization in local gc", reff);
            if crate::util::metadata::public_bit::is_public::<VM>(reff) {
                // public object is untouched, so nothing needs to be done
                continue;
            }
            if reff.is_live() {
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

        // Finalization thread is expecting a gc thread to wake it up, since
        // local gc is done by the mutator itself, finalization cannot be done after a
        // local gc.

        // VM::VMCollection::schedule_finalization(VMWorkerThread(VMThread::UNINITIALIZED));
    }
}

pub(crate) static LOCAL_GC_ACTIVE: AtomicBool = AtomicBool::new(false);
