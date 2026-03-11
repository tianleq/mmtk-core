use scheduler::GCWorker;

use crate::plan::PlanThreadlocalTraceObject;
use crate::plan::ThreadlocalTracedObjectType::*;
use crate::policy::gc_work::TraceKind;
use crate::policy::space::Space;
use crate::util::metadata::public_bit::is_public;
use crate::util::*;
use crate::vm::slot::Slot;
use crate::vm::*;
use crate::*;
use std::marker::PhantomData;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

pub const THREAD_LOCAL_GC_ACTIVE: u32 = 1;
pub const THREAD_LOCAL_GC_INACTIVE: u32 = 0;
pub const THREAD_LOCAL_GC_PENDING: u32 = u32::MAX;

lazy_static! {
    pub static ref IMMIX_LIVE_BYTES_IN_FORCED_LOCAL_GC: AtomicUsize = AtomicUsize::new(0);
    pub static ref LOS_LIVE_BYTES_IN_FORCED_LOCAL_GC: AtomicUsize = AtomicUsize::new(0);
}

pub struct ScheduleExecuteThreadlocalCollectionWork;

impl<VM: VMBinding> scheduler::GCWork<VM> for ScheduleExecuteThreadlocalCollectionWork {
    fn do_work(&mut self, worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        IMMIX_LIVE_BYTES_IN_FORCED_LOCAL_GC.store(0, std::sync::atomic::Ordering::SeqCst);
        LOS_LIVE_BYTES_IN_FORCED_LOCAL_GC.store(0, std::sync::atomic::Ordering::SeqCst);
        let count = _mmtk.get_options().get_max_concurrent_local_gc() as usize;

        match _mmtk.get_options().get_local_gc_policy() {
            options::LocalGCPolicy::NONE => {
                for mutator in VM::VMActivePlan::mutators() {
                    if mutator.is_thread_local_gc_pending() {
                        if mutator.has_mutator_allocated() {
                            worker.add_work(
                                scheduler::WorkBucketStage::Local,
                                ExecuteThreadlocalCollectionWork::new(mutator.mutator_tls),
                            );
                        } else {
                            worker.add_work(
                                scheduler::WorkBucketStage::Local,
                                ExecuteThreadlocalMarkingWork::new(mutator.mutator_tls),
                            );
                        }
                    }
                }
            }
            options::LocalGCPolicy::APPLICATIONS => {
                for mutator in VM::VMActivePlan::mutators() {
                    if (mutator.is_thread_local_gc_pending() && mutator.has_mutator_allocated())
                        || mutator.mutator_id >= 30
                    {
                        worker.add_work(
                            scheduler::WorkBucketStage::Local,
                            ExecuteThreadlocalCollectionWork::new(mutator.mutator_tls),
                        );
                    }
                }
            }
            options::LocalGCPolicy::INTERNAL => {
                for mutator in VM::VMActivePlan::mutators() {
                    if (mutator.is_thread_local_gc_pending() && mutator.has_mutator_allocated())
                        || mutator.mutator_id < 30
                    {
                        worker.add_work(
                            scheduler::WorkBucketStage::Local,
                            ExecuteThreadlocalCollectionWork::new(mutator.mutator_tls),
                        );
                    }
                }
            }
            options::LocalGCPolicy::LRT => {
                let mut mutators = Vec::with_capacity(count);
                for mutator in VM::VMActivePlan::mutators() {
                    mutators.push(mutator);
                }
                mutators
                    .sort_by(|m1, m2: &_| m2.local_allocation_size.cmp(&m1.local_allocation_size));
                for m in mutators.into_iter().take(count) {
                    worker.add_work(
                        scheduler::WorkBucketStage::Local,
                        ExecuteThreadlocalCollectionWork::new(m.mutator_tls),
                    );
                }
            }
            options::LocalGCPolicy::ALL => {
                for mutator in VM::VMActivePlan::mutators() {
                    worker.add_work(
                        scheduler::WorkBucketStage::Local,
                        ExecuteThreadlocalCollectionWork::new(mutator.mutator_tls),
                    );
                }
            }
        }
    }
}

struct ExecuteThreadlocalMarkingWork {
    mutator_tls: VMMutatorThread,
}

impl ExecuteThreadlocalMarkingWork {
    pub fn new(tls: VMMutatorThread) -> Self {
        Self { mutator_tls: tls }
    }
}

impl<VM: VMBinding> scheduler::GCWork<VM> for ExecuteThreadlocalMarkingWork {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        mmtk.get_plan()
            .do_thread_local_marking(self.mutator_tls, mmtk);
    }
}

struct ExecuteThreadlocalCollectionWork {
    mutator_tls: VMMutatorThread,
}

impl ExecuteThreadlocalCollectionWork {
    pub fn new(tls: VMMutatorThread) -> Self {
        Self { mutator_tls: tls }
    }
}

impl<VM: VMBinding> scheduler::GCWork<VM> for ExecuteThreadlocalCollectionWork {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        ExecuteThreadlocalCollection {
            mmtk,
            mutator_tls: self.mutator_tls,
            start_time: std::time::Instant::now(),
        }
        .execute();
        {
            let plan = mmtk
                .get_plan()
                .downcast_ref::<crate::plan::immix::Immix<VM>>()
                .unwrap();
            let immix_index = plan.immix_space.get_descriptor().get_index();
            let los_index = plan.common().get_los().get_descriptor().get_index();
            let mut live_bytes_stats = _worker.shared.live_bytes_per_space.borrow_mut();
            live_bytes_stats[immix_index] +=
                IMMIX_LIVE_BYTES_IN_FORCED_LOCAL_GC.swap(0, Ordering::SeqCst);
            live_bytes_stats[los_index] +=
                LOS_LIVE_BYTES_IN_FORCED_LOCAL_GC.swap(0, Ordering::SeqCst);
        }
    }
}

pub struct ExecuteThreadlocalCollection<VM: VMBinding> {
    pub mmtk: &'static MMTK<VM>,
    pub mutator_tls: VMMutatorThread,
    pub start_time: std::time::Instant,
}

impl<VM: VMBinding> ExecuteThreadlocalCollection<VM> {
    pub fn execute(&mut self) {
        let mutator = VM::VMActivePlan::mutator(self.mutator_tls);
        mutator.thread_local_gc_status = THREAD_LOCAL_GC_ACTIVE;
        // record before local GC starts so that evacuation does not increase its value
        let allocation_bytes = mutator.allocation_bytes;
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
        {
            mutator.reset_stats();
        }
        mutator.local_allocation_size = 0;
        mutator.allocation_bytes = allocation_bytes;
        // local gc has finished,
        ACTIVE_LOCAL_GC_COUNTER.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

/// The thread-local GC Preparation Work
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

pub struct ThreadlocalDefragPrepare<VM: VMBinding> {
    tls: VMMutatorThread,
    phantom: PhantomData<VM>,
}

impl<VM: VMBinding> ThreadlocalDefragPrepare<VM> {
    pub fn new(tls: VMMutatorThread) -> Self {
        Self {
            tls,
            phantom: PhantomData,
        }
    }

    pub fn execute(&mut self) {
        trace!("Thread local DefragPrepare Mutator");
        let mutator = VM::VMActivePlan::mutator(self.tls);
        mutator.thread_local_defrag_prepare();
    }
}

/// The thread local GC release Work
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
    pub _tls: VMMutatorThread,
}

impl EndOfThreadLocalGC {
    pub fn execute<VM: VMBinding>(&mut self, _mmtk: &'static MMTK<VM>) {
        #[cfg(feature = "extreme_assertions")]
        if crate::util::edge_logger::should_check_duplicate_edges(&*_mmtk.plan) {
            // reset the logging info at the end of each GC
            mmtk.edge_logger.reset();
        }
    }
}

/// Scan a specific mutator
pub struct ScanMutator<VM, Closure, const KIND: TraceKind>
where
    VM: VMBinding,
    Closure: ThreadlocalObjectGraphTraversalClosure<VM>,
{
    tls: VMMutatorThread,
    mmtk: &'static MMTK<VM>,
    worker: Option<*mut GCWorker<VM>>,
    phantom: PhantomData<Closure>,
}

impl<VM, Closure, const KIND: TraceKind> ScanMutator<VM, Closure, KIND>
where
    VM: VMBinding,
    Closure: ThreadlocalObjectGraphTraversalClosure<VM>,
{
    pub fn new(
        tls: VMMutatorThread,
        mmtk: &'static MMTK<VM>,
        worker: Option<*mut GCWorker<VM>>,
    ) -> Self {
        Self {
            tls,
            mmtk,
            worker,
            phantom: PhantomData,
        }
    }

    pub fn execute(&mut self) {
        trace!("scan_mutator start");
        let object_graph_traversal = ThreadlocalObjectGraphTraversal::<VM, Closure, KIND>::new(
            self.mmtk,
            self.tls,
            self.worker,
        );
        VM::VMCollection::scan_mutator(self.tls, object_graph_traversal);
        trace!("scan_mutator end");
    }
}

pub struct ThreadlocalObjectGraphTraversal<VM, Closure, const KIND: TraceKind>
where
    VM: VMBinding,
    Closure: ThreadlocalObjectGraphTraversalClosure<VM>,
{
    mmtk: &'static MMTK<VM>,
    tls: VMMutatorThread,
    worker: Option<*mut GCWorker<VM>>,
    phantom: PhantomData<Closure>,
}

impl<VM, Closure, const KIND: TraceKind> ThreadlocalObjectGraphTraversal<VM, Closure, KIND>
where
    VM: VMBinding,
    Closure: ThreadlocalObjectGraphTraversalClosure<VM>,
{
    pub fn new(
        mmtk: &'static MMTK<VM>,
        tls: VMMutatorThread,
        worker: Option<*mut GCWorker<VM>>,
    ) -> Self {
        Self {
            mmtk,
            tls,
            worker,
            phantom: PhantomData,
        }
    }
}
impl<VM, Closure, const KIND: TraceKind> ObjectGraphTraversal<VM::VMSlot>
    for ThreadlocalObjectGraphTraversal<VM, Closure, KIND>
where
    VM: VMBinding,
    Closure: ThreadlocalObjectGraphTraversalClosure<VM>,
{
    fn traverse_from_roots(&mut self, root_slots: Vec<VM::VMSlot>) {
        // #[cfg(debug_assertions)]
        // {
        //     use crate::policy::STACK_ROOTS;

        //     STACK_ROOTS
        //         .lock()
        //         .unwrap()
        //         .extend(root_slots.iter().copied().map(|s| s.to_address()));
        // }
        let root_slots = Some(root_slots);
        Closure::new(self.mmtk, self.tls, root_slots, self.worker).do_closure();
    }

    fn report_roots(&mut self, _root_slots: Vec<VM::VMSlot>) {
        unreachable!();
    }

    fn traverse(&mut self) {
        unreachable!();
    }
}

pub trait ThreadlocalObjectGraphTraversalClosure<VM: VMBinding>: SlotVisitor<VM::VMSlot> {
    fn do_closure(&mut self);
    fn do_object_closure(&mut self, object: ObjectReference) -> ObjectReference;
    fn do_object_tracing(&mut self, object: ObjectReference) -> ObjectReference;
    fn new(
        mmtk: &'static MMTK<VM>,
        tls: VMMutatorThread,
        root_slots: Option<Vec<VM::VMSlot>>,
        worker: Option<*mut GCWorker<VM>>,
    ) -> Self;
}

// only collect stack roots, no scanning/tracing
pub struct ThreadStackWalker<VM>
where
    VM: VMBinding,
{
    tls: VMMutatorThread,
    _p: PhantomData<VM>,
}

impl<VM> ThreadStackWalker<VM>
where
    VM: VMBinding,
{
    pub fn new(tls: VMMutatorThread) -> Self {
        Self {
            tls,
            _p: PhantomData,
        }
    }
}

impl<VM> ObjectGraphTraversal<VM::VMSlot> for ThreadStackWalker<VM>
where
    VM: VMBinding,
{
    fn traverse_from_roots(&mut self, root_slots: Vec<VM::VMSlot>) {
        self.report_roots(root_slots);
    }

    fn report_roots(&mut self, root_slots: Vec<VM::VMSlot>) {
        // for slots containing null/private objects, there is no need to collect them
        // #[cfg(debug_assertions)]
        // {
        //     root_slots
        //         .iter()
        //         .copied()
        //         .filter(|slot| slot.load().is_some_and(is_public))
        //         .for_each(|slot| println!("stack slot: {:?}, object: {:?}", slot, slot.load()));
        // }

        // cannot store stack roots into the slot remset as the remset is carried over to the next local GC
        // but stack slots will become invalid shortly after the GC. Instead, push those stack slots into a
        // dedicated buffer
        let mutator = VM::VMActivePlan::mutator(self.tls);
        mutator.stack_slots.extend(
            root_slots
                .iter()
                // .copied()
                .filter(|slot| slot.load().is_some_and(is_public)),
        );
    }

    fn traverse(&mut self) {
        unreachable!();
    }
}

pub struct PlanThreadlocalObjectGraphTraversalClosure<
    VM: VMBinding,
    P: Plan<VM = VM> + PlanThreadlocalTraceObject<VM>,
    const KIND: TraceKind,
> {
    plan: &'static P,
    tls: VMMutatorThread,
    slot_buffer: Vec<VM::VMSlot>,
    source_buffer: Vec<Option<ObjectReference>>,
    worker: Option<*mut GCWorker<VM>>,
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
        root_slots: Option<Vec<VM::VMSlot>>,
        worker: Option<*mut GCWorker<VM>>,
    ) -> Self {
        let mut slot_buffer = Vec::with_capacity(4096);
        // let mutator = VM::VMActivePlan::mutator(tls);

        let mut source_buffer = Vec::new();
        if let Some(root_slots) = root_slots {
            source_buffer = Vec::with_capacity(root_slots.capacity());
            for slot in &root_slots {
                let root = slot.load();
                source_buffer.push(root);
            }

            slot_buffer = root_slots;
        }
        debug_assert_eq!(slot_buffer.len(), source_buffer.len());
        Self {
            plan: mmtk.get_plan().downcast_ref::<P>().unwrap(),
            tls,
            slot_buffer,
            source_buffer,
            worker,
        }
    }

    fn do_closure(&mut self) {
        #[cfg(feature = "debug_publish_object")]
        assert!(
            self.slot_buffer.len() == self.source_buffer.len(),
            "slots len != object len"
        );
        let mutator = VM::VMActivePlan::mutator(self.tls);

        while let Some(slot) = self.slot_buffer.pop() {
            let _source = self.source_buffer.pop().unwrap();
            let _object = slot.load();
            #[cfg(feature = "debug_publish_object")]
            let (Some(object), Some(source)) = (_object, _source) else {
                continue;
            };

            #[cfg(not(feature = "debug_publish_object"))]
            let Some(object) = _object
            else {
                continue;
            };

            let source = _source.unwrap();
            #[cfg(debug_assertions)]
            {
                debug_assert_eq!(self.slot_buffer.len(), self.source_buffer.len());
                if source != object {
                    // this is not roots, so _source - slot <= _source.size
                    let size = VM::VMObjectModel::get_current_size(source);
                    debug_assert!(
                        slot.to_address().as_usize() - source.to_raw_address().as_usize() <= size
                    );
                }
            }
            let new_object = match self.plan.thread_local_trace_object::<KIND>(
                mutator,
                source,
                Some(slot),
                object,
                self.worker,
            ) {
                Scanned(new_object) => {
                    #[cfg(feature = "debug_publish_object")]
                    if crate::util::metadata::public_bit::is_public(object) {
                        assert!(
                            crate::util::metadata::public_bit::is_public(new_object),
                            "public bit is corrupted. public obj: {} | private new_obj: {} ",
                            object,
                            new_object
                        );
                    }
                    new_object
                }
                ToBeScanned(new_object) => {
                    VM::VMScanning::scan_object(
                        VMWorkerThread(VMThread::UNINITIALIZED),
                        new_object,
                        self,
                    );
                    debug_assert_eq!(self.slot_buffer.len(), self.source_buffer.len());
                    self.plan
                        .thread_local_post_scan_object::<KIND>(mutator, new_object);
                    new_object
                }
            };

            #[cfg(feature = "debug_publish_object")]
            {
                // in a local gc, public objects are not moved, so source is
                // the exact object that needs to be looked at
                if crate::util::metadata::public_bit::is_public(source) {
                    assert!(
                        crate::util::metadata::public_bit::is_public(new_object),
                        "public object: {:?} {:?} points to private object: {:?} {:?}",
                        _source,
                        crate::util::object_extra_header_metadata::get_extra_header_metadata::<
                            VM,
                            usize,
                        >(source)
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
        let mutator = VM::VMActivePlan::mutator(self.tls);

        debug_assert!(self.worker.is_none());

        let new_object = match self.plan.thread_local_trace_object::<KIND>(
            mutator,
            object,
            None,
            object,
            self.worker,
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
                self.plan
                    .thread_local_post_scan_object::<KIND>(mutator, new_object);
                new_object
            }
        };

        self.do_closure();
        new_object
    }

    fn do_object_tracing(&mut self, object: ObjectReference) -> ObjectReference {
        let mutator = VM::VMActivePlan::mutator(self.tls);
        debug_assert!(self.worker.is_none());

        match self.plan.thread_local_trace_object::<KIND>(
            mutator,
            object,
            None,
            object,
            self.worker,
        ) {
            Scanned(new_object) => new_object,
            _ => {
                panic!(
                    "live object: {:?} must have been traced/scanned already",
                    object
                );
            }
        }
    }
}

impl<VM, P, const KIND: TraceKind> SlotVisitor<VM::VMSlot>
    for PlanThreadlocalObjectGraphTraversalClosure<VM, P, KIND>
where
    VM: VMBinding,
    P: PlanThreadlocalTraceObject<VM> + Plan<VM = VM>,
{
    fn visit_slot(&mut self, object: ObjectReference, slot: VM::VMSlot) {
        if slot.load().is_some() {
            self.source_buffer.push(Some(object));
            self.slot_buffer.push(slot);
        }
    }
}

impl<VM, P, const KIND: TraceKind> Drop for PlanThreadlocalObjectGraphTraversalClosure<VM, P, KIND>
where
    VM: VMBinding,
    P: PlanThreadlocalTraceObject<VM> + Plan<VM = VM>,
{
    #[inline(always)]
    fn drop(&mut self) {
        debug_assert!(
            self.slot_buffer.is_empty(),
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
        let mut closure = Closure::new(self.mmtk, self.tls, None, None);
        let mut ready_for_finalize = vec![];
        for mut f in mutator
            .finalizable_candidates
            .drain(0..)
            .collect::<Vec<<VM::VMReferenceGlue as ReferenceGlue<VM>>::FinalizableType>>()
        {
            let reff: ObjectReference = f.get_reference();

            if crate::util::metadata::public_bit::is_public(reff) {
                // public object is untouched, so nothing needs to be done other than adding it back
                // mutator.finalizable_candidates.push(f);
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
        // The following is unsound unless publishing those ready for finalizable objects. The reason is the following:
        // Once a private ready for finalizable object pushed to the global list, its memory might be reclaimed by
        // another local gc, leaving a random pointer in the global list.

        // Now push all local ready for finalize objects to the global list
        // let mut finalizable_processor = self.mmtk.finalizable_processor.lock().unwrap();
        // finalizable_processor.add_ready_for_finalize_objects(ready_for_finalize);

        // Instead of pushing to the global list, push it back to local candidates list
        // this is also semantically correct as local gc cannot trigger finalization
        // only gc thread can do it
        mutator.finalizable_candidates.extend(ready_for_finalize);

        // Finalization thread is expecting a gc thread to wake it up, since
        // local gc is done by the mutator itself, finalization cannot be done after a
        // local gc.

        // VM::VMCollection::schedule_finalization(VMWorkerThread(VMThread::UNINITIALIZED));
    }
}

// pub(crate) static LOCAL_GC_ACTIVE: AtomicBool = AtomicBool::new(false);

pub(crate) static ACTIVE_LOCAL_GC_COUNTER: std::sync::atomic::AtomicU32 =
    std::sync::atomic::AtomicU32::new(0);
pub(crate) static DEFAULT_MAX_CONCURRENT_LOCAL_GC: u32 = 3;
pub(crate) static DEFAULT_MAX_LOCAL_COPY_RESERVE: u8 = 4;
pub(crate) static DEFRAG_MUTATOR_THRESHOLD: usize = 16;
