use itertools::Itertools;

use super::global::Immix;
use crate::plan::PlanTraceObject;
use crate::plan::VectorQueue;
use crate::policy::gc_work::TraceKind;
use crate::policy::gc_work::TRACE_KIND_PUBLIC;
use crate::policy::gc_work::TRACE_KIND_TRANSITIVE_PIN;
use crate::scheduler::gc_work::PlanProcessEdges;
use crate::scheduler::thread_local_gc_work::ThreadStackWalker;
use crate::scheduler::GCWork;
use crate::scheduler::GCWorker;
use crate::scheduler::WorkBucketStage;
use crate::util::metadata::public_bit::is_public;
use crate::util::ObjectReference;
use crate::util::VMMutatorThread;
use crate::vm::slot::Slot;
use crate::Mutator;
use crate::ObjectQueue;
use crate::Plan;
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

pub(super) struct CreateProcessRemsetWorks<VM, P>
where
    VM: VMBinding,
    P: crate::Plan<VM = VM> + crate::plan::PlanTraceObject<VM>,
{
    _p: PhantomData<(VM, P)>,
}

impl<VM, P> CreateProcessRemsetWorks<VM, P>
where
    VM: VMBinding,
    P: crate::Plan<VM = VM> + crate::plan::PlanTraceObject<VM>,
{
    pub fn new() -> Self {
        Self { _p: PhantomData }
    }
}

unsafe impl<VM, P> Send for CreateProcessRemsetWorks<VM, P>
where
    VM: VMBinding,
    P: crate::Plan<VM = VM> + crate::plan::PlanTraceObject<VM>,
{
}

impl<VM, P> crate::scheduler::GCWork<VM> for CreateProcessRemsetWorks<VM, P>
where
    VM: VMBinding,
    P: crate::Plan<VM = VM> + crate::plan::PlanTraceObject<VM>,
{
    fn do_work(
        &mut self,
        worker: &mut crate::scheduler::GCWorker<VM>,
        _mmtk: &'static crate::MMTK<VM>,
    ) {
        use crate::scheduler::WorkBucketStage;
        use crate::vm::ActivePlan;

        for mutator in <VM as VMBinding>::VMActivePlan::mutators() {
            worker.scheduler().work_buckets[WorkBucketStage::Prepare]
                .add(CreateProcessRemsetWork::<VM, P>::new(mutator.mutator_tls));
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

pub struct ProcessSlotRemset<VM: VMBinding, P: Plan<VM = VM> + PlanTraceObject<VM>> {
    plan: &'static P,
    slots: Option<Vec<VM::VMSlot>>,
    // recursively generated objects
    next_slots: VectorQueue<VM::VMSlot>,
    worker: *mut GCWorker<VM>,
    #[cfg(debug_assertions)]
    mutator_id: u32,
}

unsafe impl<VM: VMBinding, P: Plan<VM = VM> + PlanTraceObject<VM>> Send
    for ProcessSlotRemset<VM, P>
{
}

impl<VM: VMBinding, P: Plan<VM = VM> + PlanTraceObject<VM>> ProcessSlotRemset<VM, P> {
    const BUFFER_SIZE: usize = 1024;

    pub fn new(
        slots: Vec<VM::VMSlot>,
        #[cfg(debug_assertions)] mutator_id: u32,
        mmtk: &'static MMTK<VM>,
    ) -> Self {
        let plan = mmtk.get_plan().downcast_ref::<P>().unwrap();
        // crate::NUM_CONCURRENT_TRACING_PACKETS.fetch_add(1, Ordering::SeqCst);

        Self {
            plan,
            slots: Some(slots),
            next_slots: VectorQueue::default(),
            worker: std::ptr::null_mut(),
            #[cfg(debug_assertions)]
            mutator_id,
        }
    }

    pub fn worker(&self) -> &'static mut GCWorker<VM> {
        debug_assert_ne!(self.worker, std::ptr::null_mut());
        unsafe { &mut *self.worker }
    }

    #[cold]
    fn flush(&mut self) {
        if !self.next_slots.is_empty() {
            let slots = self.next_slots.take();

            let worker = self.worker();
            let w = Self::new(
                slots,
                #[cfg(debug_assertions)]
                self.mutator_id,
                worker.mmtk,
            );
            worker.add_work(WorkBucketStage::Closure, w);
        }
    }

    fn trace_object(&mut self, object: ObjectReference) -> ObjectReference {
        self.plan
            .trace_object::<Self, { TRACE_KIND_PUBLIC }>(self, object, self.worker())
    }

    fn scan_and_enqueue(&mut self, object: ObjectReference) {
        object.iterate_fields::<VM, _>(|_o, s| {
            if s.load().is_none() {
                return;
            }
            self.next_slots.push(s);
            if self.next_slots.len() > Self::BUFFER_SIZE {
                self.flush();
            }
        });
        self.plan.post_scan_object(object);
    }

    fn process_slots(&mut self, slots: &[VM::VMSlot]) {
        for slot in slots.iter() {
            if let Some(object) = slot.load() {
                // slots may contain private objects as public object might be overwritten by
                // some other private object, so need to exclude those private ones
                if is_public(object) {
                    let new_object = self.trace_object(object);
                    slot.store(new_object);
                }
            }
        }
    }
}

impl<VM: VMBinding, P: Plan<VM = VM> + PlanTraceObject<VM>> GCWork<VM>
    for ProcessSlotRemset<VM, P>
{
    fn do_work(&mut self, worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        self.worker = worker;
        // trace objects
        if let Some(slots) = self.slots.take() {
            self.process_slots(&slots);
        }

        let mut next_slots = vec![];
        while !self.next_slots.is_empty() {
            next_slots.clear();
            self.next_slots.swap(&mut next_slots);
            self.process_slots(&next_slots);
        }
        self.flush();
    }
}

impl<VM: VMBinding, P: Plan<VM = VM> + PlanTraceObject<VM>> ObjectQueue
    for ProcessSlotRemset<VM, P>
{
    fn enqueue(&mut self, object: ObjectReference) {
        debug_assert!(
            object.to_raw_address().is_mapped(),
            "Invalid obj {:?}: address is not mapped",
            object
        );
        // assert!(is_public(object));
        self.scan_and_enqueue(object);
    }
}
struct ProcessObjectRemset<VM: VMBinding, P: Plan<VM = VM> + PlanTraceObject<VM>> {
    plan: &'static P,
    objects: Option<Vec<ObjectReference>>,
    // slots discovered while scanning the objects above. Forwarding them is
    // `ProcessSlotRemset`'s job, so this packet only ever hands them over
    next_slots: VectorQueue<VM::VMSlot>,
    worker: *mut GCWorker<VM>,
    #[cfg(debug_assertions)]
    mutator_id: u32,
}

unsafe impl<VM: VMBinding, P: Plan<VM = VM> + PlanTraceObject<VM>> Send
    for ProcessObjectRemset<VM, P>
{
}

impl<VM: VMBinding, P: Plan<VM = VM> + PlanTraceObject<VM>> ProcessObjectRemset<VM, P> {
    const BUFFER_SIZE: usize = 1024;

    // This work packet is only for newly published objects
    // such objects are pinned
    pub fn new(
        objects: Vec<ObjectReference>,
        #[cfg(debug_assertions)] mutator_id: u32,
        mmtk: &'static MMTK<VM>,
    ) -> Self {
        let plan = mmtk.get_plan().downcast_ref::<P>().unwrap();

        Self {
            plan,
            objects: Some(objects),
            next_slots: VectorQueue::default(),
            worker: std::ptr::null_mut(),
            #[cfg(debug_assertions)]
            mutator_id,
        }
    }

    pub fn worker(&self) -> &'static mut GCWorker<VM> {
        debug_assert_ne!(self.worker, std::ptr::null_mut());
        unsafe { &mut *self.worker }
    }

    #[cold]
    fn flush(&mut self) {
        if !self.next_slots.is_empty() {
            let slots = self.next_slots.take();
            let worker = self.worker();
            // Hand the slots over to `ProcessSlotRemset`, which loads each slot,
            // traces the public object it holds and stores the (possibly forwarded)
            // reference back. The rest of the closure is then driven by that packet.
            let w = ProcessSlotRemset::<VM, P>::new(
                slots,
                #[cfg(debug_assertions)]
                self.mutator_id,
                worker.mmtk,
            );
            worker.add_work(WorkBucketStage::Closure, w);
        }
    }

    fn trace_object(&mut self, object: ObjectReference) -> ObjectReference {
        self.plan
            .trace_object::<Self, { TRACE_KIND_PUBLIC }>(self, object, self.worker())
    }

    fn scan_and_enqueue(&mut self, object: ObjectReference) {
        object.iterate_fields::<VM, _>(|_, s| {
            if s.load().is_none() {
                return;
            }
            self.next_slots.push(s);
            if self.next_slots.len() > Self::BUFFER_SIZE {
                self.flush();
            }
        });
        self.plan.post_scan_object(object);
    }

    fn process_objects(&mut self, objects: &[ObjectReference]) {
        for object in objects.iter() {
            // Objects in the object remset are pinned, so tracing never moves them and
            // there is no slot to write back to. Their outgoing references are a
            // different matter: those targets are not necessarily pinned, so the slots
            // collected by `scan_and_enqueue` must still be forwarded.
            let _new_object = self.trace_object(*object);
            debug_assert_eq!(
                _new_object, *object,
                "object: {} in the object remset has been moved",
                *object
            );
        }
    }
}

impl<VM: VMBinding, P: Plan<VM = VM> + PlanTraceObject<VM>> GCWork<VM>
    for ProcessObjectRemset<VM, P>
{
    fn do_work(&mut self, worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        self.worker = worker;
        if let Some(objects) = self.objects.take() {
            self.process_objects(&objects);
        }
        // No local closure to drain: every slot found while scanning the seed objects
        // is handed to `ProcessSlotRemset`, which owns the transitive closure.
        self.flush();
    }
}

impl<VM: VMBinding, P: Plan<VM = VM> + PlanTraceObject<VM>> ObjectQueue
    for ProcessObjectRemset<VM, P>
{
    fn enqueue(&mut self, object: ObjectReference) {
        debug_assert!(
            object.to_raw_address().is_mapped(),
            "Invalid obj {:?}: address is not mapped",
            object
        );
        self.scan_and_enqueue(object);
    }
}

struct CreateProcessRemsetWork<VM, P>
where
    VM: VMBinding,
    P: crate::Plan<VM = VM> + crate::plan::PlanTraceObject<VM>,
{
    tls: VMMutatorThread,
    _p: PhantomData<(VM, P)>,
}

impl<VM, P> CreateProcessRemsetWork<VM, P>
where
    VM: VMBinding,
    P: crate::Plan<VM = VM> + crate::plan::PlanTraceObject<VM>,
{
    pub fn new(tls: VMMutatorThread) -> Self {
        Self {
            tls,
            _p: PhantomData,
        }
    }
}

unsafe impl<VM, P> Send for CreateProcessRemsetWork<VM, P>
where
    VM: VMBinding,
    P: crate::Plan<VM = VM> + crate::plan::PlanTraceObject<VM>,
{
}

impl<VM: VMBinding, P: Plan<VM = VM> + PlanTraceObject<VM>> GCWork<VM>
    for CreateProcessRemsetWork<VM, P>
{
    fn do_work(&mut self, worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        use crate::vm::ActivePlan;

        let mutator = VM::VMActivePlan::mutator(self.tls);
        let mut slots = vec![];
        for o in mutator.slot_remset.iter().filter(|o| !is_public(**o)) {
            o.iterate_fields::<VM, _>(|_, s| {
                if s.load().is_some_and(is_public) {
                    slots.push(s);
                }
            });
        }
        worker.scheduler().work_buckets[WorkBucketStage::Closure].add(
            ProcessSlotRemset::<VM, P>::new(
                slots,
                #[cfg(debug_assertions)]
                mutator.mutator_id,
                _mmtk,
            ),
        );

        let stack_slots = mutator
            .stack_slots
            .drain(..)
            .filter(|s| s.load().is_some_and(is_public))
            .collect_vec();

        // stack slots do not carry over
        worker.scheduler().work_buckets[WorkBucketStage::Closure].add(
            ProcessSlotRemset::<VM, P>::new(
                stack_slots,
                #[cfg(debug_assertions)]
                mutator.mutator_id,
                _mmtk,
            ),
        );

        #[cfg(debug_assertions)]
        {
            use crate::util::metadata::public_bit::is_public;
            // debug_assert!(mutator.object_remset.is_empty());
            for o in mutator.object_remset.iter() {
                debug_assert!(
                    crate::memory_manager::is_pinned(*o),
                    "object: {} is not pinned",
                    *o
                );
                debug_assert!(is_public(*o), "object: {} is not published", *o)
            }
        }

        worker.scheduler().work_buckets[WorkBucketStage::Closure].add(
            ProcessObjectRemset::<VM, P>::new(
                mutator.object_remset.iter().copied().collect_vec(),
                #[cfg(debug_assertions)]
                mutator.mutator_id,
                _mmtk,
            ),
        );
    }
}
