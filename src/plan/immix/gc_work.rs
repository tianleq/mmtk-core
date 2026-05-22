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
use crate::util::Address;
use crate::util::ObjectReference;
// use crate::util::PINNED_OBJECT_COUNT_IN_GC;
use crate::vm::slot::Slot;
use crate::Mutator;
use crate::ObjectQueue;
use crate::Plan;
use crate::MMTK;
use std::marker::PhantomData;

use crate::vm::VMBinding;

const BUFFER_SIZE: usize = 8192;

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
        use crate::scheduler::WorkBucketStage;
        use crate::vm::slot::Slot;
        use crate::vm::ActivePlan;

        for mutator in <VM as VMBinding>::VMActivePlan::mutators() {
            // Process private objects remset, those objects carried over until the next local GC
            {
                let mut sources = Vec::with_capacity(BUFFER_SIZE);
                let mut slots = Vec::with_capacity(BUFFER_SIZE);
                for object in mutator
                    .source_object_remset
                    .iter()
                    .unique()
                    .copied()
                    .filter(|o| !is_public(*o))
                {
                    object.iterate_fields::<VM, _>(|_o, s| {
                        if s.load().is_none() {
                            return;
                        }
                        sources.push(object);
                        slots.push(s);
                    });
                }
                worker.scheduler().work_buckets[WorkBucketStage::Closure].add(ProcessSlotRemset::<
                    VM,
                    P,
                >::new(
                    Some(sources),
                    slots,
                    #[cfg(debug_assertions)]
                    mutator.mutator_id,
                    _mmtk,
                ));
            }

            // Process stack slots, since stack slots do not carry over, so can safely drain remset
            {
                let stack_slots = mutator
                    .stack_slots
                    .drain(..)
                    .filter(|s| s.load().is_some_and(is_public))
                    .collect_vec();

                let sources = if cfg!(debug_assertions) {
                    Some(vec![
                        ObjectReference::from_raw_address(unsafe {
                            Address::from_usize(0xFFFFFFFFFFFFFFF0)
                        })
                        .unwrap();
                        stack_slots.len()
                    ])
                } else {
                    None
                };

                // stack slots do not carry over
                worker.scheduler().work_buckets[WorkBucketStage::Closure].add(ProcessSlotRemset::<
                    VM,
                    P,
                >::new(
                    sources,
                    stack_slots,
                    #[cfg(debug_assertions)]
                    mutator.mutator_id,
                    _mmtk,
                ));
            }

            // Process newly published objects, those objects need to be carried over until the next local GC
            {
                #[cfg(debug_assertions)]
                {
                    use crate::util::metadata::public_bit::is_public;
                    // debug_assert!(mutator.object_remset.is_empty());
                    for o in mutator.fresh_public_object_remset.iter() {
                        debug_assert!(
                            crate::memory_manager::is_pinned(*o),
                            "object: {} is not pinned",
                            *o
                        );
                        debug_assert!(is_public(*o), "object: {} is not published", *o)
                    }
                }
                // PINNED_OBJECT_COUNT_IN_GC.fetch_add(
                //     mutator.fresh_public_object_remset.len(),
                //     std::sync::atomic::Ordering::SeqCst,
                // );
                // Process newly published objects
                worker.scheduler().work_buckets[WorkBucketStage::Closure].add(
                    ProcessObjectRemset::<VM, P>::new(
                        mutator
                            .fresh_public_object_remset
                            .iter()
                            .copied()
                            .collect_vec(),
                        #[cfg(debug_assertions)]
                        mutator.mutator_id,
                        _mmtk,
                    ),
                );
            }
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
    sources: Option<Vec<ObjectReference>>,
    slots: Vec<VM::VMSlot>,
    // recursively generated objects
    next_objects: VectorQueue<ObjectReference>,
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
    pub fn new(
        sources: Option<Vec<ObjectReference>>,
        slots: Vec<VM::VMSlot>,
        #[cfg(debug_assertions)] mutator_id: u32,
        mmtk: &'static MMTK<VM>,
    ) -> Self {
        let plan = mmtk.get_plan().downcast_ref::<P>().unwrap();

        Self {
            plan,
            sources,
            slots,
            next_objects: VectorQueue::default(),
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
            let objects = self.next_objects.take();
            let slots = self.next_slots.take();

            let worker = self.worker();
            let w = Self::new(
                Some(objects),
                slots,
                #[cfg(debug_assertions)]
                self.mutator_id,
                worker.mmtk,
            );
            worker.add_work(WorkBucketStage::Closure, w);
        }
    }

    fn trace_object(
        &mut self,
        source: ObjectReference,
        object: ObjectReference,
    ) -> ObjectReference {
        self.plan
            .trace_object::<Self, { TRACE_KIND_PUBLIC }>(self, source, object, self.worker())
    }

    fn scan_and_enqueue(&mut self, object: ObjectReference) {
        object.iterate_fields::<VM, _>(|_o, s| {
            if s.load().is_none() {
                return;
            }
            self.next_objects.push(object);
            self.next_slots.push(s);
            if self.next_slots.len() > BUFFER_SIZE {
                self.flush();
            }
        });
        self.plan.post_scan_object(object);
    }

    fn process_slots(&mut self, sources: &[ObjectReference], slots: &[VM::VMSlot]) {
        debug_assert_eq!(sources.len(), slots.len());
        for (source, slot) in sources.iter().zip(slots.iter()) {
            if let Some(object) = slot.load() {
                // slots may contain private objects as public object might be overwritten by
                // some other private object, so need to exclude those private ones
                if is_public(object) {
                    let new_object = self.trace_object(*source, object);
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

        let objects = if self.sources.is_some() {
            self.sources.take().unwrap()
        } else {
            vec![
                ObjectReference::from_raw_address(unsafe {
                    Address::from_usize(0xFFFFFFFFFFFFFFF0)
                })
                .unwrap();
                self.slots.len()
            ]
        };
        let mut slots = vec![];
        std::mem::swap(&mut slots, &mut self.slots);
        // trace objects
        self.process_slots(&objects, &slots);

        let mut next_objects = vec![];
        let mut next_slots = vec![];
        while !self.next_slots.is_empty() {
            next_slots.clear();
            next_objects.clear();
            self.next_slots.swap(&mut next_slots);
            self.next_objects.swap(&mut next_objects);
            self.process_slots(&next_objects, &next_slots);
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
        self.scan_and_enqueue(object);
    }
}

pub struct ProcessObjectRemset<VM: VMBinding, P: Plan<VM = VM> + PlanTraceObject<VM>> {
    plan: &'static P,
    objects: Vec<ObjectReference>,
    // recursively generated objects
    next_objects: VectorQueue<ObjectReference>,
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
    pub fn new(
        objects: Vec<ObjectReference>,
        #[cfg(debug_assertions)] mutator_id: u32,
        mmtk: &'static MMTK<VM>,
    ) -> Self {
        let plan = mmtk.get_plan().downcast_ref::<P>().unwrap();

        Self {
            plan,
            objects,
            next_objects: VectorQueue::default(),
            next_slots: VectorQueue::default(),
            worker: std::ptr::null_mut(),
            #[cfg(debug_assertions)]
            mutator_id,
        }
    }

    fn worker(&self) -> &'static mut GCWorker<VM> {
        debug_assert_ne!(self.worker, std::ptr::null_mut());
        unsafe { &mut *self.worker }
    }

    #[cold]
    fn flush(&mut self) {
        if !self.next_slots.is_empty() {
            let objects = self.next_objects.take();
            let slots = self.next_slots.take();
            let worker = self.worker();
            let w: ProcessSlotRemset<VM, P> = ProcessSlotRemset::new(
                Some(objects),
                slots,
                #[cfg(debug_assertions)]
                self.mutator_id,
                worker.mmtk,
            );
            worker.add_work(WorkBucketStage::Closure, w);
        }
    }

    fn trace_object(
        &mut self,
        source: ObjectReference,
        object: ObjectReference,
    ) -> ObjectReference {
        self.plan
            .trace_object::<Self, { TRACE_KIND_PUBLIC }>(self, source, object, self.worker())
    }

    fn scan_and_enqueue(&mut self, object: ObjectReference) {
        object.iterate_fields::<VM, _>(|_o, s| {
            if s.load().is_none() {
                return;
            }
            self.next_objects.push(object);
            self.next_slots.push(s);
            if self.next_slots.len() > BUFFER_SIZE {
                self.flush();
            }
        });
        self.plan.post_scan_object(object);
    }

    fn process_objects(&mut self, objects: &[ObjectReference], slots: &[VM::VMSlot]) {
        debug_assert_eq!(objects.len(), slots.len());
        for (source, slot) in objects.iter().zip(slots.iter()) {
            let object = slot.load().unwrap();
            let new_object = self.trace_object(*source, object);
            // debug_assert_eq!(object, new_object);
            slot.store(new_object);
        }
    }
}

impl<VM: VMBinding, P: Plan<VM = VM> + PlanTraceObject<VM>> GCWork<VM>
    for ProcessObjectRemset<VM, P>
{
    fn do_work(&mut self, worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        self.worker = worker;
        // objects contain those newly published object. They are pinned and
        // needs to be traced in case there exists uncaptured private --> public
        let mut objects = vec![];
        std::mem::swap(&mut objects, &mut self.objects);
        for object in objects {
            debug_assert!(is_public(object));
            let new_object = self.trace_object(object, object);
            debug_assert_eq!(object, new_object);
        }

        let mut next_objects = vec![];
        let mut next_slots = vec![];
        while !self.next_slots.is_empty() {
            next_slots.clear();
            next_objects.clear();
            self.next_slots.swap(&mut next_slots);
            self.next_objects.swap(&mut next_objects);
            self.process_objects(&next_objects, &next_slots);
        }
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
