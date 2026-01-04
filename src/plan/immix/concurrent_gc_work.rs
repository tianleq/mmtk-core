use crate::plan::{PlanTraceObject, VectorQueue};
use crate::policy::gc_work::TRACE_KIND_PUBLIC;
use crate::util::metadata::public_bit::is_public;
use crate::util::ObjectReference;
use crate::vm::slot::Slot;

use crate::Plan;
use crate::{
    plan::ObjectQueue,
    scheduler::{GCWork, GCWorker, WorkBucketStage},
    vm::*,
    MMTK,
};

pub struct ConcurrentTraceObjects<VM: VMBinding, P: Plan<VM = VM> + PlanTraceObject<VM>> {
    plan: &'static P,
    // objects to mark and scan
    objects: Option<Vec<ObjectReference>>,
    // recursively generated objects
    next_objects: VectorQueue<ObjectReference>,
    worker: *mut GCWorker<VM>,
}

impl<VM: VMBinding, P: Plan<VM = VM> + PlanTraceObject<VM>> ConcurrentTraceObjects<VM, P> {
    const SATB_BUFFER_SIZE: usize = 8192;

    pub fn new(objects: Vec<ObjectReference>, mmtk: &'static MMTK<VM>) -> Self {
        let plan = mmtk.get_plan().downcast_ref::<P>().unwrap();
        // crate::NUM_CONCURRENT_TRACING_PACKETS.fetch_add(1, Ordering::SeqCst);
        Self {
            plan,
            objects: Some(objects),
            next_objects: VectorQueue::default(),
            worker: std::ptr::null_mut(),
        }
    }

    pub fn worker(&self) -> &'static mut GCWorker<VM> {
        debug_assert_ne!(self.worker, std::ptr::null_mut());
        unsafe { &mut *self.worker }
    }

    #[cold]
    fn flush(&mut self) {
        if !self.next_objects.is_empty() {
            let objects = self.next_objects.take();
            let worker = self.worker();
            let w = Self::new(objects, worker.mmtk);
            worker.add_work(WorkBucketStage::Closure, w);
        }
    }

    fn trace_object(&mut self, object: ObjectReference) -> ObjectReference {
        self.plan
            .trace_object::<Self, { TRACE_KIND_PUBLIC }>(self, object, object, self.worker())
    }

    fn trace_objects(&mut self, objects: &[ObjectReference]) {
        for o in objects.iter() {
            self.trace_object(*o);
        }
    }

    fn scan_and_enqueue(&mut self, object: ObjectReference) {
        object.iterate_fields::<VM, _>(|s| {
            let Some(t) = s.load() else {
                return;
            };
            self.next_objects.push(t);
            if self.next_objects.len() > Self::SATB_BUFFER_SIZE {
                self.flush();
            }
        });
        self.plan.post_scan_object(object);
    }
}

impl<VM: VMBinding, P: Plan<VM = VM> + PlanTraceObject<VM>> ObjectQueue
    for ConcurrentTraceObjects<VM, P>
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

unsafe impl<VM: VMBinding, P: Plan<VM = VM> + PlanTraceObject<VM>> Send
    for ConcurrentTraceObjects<VM, P>
{
}

impl<VM: VMBinding, P: Plan<VM = VM> + PlanTraceObject<VM>> GCWork<VM>
    for ConcurrentTraceObjects<VM, P>
{
    fn do_work(&mut self, worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        self.worker = worker;
        // mark objects
        if let Some(objects) = self.objects.take() {
            self.trace_objects(&objects)
        }
        // let pause_opt = self.plan.current_pause();
        // if pause_opt == Some(Pause::FinalMark) || pause_opt.is_none() {
        //     let mut next_objects = vec![];
        //     while !self.next_objects.is_empty() {
        //         let pause_opt = self.plan.current_pause();
        //         if !(pause_opt == Some(Pause::FinalMark) || pause_opt.is_none()) {
        //             break;
        //         }
        //         next_objects.clear();
        //         self.next_objects.swap(&mut next_objects);
        //         self.trace_objects(&next_objects);
        //     }
        // }
        let mut next_objects = vec![];
        while !self.next_objects.is_empty() {
            next_objects.clear();
            self.next_objects.swap(&mut next_objects);
            self.trace_objects(&next_objects);
        }
        self.flush();
        // crate::NUM_CONCURRENT_TRACING_PACKETS.fetch_sub(1, Ordering::SeqCst);
    }
}

// pub struct ProcessModBufSATB<VM: VMBinding, P: Plan<VM = VM> + PlanTraceObject<VM>> {
//     nodes: Option<Vec<ObjectReference>>,
//     _p: std::marker::PhantomData<(VM, P)>,
// }

// unsafe impl<VM: VMBinding, P: Plan<VM = VM> + PlanTraceObject<VM>> Send
//     for ProcessModBufSATB<VM, P>
// {
// }

// impl<VM: VMBinding, P: Plan<VM = VM> + PlanTraceObject<VM>> ProcessModBufSATB<VM, P> {
//     pub fn new(nodes: Vec<ObjectReference>) -> Self {
//         #[cfg(debug_assertions)]
//         {
//             use crate::util::metadata::public_bit::is_public;

//             assert!(nodes.iter().all(|o| is_public(*o)));
//         }
//         Self {
//             nodes: Some(nodes),
//             _p: std::marker::PhantomData,
//         }
//     }
// }

// impl<VM: VMBinding, P: Plan<VM = VM> + PlanTraceObject<VM>> GCWork<VM>
//     for ProcessModBufSATB<VM, P>
// {
//     fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
//         let mut w = if let Some(nodes) = self.nodes.take() {
//             if nodes.is_empty() {
//                 return;
//             }
//             #[cfg(debug_assertions)]
//             {
//                 use crate::util::metadata::public_bit::is_public;

//                 debug_assert!(nodes.iter().all(|object| is_public(*object)));
//             }
//             ConcurrentTraceObjects::<VM, P>::new(nodes, mmtk)
//         } else {
//             return;
//         };
//         GCWork::do_work(&mut w, worker, mmtk);
//     }
// }

pub struct ProcessSlotRemset<VM: VMBinding, P: Plan<VM = VM> + PlanTraceObject<VM>> {
    plan: &'static P,
    objects: Option<Vec<ObjectReference>>,
    slots: Option<Vec<VM::VMSlot>>,
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
    const BUFFER_SIZE: usize = 8192;

    pub fn new(
        objects: Vec<ObjectReference>,
        slots: Vec<VM::VMSlot>,
        #[cfg(debug_assertions)] mutator_id: u32,
        mmtk: &'static MMTK<VM>,
    ) -> Self {
        let plan = mmtk.get_plan().downcast_ref::<P>().unwrap();
        // crate::NUM_CONCURRENT_TRACING_PACKETS.fetch_add(1, Ordering::SeqCst);

        Self {
            plan,
            objects: Some(objects),
            slots: Some(slots),
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
                objects,
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
        object.iterate_fields::<VM, _>(|s| {
            if s.load().is_none() {
                return;
            }
            self.next_objects.push(object);
            self.next_slots.push(s);
            if self.next_slots.len() > Self::BUFFER_SIZE {
                self.flush();
            }
        });
        self.plan.post_scan_object(object);
    }

    fn process_slots(&mut self, sources: &[ObjectReference], slots: &[VM::VMSlot]) {
        debug_assert_eq!(sources.len(), slots.len());
        for (source, slot) in sources.iter().zip(slots.iter()) {
            if let Some(object) = slot.load() {
                // #[cfg(debug_assertions)]
                // {
                //     let description = if is_public(object) {
                //         "public"
                //     } else {
                //         "private"
                //     };
                //     println!(
                //         "ProcessSlotRemset | slot: {:?}, {} object: {:?}",
                //         slot, description, object
                //     );
                // }

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
        // trace objects
        if let (Some(objects), Some(slots)) = (self.objects.take(), self.slots.take()) {
            self.process_slots(&objects, &slots);
        }

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
    objects: Option<Vec<ObjectReference>>,
    slots: Option<Vec<VM::VMSlot>>,
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
    const BUFFER_SIZE: usize = 8192;

    pub fn new(
        objects: Vec<ObjectReference>,
        #[cfg(debug_assertions)] mutator_id: u32,
        mmtk: &'static MMTK<VM>,
    ) -> Self {
        let plan = mmtk.get_plan().downcast_ref::<P>().unwrap();
        // crate::NUM_CONCURRENT_TRACING_PACKETS.fetch_add(1, Ordering::SeqCst);
        let mut slots = Vec::with_capacity(Self::BUFFER_SIZE);
        let mut sources = Vec::with_capacity(Self::BUFFER_SIZE);
        for object in objects {
            object.iterate_fields::<VM, _>(|s| {
                if s.load().is_none() {
                    return;
                }
                sources.push(object);
                slots.push(s);
            });
        }
        Self {
            plan,
            objects: Some(sources),
            slots: Some(slots),
            next_objects: VectorQueue::default(),
            next_slots: VectorQueue::default(),
            worker: std::ptr::null_mut(),
            #[cfg(debug_assertions)]
            mutator_id,
        }
    }

    pub fn __new(
        objects: Vec<ObjectReference>,
        slots: Vec<VM::VMSlot>,
        #[cfg(debug_assertions)] mutator_id: u32,
        mmtk: &'static MMTK<VM>,
    ) -> Self {
        let plan = mmtk.get_plan().downcast_ref::<P>().unwrap();

        Self {
            plan,
            objects: Some(objects),
            slots: Some(slots),
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
            let w = Self::__new(
                objects,
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
        object.iterate_fields::<VM, _>(|s| {
            if s.load().is_none() {
                return;
            }
            self.next_objects.push(object);
            self.next_slots.push(s);
            if self.next_slots.len() > Self::BUFFER_SIZE {
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
        if let (Some(objects), Some(slots)) = (self.objects.take(), self.slots.take()) {
            self.process_objects(&objects, &slots);
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

// pub struct ProcessRootSlots<VM: VMBinding> {
//     base: ProcessEdgesBase<VM>,
// }

// impl<VM: VMBinding> ProcessEdgesWork for ProcessRootSlots<VM> {
//     type VM = VM;
//     type ScanObjectsWorkType = ScanObjects<Self>;
//     const OVERWRITE_REFERENCE: bool = false;
//     const SCAN_OBJECTS_IMMEDIATELY: bool = true;

//     fn new(
//         slots: Vec<SlotOf<Self>>,
//         roots: bool,
//         mmtk: &'static MMTK<VM>,
//         bucket: WorkBucketStage,
//     ) -> Self {
//         debug_assert!(roots);
//         let base = ProcessEdgesBase::new(slots, roots, mmtk, bucket);
//         Self { base }
//     }

//     fn flush(&mut self) {}

//     fn trace_object(&mut self, _object: ObjectReference) -> ObjectReference {
//         unreachable!()
//     }

//     fn process_slots(&mut self) {
//         let pause = self
//             .base
//             .plan()
//             .downcast_ref::<Immix<VM>>()
//             .unwrap()
//             .current_pause()
//             .unwrap();
//         // No need to scan roots in the final mark
//         if pause == Pause::FinalMark {
//             return;
//         }
//         let mut root_objects = Vec::with_capacity(Self::CAPACITY);
//         if !self.slots.is_empty() {
//             let slots = std::mem::take(&mut self.slots);
//             for slot in slots {
//                 if let Some(object) = slot.load() {
//                     root_objects.push(object);
//                     if root_objects.len() == Self::CAPACITY {
//                         // create the packet
//                         let worker = self.worker();
//                         let mmtk = self.mmtk();
//                         let w = ConcurrentTraceObjects::new(root_objects.clone(), mmtk);

//                         match pause {
//                             Pause::InitialMark => worker.scheduler().postpone(w),
//                             _ => unreachable!(),
//                         }

//                         root_objects.clear();
//                     }
//                 }
//             }
//             if !root_objects.is_empty() {
//                 let worker = self.worker();
//                 let w = ConcurrentTraceObjects::new(root_objects.clone(), self.mmtk());

//                 match pause {
//                     Pause::InitialMark => worker.scheduler().postpone(w),
//                     _ => unreachable!(),
//                 }
//             }
//         }
//     }

//     fn create_scan_work(&self, _nodes: Vec<ObjectReference>) -> Self::ScanObjectsWorkType {
//         unimplemented!()
//     }
// }

// impl<VM: VMBinding> Deref for ProcessRootSlots<VM> {
//     type Target = ProcessEdgesBase<VM>;
//     fn deref(&self) -> &Self::Target {
//         &self.base
//     }
// }

// impl<VM: VMBinding> DerefMut for ProcessRootSlots<VM> {
//     fn deref_mut(&mut self) -> &mut Self::Target {
//         &mut self.base
//     }
// }
