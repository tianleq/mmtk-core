use crate::plan::immix::Pause;
use crate::plan::VectorQueue;
use crate::policy::space::Space;
use crate::scheduler::gc_work::{ScanObjects, SlotOf};
use crate::util::ObjectReference;
use crate::vm::slot::Slot;

use crate::Plan;
use crate::{
    plan::ObjectQueue,
    scheduler::{gc_work::ProcessEdgesBase, GCWork, GCWorker, ProcessEdgesWork, WorkBucketStage},
    vm::*,
    MMTK,
};
use atomic::Ordering;
use std::ops::{Deref, DerefMut};

use super::Immix;

pub struct ConcurrentTraceObjects<VM: VMBinding> {
    plan: &'static Immix<VM>,
    // objects to mark and scan
    objects: Option<Vec<ObjectReference>>,
    // recursively generated objects
    next_objects: VectorQueue<ObjectReference>,
    worker: *mut GCWorker<VM>,
}

impl<VM: VMBinding> ConcurrentTraceObjects<VM> {
    const SATB_BUFFER_SIZE: usize = 8192;

    pub fn new(objects: Vec<ObjectReference>, mmtk: &'static MMTK<VM>) -> Self {
        let plan = mmtk.get_plan().downcast_ref::<Immix<VM>>().unwrap();
        crate::NUM_CONCURRENT_TRACING_PACKETS.fetch_add(1, Ordering::SeqCst);
        Self {
            plan,
            objects: Some(objects),
            next_objects: VectorQueue::default(),
            worker: std::ptr::null_mut(),
        }
    }

    #[cold]
    fn flush(&mut self) {
        if !self.next_objects.is_empty() {
            let objects = self.next_objects.take();
            let worker = GCWorker::<VM>::current();
            debug_assert!(self.plan.concurrent_marking_enabled());
            let w = Self::new(objects, worker.mmtk);
            worker.add_work(WorkBucketStage::Unconstrained, w);
        }
    }

    fn trace_object(&mut self, object: ObjectReference) -> ObjectReference {
        // debug_assert!(object.is_in_any_space(), "Invalid object {:?}", object);
        if self.plan.immix_space.in_space(object) {
            self.plan
                .immix_space
                .trace_object_without_moving(self, object);
        } else {
            self.plan.common().get_los().trace_object(self, object);
        }
        object
    }

    fn trace_objects(&mut self, objects: &[ObjectReference]) {
        for o in objects.iter() {
            self.trace_object(*o);
        }
    }

    fn scan_and_enqueue<const CHECK_REMSET: bool>(&mut self, object: ObjectReference) {
        object.iterate_fields::<VM, _>(|s| {
            let Some(t) = s.load() else {
                return;
            };

            self.next_objects.push(t);
            if self.next_objects.len() > Self::SATB_BUFFER_SIZE {
                self.flush();
            }
        });
    }
}

impl<VM: VMBinding> ObjectQueue for ConcurrentTraceObjects<VM> {
    fn enqueue(&mut self, object: ObjectReference) {
        debug_assert!(
            object.to_raw_address().is_mapped(),
            "Invalid obj {:?}: address is not mapped",
            object
        );
        self.scan_and_enqueue::<false>(object);
    }
}

unsafe impl<VM: VMBinding> Send for ConcurrentTraceObjects<VM> {}

impl<VM: VMBinding> GCWork<VM> for ConcurrentTraceObjects<VM> {
    // fn should_defer(&self) -> bool {
    //     crate::PAUSE_CONCURRENT_MARKING.load(Ordering::SeqCst)
    // }
    fn is_concurrent_marking_work(&self) -> bool {
        true
    }
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        self.worker = worker;
        debug_assert!(!mmtk.scheduler.work_buckets[WorkBucketStage::Initial].is_activated());

        // mark objects
        if let Some(objects) = self.objects.take() {
            self.trace_objects(&objects)
        }
        let pause_opt = self.plan.current_pause();
        if pause_opt == Some(Pause::FinalMark) || pause_opt.is_none() {
            let mut next_objects = vec![];
            while !self.next_objects.is_empty() {
                let pause_opt = self.plan.current_pause();
                if !(pause_opt == Some(Pause::FinalMark) || pause_opt.is_none()) {
                    panic!("should not reach here");
                    // break;
                }
                next_objects.clear();
                self.next_objects.swap(&mut next_objects);
                self.trace_objects(&next_objects);
            }
        }
        self.flush();
        crate::NUM_CONCURRENT_TRACING_PACKETS.fetch_sub(1, Ordering::SeqCst);
    }
}

pub struct ProcessModBufSATB {
    nodes: Option<Vec<ObjectReference>>,
}

impl ProcessModBufSATB {
    pub fn new(nodes: Vec<ObjectReference>) -> Self {
        Self { nodes: Some(nodes) }
    }
}

impl<VM: VMBinding> GCWork<VM> for ProcessModBufSATB {
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        let mut w = if let Some(nodes) = self.nodes.take() {
            if nodes.is_empty() {
                return;
            }

            ConcurrentTraceObjects::new(nodes, mmtk)
        } else {
            return;
        };
        GCWork::do_work(&mut w, worker, mmtk);
    }
}

pub struct ProcessRootEdges<VM: VMBinding> {
    base: ProcessEdgesBase<VM>,
}

impl<VM: VMBinding> ProcessEdgesWork for ProcessRootEdges<VM> {
    type VM = VM;
    type ScanObjectsWorkType = ScanObjects<Self>;
    const OVERWRITE_REFERENCE: bool = false;
    const SCAN_OBJECTS_IMMEDIATELY: bool = true;

    fn new(
        slots: Vec<SlotOf<Self>>,
        roots: bool,
        mmtk: &'static MMTK<VM>,
        bucket: WorkBucketStage,
    ) -> Self {
        debug_assert!(roots);
        let base = ProcessEdgesBase::new(slots, roots, mmtk, bucket);
        Self { base }
    }

    fn trace_object(&mut self, _object: ObjectReference) -> ObjectReference {
        unreachable!()
    }

    fn process_slots(&mut self) {
        let pause = self
            .base
            .plan()
            .downcast_ref::<Immix<VM>>()
            .unwrap()
            .current_pause();
        if !self.slots.is_empty() {
            let mut roots = Vec::with_capacity(Self::CAPACITY);
            let slots = std::mem::take(&mut self.slots);
            for slot in slots {
                if let Some(object) = slot.load() {
                    roots.push(object);
                    if roots.len() == Self::CAPACITY {
                        // create the packet
                        let worker = self.worker();
                        let mmtk = self.mmtk();
                        let w = ConcurrentTraceObjects::new(roots.clone(), mmtk);
                        match pause {
                            Some(pause) => {
                                if pause == Pause::InitialMark {
                                    worker.scheduler().postpone(w);
                                } else if pause == Pause::FinalMark {
                                    worker.add_work(WorkBucketStage::Closure, w);
                                } else {
                                    unreachable!()
                                }
                            }
                            None => unreachable!(),
                        }

                        roots.clear();
                    }
                }
            }
        }
    }

    fn create_scan_work(&self, _nodes: Vec<ObjectReference>) -> Self::ScanObjectsWorkType {
        unimplemented!()
    }
}

impl<VM: VMBinding> Deref for ProcessRootEdges<VM> {
    type Target = ProcessEdgesBase<VM>;
    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl<VM: VMBinding> DerefMut for ProcessRootEdges<VM> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}
