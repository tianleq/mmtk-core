use std::sync::atomic::Ordering;

use crate::{
    plan::{barriers::BarrierSemantics, VectorQueue},
    scheduler::WorkBucketStage,
    util::ObjectReference,
    vm::{
        slot::{MemorySlice, Slot},
        VMBinding,
    },
    MMTK,
};

use super::{concurrent_marking::ProcessModBufSATB, Immix, Pause};

pub struct SATBBarrierSemantics<VM: VMBinding> {
    mmtk: &'static MMTK<VM>,
    satb: VectorQueue<ObjectReference>,
    refs: VectorQueue<ObjectReference>,
    immix: &'static Immix<VM>,
}

impl<VM: VMBinding> SATBBarrierSemantics<VM> {
    pub fn new(mmtk: &'static MMTK<VM>) -> Self {
        Self {
            mmtk,
            satb: VectorQueue::default(),
            refs: VectorQueue::default(),
            immix: mmtk.get_plan().downcast_ref::<Immix<VM>>().unwrap(),
        }
    }

    fn slow(&mut self, _src: Option<ObjectReference>, _slot: VM::VMSlot, old: ObjectReference) {
        self.satb.push(old);
        if self.satb.is_full() {
            self.flush_satb();
        }
    }

    fn enqueue_node(
        &mut self,
        src: Option<ObjectReference>,
        slot: VM::VMSlot,
        _new: Option<ObjectReference>,
    ) -> bool {
        if let Some(old) = slot.load() {
            if self.log_object(old) {
                self.slow(src, slot, old);
                return true;
            }
        }
        false
    }

    /// Attempt to atomically log an object.
    /// Returns true if the object is not logged previously.
    fn log_object(&self, object: ObjectReference) -> bool {
        loop {
            let old_value =
                Self::UNLOG_BIT_SPEC.load_atomic::<VM, u8>(object, None, Ordering::SeqCst);
            if old_value == 0 {
                return false;
            }
            if Self::UNLOG_BIT_SPEC
                .compare_exchange_metadata::<VM, u8>(
                    object,
                    1,
                    0,
                    None,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
            {
                return true;
            }
        }
    }

    fn flush_satb(&mut self) {
        if !self.satb.is_empty() {
            if self.should_create_satb_packets() {
                let satb = self.satb.take();
                match self.immix.current_pause() {
                    Some(pause) => {
                        if pause == Pause::FinalMark {
                            self.mmtk.scheduler.work_buckets[WorkBucketStage::Closure]
                                .add(ProcessModBufSATB::new(satb));
                        } else {
                            self.mmtk.scheduler.work_buckets[WorkBucketStage::Unconstrained]
                                .add(ProcessModBufSATB::new(satb));
                        }
                    }
                    None => self.mmtk.scheduler.work_buckets[WorkBucketStage::Unconstrained]
                        .add(ProcessModBufSATB::new(satb)),
                }
            } else {
                let _ = self.satb.take();
            };
        }
    }

    #[cold]
    fn flush_weak_refs(&mut self) {
        if !self.refs.is_empty() {
            debug_assert!(self.should_create_satb_packets());
            let nodes = self.refs.take();
            self.mmtk.scheduler.work_buckets[WorkBucketStage::Unconstrained]
                .add(ProcessModBufSATB::new(nodes));
        }
    }

    fn should_create_satb_packets(&self) -> bool {
        self.immix.concurrent_marking_enabled()
            && (self.immix.concurrent_marking_in_progress()
                || self.immix.current_pause() == Some(Pause::FinalMark))
    }
}

impl<VM: VMBinding> BarrierSemantics for SATBBarrierSemantics<VM> {
    type VM = VM;

    #[cold]
    fn flush(&mut self) {
        self.flush_satb();
        self.flush_weak_refs();
    }

    fn object_reference_write_slow(
        &mut self,
        src: ObjectReference,
        slot: <Self::VM as VMBinding>::VMSlot,
        target: Option<ObjectReference>,
    ) {
        self.enqueue_node(Some(src), slot, target);
    }

    fn memory_region_copy_slow(
        &mut self,
        _src: <Self::VM as VMBinding>::VMMemorySlice,
        dst: <Self::VM as VMBinding>::VMMemorySlice,
    ) {
        for s in dst.iter_slots() {
            self.enqueue_node(None, s, None);
        }
    }

    // fn load_reference(&mut self, o: ObjectReference) {
    //     if !self.immix.concurrent_marking_in_progress() || self.immix.is_marked(o) {
    //         return;
    //     }
    //     self.refs.push(o);
    //     if self.refs.is_full() {
    //         self.flush_weak_refs();
    //     }
    // }

    fn object_probable_write_slow(&mut self, obj: ObjectReference) {
        obj.iterate_fields::<VM, _>(|s| {
            self.enqueue_node(Some(obj), s, None);
        });
    }
}
