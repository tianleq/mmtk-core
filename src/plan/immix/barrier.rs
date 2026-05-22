use crate::util::metadata::public_bit::set_public_bit;
use crate::{plan::barriers::BarrierSemantics, util::VMMutatorThread};
use crate::{
    plan::PublishObjectClosure,
    util::{ObjectReference, VMThread, VMWorkerThread},
    vm::ActivePlan,
    vm::Scanning,
    vm::VMBinding,
    MMTK,
};

pub struct PublicObjectMarkingBarrierSemantics<VM: VMBinding> {
    mmtk: &'static MMTK<VM>,
    #[cfg(feature = "debug_publish_object")]
    mutator_id: u32,
    tls: VMMutatorThread,
}

impl<VM: VMBinding> PublicObjectMarkingBarrierSemantics<VM> {
    pub fn new(
        mmtk: &'static MMTK<VM>,
        #[cfg(feature = "debug_publish_object")] mutator_id: u32,
        tls: VMMutatorThread,
    ) -> Self {
        Self {
            mmtk,
            #[cfg(feature = "debug_publish_object")]
            mutator_id,
            tls,
        }
    }

    fn update_remset(&self, source: ObjectReference, _slot: VM::VMSlot) {
        // Assumption here is objects published by Non-Java thread are globally reachable
        // So only keep track of objects published by Java thread
        if VM::VMActivePlan::is_mutator(self.tls.0) {
            VM::VMActivePlan::mutator(self.tls)
                .source_object_remset
                .push(source);
        } else {
            // This should not be necessary, non-java thread should be part of VM specific roots
            // so all such public objects should be correctly forwarded if necessary
            panic!("should not reach here, non-java thread: {:?}", self.tls);
        }
    }

    fn trace_public_object(
        &mut self,
        _src: ObjectReference,
        _slot: VM::VMSlot,
        value: ObjectReference,
    ) {
        debug_assert!(
            VM::VMActivePlan::is_mutator(self.tls.0),
            "should not reach here"
        );
        let mut closure = PublishObjectClosure::<VM>::new(
            self.mmtk,
            #[cfg(feature = "debug_publish_object")]
            self.mutator_id,
            #[cfg(feature = "debug_thread_local_gc_copying")]
            self.tls,
        );
        #[cfg(feature = "debug_publish_object")]
        set_public_bit(value, Some(self.mutator_id));
        #[cfg(not(feature = "debug_publish_object"))]
        set_public_bit(value);
        #[cfg(feature = "thread_local_gc")]
        self.mmtk.get_plan().publish_object(
            value,
            #[cfg(feature = "debug_thread_local_gc_copying")]
            self.tls,
        );

        // Assumption here is objects published by Non-Java thread are globally reachable
        // So only keep track of objects published by Java thread
        VM::VMActivePlan::mutator(self.tls)
            .fresh_public_object_remset
            .push(value);

        // pin the object so that even if this object is still pointed by
        // some other private object, that private object will never contain
        // a stale pointer
        crate::memory_manager::pin_object(value);

        VM::VMScanning::scan_object(VMWorkerThread(VMThread::UNINITIALIZED), value, &mut closure);
        closure.do_closure(self.tls);

        #[cfg(feature = "debug_thread_local_gc_copying")]
        {
            use crate::vm::ActivePlan;

            if VM::VMActivePlan::is_mutator(self.tls.0) {
                let mutator = VM::VMActivePlan::mutator(self.tls);
                mutator.stats.bytes_published += VM::VMObjectModel::get_current_size(value);
            }

            let mut guard = GLOBAL_GC_STATISTICS.lock().unwrap();
            guard.bytes_published += VM::VMObjectModel::get_current_size(value);
            guard.live_public_bytes += VM::VMObjectModel::get_current_size(value);
            TOTAL_PU8LISHED_BYTES
                .fetch_add(VM::VMObjectModel::get_current_size(value), Ordering::SeqCst);
        }
    }
}

#[cfg(feature = "public_bit")]
impl<VM: VMBinding> BarrierSemantics for PublicObjectMarkingBarrierSemantics<VM> {
    type VM = VM;

    fn object_reference_write_slow(
        &mut self,
        _src: ObjectReference,
        _slot: VM::VMSlot,
        _target: Option<ObjectReference>,
    ) {
        // self.trace_public_object(src, _slot, target.unwrap())
        panic!("should not reach here");
    }

    fn flush(&mut self) {}

    fn memory_region_copy_slow(&mut self, _src: VM::VMMemorySlice, _dst: VM::VMMemorySlice) {}

    fn object_array_copy_slow(
        &mut self,
        src_base: ObjectReference,
        dst_base: ObjectReference,
        src: <Self::VM as VMBinding>::VMMemorySlice,
        dst: <Self::VM as VMBinding>::VMMemorySlice,
    ) {
        // publish all objects in the src slice

        use crate::util::metadata::public_bit::is_public;
        use crate::vm::slot::MemorySlice;
        use crate::vm::slot::Slot;

        if is_public(dst_base) {
            if !is_public(src_base) {
                for slot in src.iter_slots() {
                    let object = slot.load();
                    // although src array is private, it may contain
                    // public objects, so need to rule out those public
                    // objects
                    if let Some(obj) = object {
                        if !is_public(obj) {
                            self.trace_public_object(dst_base, slot, obj)
                        }
                    }
                }
            }
        } else {
            debug_assert_eq!(src.bytes(), dst.bytes());
            // #[cfg(debug_assertions)]
            // let mut update = false;

            // now we know dst_base is private, but src might still contain public objects,
            // so for any slot that is about to be containing a public objects, it needs to
            // be put into the remset
            let mut slots = dst.iter_slots();
            for s in src.iter_slots() {
                let slot = slots.next().unwrap();
                if let Some(object) = s.load() {
                    if is_public(object) {
                        self.update_remset(dst_base, slot);
                        // #[cfg(debug_assertions)]
                        // {
                        //     update = true;
                        // }
                    }
                }
            }
            // #[cfg(debug_assertions)]
            // {
            //     if update {
            //         use crate::policy::GLOBAL_OBJECT_REMSET;

            //         GLOBAL_OBJECT_REMSET.lock().unwrap().insert(dst_base);
            //     }
            // }
        }
    }

    #[cfg(all(feature = "debug_publish_object", debug_assertions))]
    fn get_object_owner(&self, _object: ObjectReference) -> u32 {
        self.mmtk.get_plan().get_object_owner(_object)
    }

    /// Slow-path call for object field write operations.
    fn object_reference_write_s1(
        &mut self,
        src: ObjectReference,
        slot: <Self::VM as VMBinding>::VMSlot,
        target: Option<ObjectReference>,
    ) {
        use crate::util::metadata::public_bit::is_public;
        // slot might not be valid but it is not used

        let val = target.unwrap();
        debug_assert!(is_public(src), "source: {} should be public", src);
        debug_assert!(!is_public(val), "target: {} should be private", val);
        // object publication semantic
        self.trace_public_object(src, slot, val)
    }

    /// Slow-path call for object field write operations.
    fn object_reference_write_s2(
        &mut self,
        src: ObjectReference,
        slot: <Self::VM as VMBinding>::VMSlot,
        target: Option<ObjectReference>,
    ) {
        // private --> public remset semantic

        use crate::{
            util::{constants::BYTES_IN_ADDRESS, metadata::public_bit::is_public},
            vm::slot::Slot,
        };
        debug_assert!(!is_public(src), "source: {} should be private", src);
        debug_assert!(
            is_public(target.unwrap()),
            "target: {} should be public",
            target.unwrap()
        );
        debug_assert!(
            slot.to_address().is_aligned_to(BYTES_IN_ADDRESS),
            "invalid slot: {:?}",
            slot
        );
        // #[cfg(debug_assertions)]
        // {
        //     use crate::policy::GLOBAL_OBJECT_REMSET;

        //     GLOBAL_OBJECT_REMSET.lock().unwrap().insert(src);
        // }
        self.update_remset(src, slot);
    }

    fn object_reference_write_s3(
        &mut self,
        src: ObjectReference,
        _slot: <Self::VM as VMBinding>::VMSlot,
        target: Option<ObjectReference>,
    ) {
        // private --> public remset semantic

        use crate::util::metadata::public_bit::is_public;
        debug_assert!(!is_public(src), "source: {} should be private", src);
        debug_assert!(
            is_public(target.unwrap()),
            "target: {} should be public",
            target.unwrap()
        );

        src.iterate_fields::<VM, _>(|_o, slot| {
            self.update_remset(src, slot);
        });
    }

    fn object_reference_clone_pre(&mut self, obj: ObjectReference) {
        obj.iterate_fields::<VM, _>(|_o, slot| {
            self.update_remset(obj, slot);
        });
    }
}
