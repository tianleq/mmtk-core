use crate::plan::barriers::BarrierSemantics;
#[cfg(not(feature = "debug_publish_object"))]
use crate::util::metadata::public_bit::set_public_bit;
use crate::{
    plan::PublishObjectClosure,
    util::{ObjectReference, VMThread, VMWorkerThread},
    vm::Scanning,
    vm::VMBinding,
    MMTK,
};

pub struct PublicObjectMarkingBarrierSemantics<VM: VMBinding> {
    mmtk: &'static MMTK<VM>,
    #[cfg(feature = "debug_publish_object")]
    mutator_id: u32,
    #[cfg(feature = "debug_thread_local_gc_copying")]
    tls: VMMutatorThread,
}

impl<VM: VMBinding> PublicObjectMarkingBarrierSemantics<VM> {
    pub fn new(
        mmtk: &'static MMTK<VM>,
        #[cfg(feature = "debug_publish_object")] mutator_id: u32,
        #[cfg(feature = "debug_thread_local_gc_copying")] tls: VMMutatorThread,
    ) -> Self {
        Self {
            mmtk,
            #[cfg(feature = "debug_publish_object")]
            mutator_id,
            #[cfg(feature = "debug_thread_local_gc_copying")]
            tls,
        }
    }

    fn trace_public_object(&mut self, _src: ObjectReference, value: ObjectReference) {
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
        VM::VMScanning::scan_object(VMWorkerThread(VMThread::UNINITIALIZED), value, &mut closure);
        closure.do_closure();

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
        src: ObjectReference,
        _slot: VM::VMSlot,
        target: Option<ObjectReference>,
    ) {
        self.trace_public_object(src, target.unwrap())
    }

    fn flush(&mut self) {}

    fn memory_region_copy_slow(&mut self, _src: VM::VMMemorySlice, _dst: VM::VMMemorySlice) {}

    fn object_array_copy_slow(
        &mut self,
        _src_base: ObjectReference,
        _dst_base: ObjectReference,
        src: <Self::VM as VMBinding>::VMMemorySlice,
        _dst: <Self::VM as VMBinding>::VMMemorySlice,
    ) {
        // publish all objects in the src slice

        use crate::vm::slot::MemorySlice;
        use crate::vm::slot::Slot;
        for slot in src.iter_slots() {
            // info!("array_copy_slow:: slot: {:?}", slot);

            let object = slot.load();
            // although src array is private, it may contain
            // public objects, so need to rule out those public
            // objects
            if let Some(obj) = object {
                use crate::util::metadata::public_bit::is_public;

                if !is_public(obj) {
                    self.trace_public_object(_dst_base, obj)
                }
            }
        }
    }

    #[cfg(all(feature = "debug_publish_object", debug_assertions))]
    fn get_object_owner(&self, _object: ObjectReference) -> u32 {
        self.mmtk.get_plan().get_object_owner(_object).unwrap()
    }
}
