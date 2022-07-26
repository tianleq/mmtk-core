//! Read/Write barrier implementations.

use atomic::Ordering;

use crate::scheduler::gc_work::*;
use crate::scheduler::WorkBucketStage;
use crate::util::metadata::load_metadata;
use crate::util::metadata::{compare_exchange_metadata, MetadataSpec};
use crate::util::*;
use crate::vm::VMBinding;
use crate::MMTK;

/// BarrierSelector describes which barrier to use.
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum BarrierSelector {
    NoBarrier,
    ObjectBarrier,
    ObjectOwnerBarrier,
}

impl BarrierSelector {
    pub const fn equals(&self, other: BarrierSelector) -> bool {
        // cast enum to u8 then compare. Otherwise, we cannot do it in a const fn.
        *self as u8 == other as u8
    }
}

/// For field writes in HotSpot, we cannot always get the source object pointer and the field address
pub enum WriteTarget {
    Object(ObjectReference),
    Slot(Address),
}

pub trait Barrier: 'static + Send {
    fn flush(&mut self);
    fn post_write_barrier(&mut self, target: WriteTarget);
    fn pre_write_barrier(&mut self, _target: WriteTarget, new_val: ObjectReference);
}

pub struct NoBarrier;

impl Barrier for NoBarrier {
    fn flush(&mut self) {}
    fn post_write_barrier(&mut self, _target: WriteTarget) {}
    fn pre_write_barrier(&mut self, _target: WriteTarget, _new_val: ObjectReference) {}
}

pub struct ObjectRememberingBarrier<E: ProcessEdgesWork> {
    mmtk: &'static MMTK<E::VM>,
    modbuf: Vec<ObjectReference>,
    /// The metadata used for log bit. Though this allows taking an arbitrary metadata spec,
    /// for this field, 0 means logged, and 1 means unlogged (the same as the vm::object_model::VMGlobalLogBitSpec).
    meta: MetadataSpec,
}

impl<E: ProcessEdgesWork> ObjectRememberingBarrier<E> {
    #[allow(unused)]
    pub fn new(mmtk: &'static MMTK<E::VM>, meta: MetadataSpec) -> Self {
        Self {
            mmtk,
            modbuf: vec![],
            meta,
        }
    }

    /// Attepmt to atomically log an object.
    /// Returns true if the object is not logged previously.
    #[inline(always)]
    fn log_object(&self, object: ObjectReference) -> bool {
        loop {
            let old_value =
                load_metadata::<E::VM>(&self.meta, object, None, Some(Ordering::SeqCst));
            if old_value == 0 {
                return false;
            }
            if compare_exchange_metadata::<E::VM>(
                &self.meta,
                object,
                1,
                0,
                None,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                return true;
            }
        }
    }

    #[inline(always)]
    fn enqueue_node(&mut self, obj: ObjectReference) {
        // If the objecct is unlogged, log it and push it to mod buffer
        if self.log_object(obj) {
            self.modbuf.push(obj);
            if self.modbuf.len() >= E::CAPACITY {
                self.flush();
            }
        }
    }
}

impl<E: ProcessEdgesWork> Barrier for ObjectRememberingBarrier<E> {
    #[cold]
    fn flush(&mut self) {
        let mut modbuf = vec![];
        std::mem::swap(&mut modbuf, &mut self.modbuf);
        debug_assert!(
            !self.mmtk.scheduler.work_buckets[WorkBucketStage::Final].is_activated(),
            "{:?}",
            self as *const _
        );
        if !modbuf.is_empty() {
            self.mmtk.scheduler.work_buckets[WorkBucketStage::Closure]
                .add(ProcessModBuf::<E>::new(modbuf, self.meta));
        }
    }

    #[inline(always)]
    fn post_write_barrier(&mut self, target: WriteTarget) {
        match target {
            WriteTarget::Object(obj) => {
                self.enqueue_node(obj);
            }
            _ => unreachable!(),
        }
    }

    fn pre_write_barrier(&mut self, _target: WriteTarget, _new_val: ObjectReference) {}
}

pub struct ObjectOwnerBarrier<VM: VMBinding> {
    mmtk: &'static MMTK<VM>,
    /// The metadata used for log bit. Though this allows taking an arbitrary metadata spec,
    /// for this field, 0 means logged, and 1 means unlogged (the same as the vm::object_model::VMGlobalLogBitSpec).
    meta: MetadataSpec,
}

impl<VM: VMBinding> ObjectOwnerBarrier<VM> {
    #[allow(unused)]
    pub fn new(mmtk: &'static MMTK<VM>, meta: MetadataSpec) -> Self {
        Self { mmtk, meta }
    }

    /// Attepmt to atomically log an object.
    /// Returns true if the object is not logged previously.
    #[inline(always)]
    fn log_object(&self, object: ObjectReference) -> bool {
        loop {
            let old_value = load_metadata::<VM>(&self.meta, object, None, Some(Ordering::SeqCst));
            if old_value == 0 {
                return false;
            }
            if compare_exchange_metadata::<VM>(
                &self.meta,
                object,
                1,
                0,
                None,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                return true;
            }
        }
    }

    fn trace_non_local_object(&mut self, obj: ObjectReference) {
        // If the objecct is unlogged, log it and push it to mod buffer
        if self.log_object(obj) {
            // self.modbuf.push(obj);
            // if self.modbuf.len() >= E::CAPACITY {
            //     self.flush();
            // }
        }
    }

    fn print_mutator_stack_trace(&self, obj: ObjectReference) {
        if self.log_object(obj) {
            // VM::VMActivePlan::print_thread_stack();
        }
    }

    #[inline(always)]
    fn header_object_owner_address(object: ObjectReference) -> Address {
        use crate::vm::ObjectModel;
        const GC_EXTRA_HEADER_BYTES: usize = 8;
        <VM as crate::vm::VMBinding>::VMObjectModel::object_start_ref(object)
            - GC_EXTRA_HEADER_BYTES
    }

    /// Get header forwarding pointer for an object
    #[inline(always)]
    fn get_header_object_owner(object: ObjectReference) -> usize {
        unsafe { Self::header_object_owner_address(object).load::<usize>() }
    }
}

impl<VM: VMBinding> Barrier for ObjectOwnerBarrier<VM> {
    #[cold]
    fn flush(&mut self) {}

    #[inline(always)]
    fn post_write_barrier(&mut self, _target: WriteTarget) {}

    fn pre_write_barrier(&mut self, target: WriteTarget, new_val: ObjectReference) {
        // match target {
        //     WriteTarget::Object(obj) => {
        //         // let owner = Self::get_header_object_owner(obj);
        //         // let new_owner = Self::get_header_object_owner(new_val);
        //         // if owner != new_owner {
        //         //     self.print_mutator_stack_trace(obj);
        //         //     // self.trace_non_local_object(obj);
        //         // }
        //         // self.print_mutator_stack_trace(obj);
        //     }
        //     _ => unreachable!(),
        // }
    }
}
