//! Read/Write barrier implementations.

use std::sync::atomic::AtomicUsize;

use crate::scheduler::gc_work::*;
use crate::scheduler::WorkBucketStage;
use crate::util::metadata::load_metadata;
use crate::util::metadata::{compare_exchange_metadata, MetadataSpec};
use crate::util::*;
use crate::vm::ActivePlan;
use crate::vm::VMBinding;
use crate::MMTK;
use atomic::Ordering;

use crate::vm::ObjectModel;
use crate::vm::Scanning;

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
    fn statistics(&self) -> usize {
        0
    }
    fn reset_statistics(&mut self) {}
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
    public_counter: AtomicUsize,
}

impl<VM: VMBinding> ObjectOwnerBarrier<VM> {
    #[allow(unused)]
    pub fn new(mmtk: &'static MMTK<VM>, meta: MetadataSpec) -> Self {
        Self {
            mmtk,
            meta,
            public_counter: AtomicUsize::new(0),
        }
    }

    fn trace_non_local_object(&mut self, object: ObjectReference, value: ObjectReference) {
        use crate::util::public_bit::is_public;
        // If the objecct is unlogged, log it and do the transitive closure(first time come across the object)
        // if the new value is public, then it must have been reached before and transitive closure
        // is done
        if value.is_null() {
            return;
        }
        let owner = Self::get_header_object_owner(object);
        let new_owner = Self::get_header_object_owner(value);
        // here request id is embedded in owner, so even objects within the same thread might be public
        // once it is attached to an object allocated in a different request
        if owner != new_owner || is_public(object) {
            // println!("---- print thread stack begin ----");
            // self.print_mutator_stack_trace();
            // println!("---- print thread stack end ----");
            let mut closure = BlockingObjectClosure::<VM>::new();
            crate::util::public_bit::set_public_bit(value);
            // println!("{:?} is public", value);
            VM::VMScanning::scan_object(
                VMWorkerThread(VMThread::UNINITIALIZED),
                value,
                &mut closure,
            );
            let v = closure.do_closure();
            self.public_counter.fetch_add(v + 1, Ordering::SeqCst);
            // self.trace_non_local_object(obj);
        }
    }

    fn print_mutator_stack_trace(&self) {
        VM::VMActivePlan::print_thread_stack();
    }

    #[inline(always)]
    fn header_object_owner_address(object: ObjectReference) -> Address {
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
        match target {
            WriteTarget::Object(obj) => {
                self.trace_non_local_object(obj, new_val);
            }
            _ => unreachable!(),
        }
    }

    fn statistics(&self) -> usize {
        self.public_counter.load(Ordering::SeqCst)
    }

    fn reset_statistics(&mut self) {
        self.public_counter.store(0, Ordering::SeqCst);
    }
}

struct BlockingObjectClosure<VM: crate::vm::VMBinding> {
    edge_buffer: std::collections::VecDeque<Address>,
    // mark: std::collections::HashSet<ObjectReference>,
    phantom: std::marker::PhantomData<VM>,
}

impl<VM: crate::vm::VMBinding> BlockingObjectClosure<VM> {
    pub fn new() -> Self {
        BlockingObjectClosure {
            edge_buffer: std::collections::VecDeque::new(),
            // mark: std::collections::HashSet::new(),
            phantom: std::marker::PhantomData,
        }
    }

    pub fn do_closure(&mut self) -> usize {
        let mut count = 0;
        while !self.edge_buffer.is_empty() {
            let slot = self.edge_buffer.pop_front().unwrap();
            let object = unsafe { slot.load::<ObjectReference>() };
            if object.is_null() {
                continue;
            }
            if !crate::util::public_bit::is_public(object) {
                // if !self.mark.contains(&object) {
                // set mark bit and public bit on the object
                crate::util::public_bit::set_public_bit(object);
                // crate::util::mark_bit::set_global_mark_bit(object);
                // println!("{:?} is public", object);
                VM::VMScanning::scan_object(
                    crate::util::VMWorkerThread(crate::util::VMThread::UNINITIALIZED),
                    object,
                    self,
                );
                // self.mark.insert(object);
                count += 1;
            }
        }
        count
        // self.mark.clear();
    }
}

impl<VM: crate::vm::VMBinding> crate::vm::EdgeVisitor for BlockingObjectClosure<VM> {
    fn visit_edge(&mut self, edge: Address) {
        self.edge_buffer.push_back(edge);
    }

    fn visit_edge_with_source(&mut self, _source: ObjectReference, _edge: Address) {
        self.edge_buffer.push_back(_edge);
    }
}

impl<VM: crate::vm::VMBinding> Drop for BlockingObjectClosure<VM> {
    #[inline(always)]
    fn drop(&mut self) {
        assert!(
            self.edge_buffer.is_empty(),
            "There are edges left over. Closure is not done correctly."
        );
    }
}
