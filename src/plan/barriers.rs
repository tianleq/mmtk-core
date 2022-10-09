//! Read/Write barrier implementations.

use crate::plan::BlockingObjectClosure;
use crate::util::public_bit::is_public;
use crate::vm::edge_shape::{Edge, MemorySlice};
use crate::vm::ObjectModel;
use crate::{
    util::{metadata::MetadataSpec, *},
    vm::Scanning,
    vm::VMBinding,
};
use atomic::Ordering;
use downcast_rs::Downcast;

/// BarrierSelector describes which barrier to use.
///
/// This is used as an *indicator* for each plan to enable the correct barrier.
/// For example, immix can use this selector to enable different barriers for analysis.
///
/// VM bindings may also use this to enable the correct fast-path, if the fast-path is implemented in the binding.
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

/// A barrier is a combination of fast-path behaviour + slow-path semantics.
/// This trait exposes generic barrier interfaces. The implementations will define their
/// own fast-path code and slow-path semantics.
///
/// Normally, a binding will call these generic barrier interfaces (`object_reference_write` and `memory_region_copy`) for subsuming barrier calls.
///
/// If a subsuming barrier cannot be easily deployed due to platform limitations, the binding may chosse to call both `object_reference_write_pre` and `object_reference_write_post`
/// barrier before and after the store operation.
///
/// As a performance optimization, the binding may also choose to port the fast-path to the VM side,
/// and call the slow-path (`object_reference_write_slow`) only if necessary.
pub trait Barrier<VM: VMBinding>: 'static + Send + Downcast {
    fn flush(&mut self) {}

    /// Subsuming barrier for object reference write
    fn object_reference_write(
        &mut self,
        src: ObjectReference,
        slot: VM::VMEdge,
        target: ObjectReference,
    ) {
        self.object_reference_write_pre(src, slot, target);
        slot.store(target);
        self.object_reference_write_post(src, slot, target);
    }

    /// Full pre-barrier for object reference write
    fn object_reference_write_pre(
        &mut self,
        _src: ObjectReference,
        _slot: VM::VMEdge,
        _target: ObjectReference,
    ) {
    }

    /// Full post-barrier for object reference write
    fn object_reference_write_post(
        &mut self,
        _src: ObjectReference,
        _slot: VM::VMEdge,
        _target: ObjectReference,
    ) {
    }

    /// Object reference write slow-path call.
    /// This can be called either before or after the store, depend on the concrete barrier implementation.
    fn object_reference_write_slow(
        &mut self,
        _src: ObjectReference,
        _slot: VM::VMEdge,
        _target: ObjectReference,
    ) {
    }

    /// Object reference read slow-path call.
    /// This can be called either before or after the store, depend on the concrete barrier implementation.
    fn object_reference_read_slow(&mut self, _target: ObjectReference) {}

    /// Subsuming barrier for array copy
    fn memory_region_copy(&mut self, src: VM::VMMemorySlice, dst: VM::VMMemorySlice) {
        self.memory_region_copy_pre(src.clone(), dst.clone());
        VM::VMMemorySlice::copy(&src, &dst);
        self.memory_region_copy_post(src, dst);
    }

    /// Full pre-barrier for array copy
    fn memory_region_copy_pre(&mut self, _src: VM::VMMemorySlice, _dst: VM::VMMemorySlice) {}

    /// Full post-barrier for array copy
    fn memory_region_copy_post(&mut self, _src: VM::VMMemorySlice, _dst: VM::VMMemorySlice) {}

    fn statistics(&self) -> (u32, usize, u32, u32) {
        (0, 0, 0, 0)
    }
    fn reset_statistics(&mut self, _mutator_id: usize) {}
}

impl_downcast!(Barrier<VM> where VM: VMBinding);

/// Empty barrier implementation.
/// For GCs that do not need any barriers
///
/// Note that since NoBarrier noes nothing but the object field write itself, it has no slow-path semantics (i.e. an no-op slow-path).
pub struct NoBarrier;

impl<VM: VMBinding> Barrier<VM> for NoBarrier {}

/// A barrier semantics defines the barrier slow-path behaviour. For example, how an object barrier processes it's modbufs.
/// Specifically, it defines the slow-path call interfaces and a call to flush buffers.
///
/// A barrier is a combination of fast-path behaviour + slow-path semantics.
/// The fast-path code will decide whether to call the slow-path calls.
pub trait BarrierSemantics: 'static + Send {
    type VM: VMBinding;

    const UNLOG_BIT_SPEC: MetadataSpec =
        *<Self::VM as VMBinding>::VMObjectModel::GLOBAL_LOG_BIT_SPEC.as_spec();

    /// Flush thread-local buffers or remembered sets.
    /// Normally this is called by the slow-path implementation whenever the thread-local buffers are full.
    /// This will also be called externally by the VM, when the thread is being destroyed.
    fn flush(&mut self);

    /// Slow-path call for object field write operations.
    fn object_reference_write_slow(
        &mut self,
        src: ObjectReference,
        slot: <Self::VM as VMBinding>::VMEdge,
        target: ObjectReference,
    );

    /// Slow-path call for object field write operations.
    fn object_reference_read_slow(&mut self, _target: ObjectReference) {}

    /// Slow-path call for mempry slice copy operations. For example, array-copy operations.
    fn memory_region_copy_slow(
        &mut self,
        src: <Self::VM as VMBinding>::VMMemorySlice,
        dst: <Self::VM as VMBinding>::VMMemorySlice,
    );
    fn statistics(&self) -> (u32, usize, u32, u32) {
        (0, 0, 0, 0)
    }
    fn reset_statistics(&mut self, _mutator_id: usize) {}
}

/// Generic object barrier with a type argument defining it's slow-path behaviour.
pub struct ObjectBarrier<S: BarrierSemantics> {
    semantics: S,
}

impl<S: BarrierSemantics> ObjectBarrier<S> {
    pub fn new(semantics: S) -> Self {
        Self { semantics }
    }

    /// Attepmt to atomically log an object.
    /// Returns true if the object is not logged previously.
    #[inline(always)]
    fn object_is_unlogged(&self, object: ObjectReference) -> bool {
        unsafe { S::UNLOG_BIT_SPEC.load::<S::VM, u8>(object, None) != 0 }
    }

    /// Attepmt to atomically log an object.
    /// Returns true if the object is not logged previously.
    #[inline(always)]
    fn log_object(&self, object: ObjectReference) -> bool {
        loop {
            let old_value =
                S::UNLOG_BIT_SPEC.load_atomic::<S::VM, u8>(object, None, Ordering::SeqCst);
            if old_value == 0 {
                return false;
            }
            if S::UNLOG_BIT_SPEC
                .compare_exchange_metadata::<S::VM, u8>(
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
}

impl<S: BarrierSemantics> Barrier<S::VM> for ObjectBarrier<S> {
    fn flush(&mut self) {
        self.semantics.flush();
    }

    #[inline(always)]
    fn object_reference_write_post(
        &mut self,
        src: ObjectReference,
        slot: <S::VM as VMBinding>::VMEdge,
        target: ObjectReference,
    ) {
        if self.object_is_unlogged(src) {
            self.object_reference_write_slow(src, slot, target);
        }
    }

    #[inline(always)]
    fn object_reference_write_slow(
        &mut self,
        src: ObjectReference,
        slot: <S::VM as VMBinding>::VMEdge,
        target: ObjectReference,
    ) {
        if self.log_object(src) {
            self.semantics
                .object_reference_write_slow(src, slot, target);
        }
    }

    #[inline(always)]
    fn memory_region_copy_post(
        &mut self,
        src: <S::VM as VMBinding>::VMMemorySlice,
        dst: <S::VM as VMBinding>::VMMemorySlice,
    ) {
        self.semantics.memory_region_copy_slow(src, dst);
    }

    #[inline]
    fn statistics(&self) -> (u32, usize, u32, u32) {
        self.semantics.statistics()
    }
    #[inline]
    fn reset_statistics(&mut self, mutator_id: usize) {
        self.semantics.reset_statistics(mutator_id);
    }
}

pub struct ObjectOwnerBarrier<S: BarrierSemantics> {
    semantics: S,
}

impl<S: BarrierSemantics> ObjectOwnerBarrier<S> {
    pub fn new(semantics: S) -> Self {
        Self { semantics }
    }
}

impl<S: BarrierSemantics> Barrier<S::VM> for ObjectOwnerBarrier<S> {
    fn flush(&mut self) {
        self.semantics.flush();
    }

    #[inline(always)]
    fn object_reference_write_post(
        &mut self,
        _src: ObjectReference,
        _slot: <S::VM as VMBinding>::VMEdge,
        _target: ObjectReference,
    ) {
    }

    #[inline(always)]
    fn object_reference_write_slow(
        &mut self,
        src: ObjectReference,
        slot: <S::VM as VMBinding>::VMEdge,
        target: ObjectReference,
    ) {
        self.semantics
            .object_reference_write_slow(src, slot, target);
    }

    #[inline(always)]
    fn object_reference_read_slow(&mut self, target: ObjectReference) {
        self.semantics.object_reference_read_slow(target);
    }

    #[inline(always)]
    fn memory_region_copy_post(
        &mut self,
        src: <S::VM as VMBinding>::VMMemorySlice,
        dst: <S::VM as VMBinding>::VMMemorySlice,
    ) {
        self.semantics.memory_region_copy_slow(src, dst);
    }

    #[inline]
    fn statistics(&self) -> (u32, usize, u32, u32) {
        self.semantics.statistics()
    }
    #[inline]
    fn reset_statistics(&mut self, mutator_id: usize) {
        self.semantics.reset_statistics(mutator_id);
    }
}

pub struct ObjectOwnerBarrierSemantics<VM: VMBinding> {
    mmtk: &'static crate::MMTK<VM>,
    mutator_id: usize,
    public_object_counter: u32,
    public_object_bytes: usize,
    write_barrier_counter: u32,
    write_barrier_slowpath_counter: u32,
    // public_objects_accessed: std::collections::HashSet<ObjectReference>,
}

impl<VM: VMBinding> ObjectOwnerBarrierSemantics<VM> {
    pub fn new(mmtk: &'static crate::MMTK<VM>) -> Self {
        Self {
            mmtk,
            mutator_id: 0,
            public_object_counter: 0,
            public_object_bytes: 0,
            write_barrier_counter: 0,
            write_barrier_slowpath_counter: 0,
        }
    }

    fn trace_non_local_object(&mut self, object: ObjectReference, value: ObjectReference) {
        self.write_barrier_counter += 1;
        if value.is_null() || is_public(value) {
            return;
        }
        const OWNER_MASK: usize = 0x00000000FFFFFFFF;
        const REQUEST_MASK: usize = 0xFFFFFFFF00000000;
        let owner = Self::get_header_object_owner(object);
        let new_owner = Self::get_header_object_owner(value);
        if self.mutator_id != 0 && new_owner & OWNER_MASK != self.mutator_id {
            // self.print_mutator_stack_trace();
            // println!("****");
            // VM::VMObjectModel::dump_object(object);
            // println!("****");
            // VM::VMObjectModel::dump_object(value);
            // println!(
            //     "target: {:x}, value: {:x}, mutator: {}, target owner: {}, value owner: {}",
            //     object,
            //     value,
            //     self.mutator_id,
            //     owner & OWNER_MASK,
            //     new_owner & OWNER_MASK
            // );
            assert!(false);
        }
        // here request id is embedded in owner, so even objects within the same thread might be public
        // once it is attached to an object allocated in a different request
        if owner != new_owner || is_public(object) || (REQUEST_MASK & owner) == 0 {
            // println!("---- print thread stack begin ----");
            // self.print_mutator_stack_trace();
            // println!("---- print thread stack end ----");
            let mut closure = BlockingObjectClosure::<VM>::new();
            crate::util::public_bit::set_public_bit(value, self.mutator_id, new_owner, false);

            VM::VMScanning::scan_object(
                VMWorkerThread(VMThread::UNINITIALIZED),
                value,
                &mut closure,
            );
            let v = closure.do_closure(self.mutator_id);
            self.public_object_counter += v.0 + 1;
            self.public_object_bytes += v.1 + VM::VMObjectModel::get_current_size(value);
            self.write_barrier_slowpath_counter += 1;
            // self.trace_non_local_object(obj);
            // println!(
            //     "{}, {}",
            //     self.public_object_counter, self.write_barrier_slowpath_counter
            // );
        }
    }
    #[inline(always)]
    fn header_object_owner_address(object: ObjectReference) -> Address {
        const GC_EXTRA_HEADER_OFFSET: usize = 8;
        <VM as crate::vm::VMBinding>::VMObjectModel::object_start_ref(object)
            - GC_EXTRA_HEADER_OFFSET
    }

    /// Get header forwarding pointer for an object
    #[inline(always)]
    fn get_header_object_owner(object: ObjectReference) -> usize {
        unsafe { Self::header_object_owner_address(object).load::<usize>() }
    }
}

impl<VM: VMBinding> BarrierSemantics for ObjectOwnerBarrierSemantics<VM> {
    type VM = VM;

    #[cold]
    fn flush(&mut self) {}

    fn object_reference_write_slow(
        &mut self,
        src: ObjectReference,
        _slot: VM::VMEdge,
        target: ObjectReference,
    ) {
        self.trace_non_local_object(src, target)
    }

    fn memory_region_copy_slow(&mut self, _src: VM::VMMemorySlice, dst: VM::VMMemorySlice) {}

    #[inline]
    fn statistics(&self) -> (u32, usize, u32, u32) {
        (
            self.public_object_counter,
            self.public_object_bytes,
            self.write_barrier_counter,
            self.write_barrier_slowpath_counter,
        )
    }

    #[inline]
    fn reset_statistics(&mut self, mutator_id: usize) {
        self.public_object_counter = 0;
        self.public_object_bytes = 0;
        self.write_barrier_counter = 0;
        self.write_barrier_slowpath_counter = 0;
        assert!(
            self.mutator_id == 0 || self.mutator_id == mutator_id,
            "mutator id invalid"
        );
        self.mutator_id = mutator_id;
    }

    fn object_reference_read_slow(&mut self, _target: ObjectReference) {
        // todo!()
    }
}
