//! Read/Write barrier implementations.

use std::marker::PhantomData;

use crate::plan::tracing::MarkingObjectPublicWithAssertClosure;
use crate::util::public_bit::{is_public, set_public_bit};
use crate::util::request_statistics::Statistics;
use crate::vm::edge_shape::{Edge, MemorySlice};
use crate::vm::ObjectModel;
use crate::{
    util::{metadata::MetadataSpec, *},
    vm::ActivePlan,
    vm::Scanning,
    vm::VMBinding,
};
use atomic::Ordering;
use downcast_rs::Downcast;

use super::tracing::MarkingObjectPublicClosure;

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
    PublicObjectMarkingBarrier,
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

    fn reset_statistics(&mut self) {}

    fn update_statistics(&mut self, _s: Statistics) {}

    fn report_statistics(&self) -> Statistics {
        Statistics::new(0, 0)
    }
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

    /// Slow-path call for mempry slice copy operations. For example, array-copy operations.
    fn memory_region_copy_slow(
        &mut self,
        src: <Self::VM as VMBinding>::VMMemorySlice,
        dst: <Self::VM as VMBinding>::VMMemorySlice,
    );

    fn current_mutator(&self) -> VMMutatorThread {
        VMMutatorThread(VMThread::UNINITIALIZED)
    }

    fn report_statistics(&self) -> (u32, usize) {
        (0, 0)
    }

    fn reset_statistics(&mut self) {}

    fn is_statistics_valid(&self) -> bool {
        true
    }
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
}

pub struct PublicObjectMarkingBarrier<S: BarrierSemantics> {
    semantics: S,
    statistics: Statistics,
}

impl<S: BarrierSemantics> PublicObjectMarkingBarrier<S> {
    pub fn new(semantics: S) -> Self {
        Self {
            semantics,
            statistics: Statistics::new(0, 0),
        }
    }
}

impl<S: BarrierSemantics> Barrier<S::VM> for PublicObjectMarkingBarrier<S> {
    #[inline(always)]
    fn object_reference_write_pre(
        &mut self,
        src: ObjectReference,
        slot: <S::VM as VMBinding>::VMEdge,
        target: ObjectReference,
    ) {
        assert!(
            self.semantics.is_statistics_valid(),
            "request scope bytes published invalid"
        );
        let mutator = <S::VM as VMBinding>::VMActivePlan::mutator(self.semantics.current_mutator());
        if mutator.in_request {
            self.statistics.write_barrier_counter += 1;
        }
        // target can still be null
        if target.is_null() {
            return;
        }
        // only store private to a public object
        if is_public(src) && !is_public(target) {
            self.object_reference_write_slow(src, slot, target);
            if mutator.in_request {
                self.statistics.write_barrier_slowpath_counter += 1;
                let (publish_counter, bytes_published) = self.semantics.report_statistics();
                self.statistics.write_barrier_publish_counter += publish_counter;
                self.statistics.write_barrier_publish_bytes += bytes_published;
                // clear the stats
                self.semantics.reset_statistics();
            }
        }
    }

    #[inline(always)]
    fn object_reference_write_slow(
        &mut self,
        src: ObjectReference,
        slot: <S::VM as VMBinding>::VMEdge,
        target: ObjectReference,
    ) {
        // debug_assert!(is_public(src), "src object is not public");
        // debug_assert!(!is_public(target), "target object is not private");
        self.semantics
            .object_reference_write_slow(src, slot, target);
    }

    #[inline(always)]
    fn memory_region_copy_post(
        &mut self,
        src: <S::VM as VMBinding>::VMMemorySlice,
        dst: <S::VM as VMBinding>::VMMemorySlice,
    ) {
        self.semantics.memory_region_copy_slow(src, dst);
    }

    fn report_statistics(&self) -> Statistics {
        self.statistics
    }

    fn update_statistics(&mut self, s: Statistics) {
        self.statistics = s;
    }

    fn reset_statistics(&mut self) {
        let mutator = <S::VM as VMBinding>::VMActivePlan::mutator(self.semantics.current_mutator());
        let mutator_id = mutator.mutator_id;
        let request_id = mutator.request_id;
        self.statistics.reset(mutator_id, request_id);
    }
}

pub struct PublicObjectMarkingBarrierSemantics<VM: VMBinding> {
    _request_active: bool,
    _phantom: PhantomData<VM>,
}

impl<VM: VMBinding> PublicObjectMarkingBarrierSemantics<VM> {
    pub fn new() -> Self {
        Self {
            _request_active: false,
            _phantom: PhantomData,
        }
    }

    fn trace_public_object(&mut self, _src: ObjectReference, value: ObjectReference) {
        let mut closure = MarkingObjectPublicClosure::<VM>::new();
        set_public_bit(value);
        VM::VMScanning::scan_object(VMWorkerThread(VMThread::UNINITIALIZED), value, &mut closure);
        closure.do_closure();
    }
}

impl<VM: VMBinding> BarrierSemantics for PublicObjectMarkingBarrierSemantics<VM> {
    type VM = VM;

    fn object_reference_write_slow(
        &mut self,
        src: ObjectReference,
        _slot: VM::VMEdge,
        target: ObjectReference,
    ) {
        self.trace_public_object(src, target)
    }

    fn flush(&mut self) {}

    fn memory_region_copy_slow(&mut self, _src: VM::VMMemorySlice, _dst: VM::VMMemorySlice) {}
}

pub struct PublicObjectMarkingWithAssertBarrierSemantics<VM: VMBinding> {
    mutator_tls: VMMutatorThread,
    _phantom: PhantomData<VM>,
    public_object_size: usize,
    public_object_counter: u32,
}

impl<VM: VMBinding> PublicObjectMarkingWithAssertBarrierSemantics<VM> {
    pub fn new(mutator_tls: VMMutatorThread) -> Self {
        Self {
            mutator_tls,
            _phantom: PhantomData,
            public_object_counter: 0,
            public_object_size: 0,
        }
    }

    fn trace_public_object(&mut self, _src: ObjectReference, value: ObjectReference) {
        let mutator = VM::VMActivePlan::mutator(self.mutator_tls);
        let mutator_id = mutator.mutator_id;
        let mut closure = MarkingObjectPublicWithAssertClosure::<VM>::new(mutator_id);
        assert!(
            mutator_id == crate::util::object_metadata::get_header_object_owner::<VM>(value),
            "public object {:?} escaped, mutator_id: {} <--> {}",
            value,
            mutator_id,
            crate::util::object_metadata::get_header_object_owner::<VM>(value)
        );

        set_public_bit(value);
        VM::VMScanning::scan_object(VMWorkerThread(VMThread::UNINITIALIZED), value, &mut closure);
        let (publish_counter, bytes_published) = closure.do_closure();
        if mutator.in_request {
            self.public_object_counter += publish_counter + 1;
            self.public_object_size += bytes_published + VM::VMObjectModel::get_current_size(value);
        }
    }
}

impl<VM: VMBinding> BarrierSemantics for PublicObjectMarkingWithAssertBarrierSemantics<VM> {
    type VM = VM;

    fn object_reference_write_slow(
        &mut self,
        src: ObjectReference,
        _slot: VM::VMEdge,
        target: ObjectReference,
    ) {
        self.trace_public_object(src, target)
    }

    fn flush(&mut self) {}

    fn memory_region_copy_slow(&mut self, _src: VM::VMMemorySlice, _dst: VM::VMMemorySlice) {}

    fn current_mutator(&self) -> VMMutatorThread {
        self.mutator_tls
    }

    fn report_statistics(&self) -> (u32, usize) {
        (self.public_object_counter, self.public_object_size)
    }

    fn reset_statistics(&mut self) {
        self.public_object_counter = 0;
        self.public_object_size = 0;
    }

    fn is_statistics_valid(&self) -> bool {
        self.public_object_counter == 0 && self.public_object_size == 0
    }
}
