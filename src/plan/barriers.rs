//! Read/Write barrier implementations.

use crate::vm::slot::{MemorySlice, Slot};
use crate::vm::ObjectModel;
use crate::{
    util::{metadata::MetadataSpec, *},
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
    /// No barrier is used.
    NoBarrier,
    /// Object remembering barrier is used.
    ObjectBarrier,
    #[cfg(feature = "public_bit")]
    PublicObjectMarkingBarrier,
}

impl BarrierSelector {
    /// A const function to check if two barrier selectors are the same.
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
        slot: VM::VMSlot,
        target: ObjectReference,
    ) {
        self.object_reference_write_pre(src, slot, Some(target));
        slot.store(target);
        self.object_reference_write_post(src, slot, Some(target));
    }

    /// Full pre-barrier for object reference write
    fn object_reference_write_pre(
        &mut self,
        _src: ObjectReference,
        _slot: VM::VMSlot,
        _target: Option<ObjectReference>,
    ) {
    }

    /// Full post-barrier for object reference write
    fn object_reference_write_post(
        &mut self,
        _src: ObjectReference,
        _slot: VM::VMSlot,
        _target: Option<ObjectReference>,
    ) {
    }

    /// Object reference write slow-path call.
    /// This can be called either before or after the store, depend on the concrete barrier implementation.
    fn object_reference_write_slow(
        &mut self,
        _src: ObjectReference,
        _slot: VM::VMSlot,
        _target: Option<ObjectReference>,
    ) {
    }

    /// Full pre-barrier for array copy
    fn object_array_copy_pre(
        &mut self,
        _src_base: ObjectReference,
        _dst_base: ObjectReference,
        _src: VM::VMMemorySlice,
        _dst: VM::VMMemorySlice,
    ) {
    }

    /// Object arraycopy write slow-path call.
    /// This can be called either before or after the store, depend on the concrete barrier implementation.

    fn object_array_copy_slow(
        &mut self,
        _src_base: ObjectReference,
        _dst_base: ObjectReference,
        _src: VM::VMMemorySlice,
        _dst: VM::VMMemorySlice,
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

    /// A pre-barrier indicating that some fields of the object will probably be modified soon.
    /// Specifically, the caller should ensure that:
    ///     * The barrier must called before any field modification.
    ///     * Some fields (unknown at the time of calling this barrier) might be modified soon, without a write barrier.
    ///     * There are no safepoints between the barrier call and the field writes.
    ///
    /// **Example use case for mmtk-openjdk:**
    ///
    /// The OpenJDK C2 slowpath allocation code
    /// can do deoptimization after the allocation and before returning to C2 compiled code.
    /// The deoptimization itself contains a safepoint. For generational plans, if a GC
    /// happens at this safepoint, the allocated object will be promoted, and all the
    /// subsequent field initialization should be recorded.
    ///
    // TODO: Review any potential use cases for other VM bindings.
    fn object_probable_write(&mut self, _obj: ObjectReference) {}
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
        slot: <Self::VM as VMBinding>::VMSlot,
        target: Option<ObjectReference>,
    );

    /// Slow-path call for mempry slice copy operations. For example, array-copy operations.
    fn object_array_copy_slow(
        &mut self,
        src_base: ObjectReference,
        dst_base: ObjectReference,
        src: <Self::VM as VMBinding>::VMMemorySlice,
        dst: <Self::VM as VMBinding>::VMMemorySlice,
    );

    /// Slow-path call for mempry slice copy operations. For example, array-copy operations.
    fn memory_region_copy_slow(
        &mut self,
        src: <Self::VM as VMBinding>::VMMemorySlice,
        dst: <Self::VM as VMBinding>::VMMemorySlice,
    );

    /// Object will probably be modified
    fn object_probable_write_slow(&mut self, _obj: ObjectReference) {}
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
    fn object_is_unlogged(&self, object: ObjectReference) -> bool {
        unsafe { S::UNLOG_BIT_SPEC.load::<S::VM, u8>(object, None) != 0 }
    }

    /// Attepmt to atomically log an object.
    /// Returns true if the object is not logged previously.
    fn log_object(&self, object: ObjectReference) -> bool {
        #[cfg(all(feature = "vo_bit", feature = "extreme_assertions"))]
        debug_assert!(
            crate::util::metadata::vo_bit::is_vo_bit_set::<S::VM>(object),
            "object bit is unset"
        );
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

    fn object_reference_write_post(
        &mut self,
        src: ObjectReference,
        slot: <S::VM as VMBinding>::VMSlot,
        target: Option<ObjectReference>,
    ) {
        if self.object_is_unlogged(src) {
            self.object_reference_write_slow(src, slot, target);
        }
    }

    fn object_reference_write_slow(
        &mut self,
        src: ObjectReference,
        slot: <S::VM as VMBinding>::VMSlot,
        target: Option<ObjectReference>,
    ) {
        if self.log_object(src) {
            self.semantics
                .object_reference_write_slow(src, slot, target);
        }
    }

    fn memory_region_copy_post(
        &mut self,
        src: <S::VM as VMBinding>::VMMemorySlice,
        dst: <S::VM as VMBinding>::VMMemorySlice,
    ) {
        self.semantics.memory_region_copy_slow(src, dst);
    }

    fn object_probable_write(&mut self, obj: ObjectReference) {
        if self.object_is_unlogged(obj) {
            self.semantics.object_probable_write_slow(obj);
        }
    }
}

#[cfg(feature = "public_bit")]
pub struct PublicObjectMarkingBarrier<S: BarrierSemantics> {
    semantics: S,
}

#[cfg(feature = "public_bit")]
impl<S: BarrierSemantics> PublicObjectMarkingBarrier<S> {
    pub fn new(semantics: S) -> Self {
        Self { semantics }
    }
}

#[cfg(feature = "public_bit")]
impl<S: BarrierSemantics> Barrier<S::VM> for PublicObjectMarkingBarrier<S> {
    #[inline(always)]
    fn object_reference_write_pre(
        &mut self,
        src: ObjectReference,
        slot: <S::VM as VMBinding>::VMSlot,
        target: Option<ObjectReference>,
    ) {
        // trace when store private to a public object
        if metadata::public_bit::is_public::<S::VM>(src) {
            if let Some(object) = target {
                if !metadata::public_bit::is_public::<S::VM>(object) {
                    self.object_reference_write_slow(src, slot, target);
                }
            }
        } else {
            #[cfg(all(feature = "debug_publish_object", debug_assertions))]
            {
                // use crate::vm::ActivePlan;
                if target.is_some() && !is_public::<S::VM>(target.unwrap()) {
                    // both source and target are private
                    // they should have the same owner
                    let source_owner = self.semantics.get_object_owner(src);
                    let target_owner = self.semantics.get_object_owner(target);
                    let valid = source_owner == target_owner;
                    if !valid {
                        panic!(
                            "source: {} owner: {}, target: {} owner: {}",
                            src, source_owner, target, target_owner
                        );
                    }
                }
            }
        }
    }

    #[inline(always)]
    fn object_reference_write_slow(
        &mut self,
        src: ObjectReference,
        slot: <S::VM as VMBinding>::VMSlot,
        target: Option<ObjectReference>,
    ) {
        debug_assert!(
            metadata::public_bit::is_public::<S::VM>(src),
            "source check is broken"
        );
        debug_assert!(!target.is_none(), "target null check is broken");
        debug_assert!(
            !metadata::public_bit::is_public::<S::VM>(target.unwrap()),
            "target check is broken"
        );
        #[cfg(feature = "publish_rate_analysis")]
        crate::REQUEST_SCOPE_BARRIER_SLOW_PATH_COUNT
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        self.semantics
            .object_reference_write_slow(src, slot, target);
    }

    #[inline(always)]
    fn object_array_copy_pre(
        &mut self,
        src_base: ObjectReference,
        dst_base: ObjectReference,
        src: <S::VM as VMBinding>::VMMemorySlice,
        dst: <S::VM as VMBinding>::VMMemorySlice,
    ) {
        // Only do publication when the dst array is public and src array is private
        // a private array should not have public object as its elements
        if metadata::public_bit::is_public::<S::VM>(dst_base) {
            if !metadata::public_bit::is_public::<S::VM>(src_base) {
                self.semantics
                    .object_array_copy_slow(src_base, dst_base, src, dst);
            }
        } else {
            #[cfg(all(feature = "debug_publish_object", debug_assertions))]
            {
                let dst_owner = self.semantics.get_object_owner(dst_base);
                let src_owner = self.semantics.get_object_owner(src_base);
                if !is_public::<S::VM>(src_base) {
                    // both src_base and dst_base are private
                    assert!(
                        src_owner == dst_owner,
                        "src base: {} owner: {}, dst base: {} owner: {}",
                        src_base,
                        src_owner,
                        dst_base,
                        dst_owner
                    );
                    // Even if src base is private, it may still contain public objects
                    // so need to rule out public objects
                    for slot in src.iter_slots() {
                        let object = slot.load();
                        if !object.is_null() && !is_public::<S::VM>(object) {
                            let owner = self.semantics.get_object_owner(object);
                            assert!(
                                dst_owner == owner,
                                "dst base: {} owner: {}, src object: {} owner: {}",
                                dst_base,
                                dst_owner,
                                object,
                                owner
                            );
                        }
                    }
                }
            }
        }
    }

    // The following is not being used by openjdk
    #[inline(always)]
    fn object_array_copy_slow(
        &mut self,
        src_base: ObjectReference,
        dst_base: ObjectReference,
        src: <S::VM as VMBinding>::VMMemorySlice,
        dst: <S::VM as VMBinding>::VMMemorySlice,
    ) {
        debug_assert!(
            metadata::public_bit::is_public::<S::VM>(dst_base),
            "arraycopy slow path: destination array: {:?} is private",
            dst_base
        );
        debug_assert!(
            !metadata::public_bit::is_public::<S::VM>(src_base),
            "arraycopy slow path: source array: {:?} is public",
            src_base
        );
        #[cfg(feature = "publish_rate_analysis")]
        crate::REQUEST_SCOPE_BARRIER_SLOW_PATH_COUNT
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.semantics
            .object_array_copy_slow(src_base, dst_base, src, dst);
    }
}

#[cfg(feature = "public_bit")]
pub struct PublicObjectMarkingBarrierSemantics<VM: VMBinding> {
    mmtk: &'static crate::MMTK<VM>,
    #[cfg(feature = "publish_rate_analysis")]
    tls: VMMutatorThread,
}

#[cfg(feature = "public_bit")]
impl<VM: VMBinding> PublicObjectMarkingBarrierSemantics<VM> {
    pub fn new(
        #[cfg(feature = "publish_rate_analysis")] tls: VMMutatorThread,
        mmtk: &'static crate::MMTK<VM>,
    ) -> Self {
        Self { tls, mmtk }
    }

    fn trace_public_object(&mut self, _src: ObjectReference, value: ObjectReference) {
        // use crate::vm::Scanning;

        let mut closure = super::tracing::PublishObjectClosure::<VM>::new(self.mmtk);

        closure.do_closure(value, self.tls);
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
        for slot in src.iter_slots() {
            // info!("array_copy_slow:: slot: {:?}", slot);
            let object = slot.load();
            // although src array is private, it may contain
            // public objects, so need to rule out those public
            // objects
            if !object.is_none() && !metadata::public_bit::is_public::<VM>(object.unwrap()) {
                self.trace_public_object(_dst_base, object.unwrap())
            }
        }
    }

    #[cfg(all(feature = "debug_publish_object", debug_assertions))]
    fn get_object_owner(&self, _object: ObjectReference) -> u32 {
        self.mmtk.get_plan().get_object_owner(_object).unwrap()
    }
}
