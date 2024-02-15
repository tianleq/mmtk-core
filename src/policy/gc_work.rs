/// Used to identify the trace if a policy has different kinds of traces. For example, defrag vs fast trace for Immix,
/// mark vs forward trace for mark compact.
pub(crate) type TraceKind = u8;

pub const DEFAULT_TRACE: u8 = u8::MAX;

use crate::plan::ObjectQueue;
#[cfg(feature = "thread_local_gc")]
use crate::plan::ThreadlocalTracedObjectType;
use crate::scheduler::GCWorker;
use crate::util::copy::CopySemantics;
use crate::util::ObjectReference;
#[cfg(feature = "thread_local_gc")]
use crate::Mutator;

use crate::vm::VMBinding;

/// This trait defines policy-specific behavior for tracing objects.
/// The procedural macro #[derive(PlanTraceObject)] will generate code
/// that uses this trait. We expect any policy to implement this trait.
/// For the sake of performance, the implementation
/// of this trait should mark methods as `[inline(always)]`.
pub trait PolicyTraceObject<VM: VMBinding> {
    /// Trace object in the policy. If the policy copies objects, we should
    /// expect `copy` to be a `Some` value.
    fn trace_object<Q: ObjectQueue, const KIND: TraceKind>(
        &self,
        queue: &mut Q,
        object: ObjectReference,
        copy: Option<CopySemantics>,
        worker: &mut GCWorker<VM>,
    ) -> ObjectReference;

    /// Policy-specific post-scan-object hook.  It is called after scanning
    /// each object in this space.
    fn post_scan_object(&self, _object: ObjectReference) {
        // Do nothing.
    }

    /// Return whether the policy moves objects.
    fn may_move_objects<const KIND: TraceKind>() -> bool;
}

#[cfg(feature = "thread_local_gc")]
pub trait PolicyThreadlocalTraceObject<VM: VMBinding> {
    #[cfg(not(feature = "debug_publish_object"))]
    /// Trace object in the policy. If the policy copies objects, we should
    /// expect `copy` to be a `Some` value.
    fn thread_local_trace_object<const KIND: TraceKind>(
        &self,
        mutator: &Mutator<VM>,
        object: ObjectReference,
        copy: Option<CopySemantics>,
        worker: &mut GCWorker<VM>,
    ) -> ThreadlocalTracedObjectType;

    #[cfg(feature = "debug_publish_object")]
    /// Trace object in the policy. If the policy copies objects, we should
    /// expect `copy` to be a `Some` value.
    fn thread_local_trace_object<const KIND: TraceKind>(
        &self,
        mutator: &Mutator<VM>,
        source: ObjectReference,
        slot: VM::VMEdge,
        object: ObjectReference,
        copy: Option<CopySemantics>,
        worker: &mut GCWorker<VM>,
    ) -> ThreadlocalTracedObjectType;

    /// Policy-specific post-scan-object hook.  It is called after scanning
    /// each object in this space.
    fn thread_local_post_scan_object(&self, _mutator: &Mutator<VM>, _object: ObjectReference) {
        // Do nothing.
    }

    /// Return whether the policy moves objects.
    fn thread_local_may_move_objects<const KIND: TraceKind>() -> bool;
}
