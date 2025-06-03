use super::global::Immix;
use crate::policy::gc_work::TraceKind;
use crate::policy::gc_work::TRACE_KIND_TRANSITIVE_PIN;
use crate::scheduler::gc_work::PlanProcessEdges;
#[cfg(feature = "satb")]
use crate::scheduler::gc_work::UnsupportedProcessEdges;
#[cfg(feature = "satb")]
use crate::scheduler::ProcessEdgesWork;
use crate::vm::VMBinding;

pub(super) struct ImmixGCWorkContext<VM: VMBinding, const KIND: TraceKind>(
    std::marker::PhantomData<VM>,
);
impl<VM: VMBinding, const KIND: TraceKind> crate::scheduler::GCWorkContext
    for ImmixGCWorkContext<VM, KIND>
{
    type VM = VM;
    type PlanType = Immix<VM>;
    type DefaultProcessEdges = PlanProcessEdges<VM, Immix<VM>, KIND>;
    type PinningProcessEdges = PlanProcessEdges<VM, Immix<VM>, TRACE_KIND_TRANSITIVE_PIN>;
}

#[cfg(feature = "satb")]
pub(super) struct ConcurrentImmixGCWorkContext<E: ProcessEdgesWork>(std::marker::PhantomData<E>);

#[cfg(feature = "satb")]
impl<E: ProcessEdgesWork> crate::scheduler::GCWorkContext for ConcurrentImmixGCWorkContext<E> {
    type VM = E::VM;
    type PlanType = Immix<E::VM>;
    type DefaultProcessEdges = E;
    type PinningProcessEdges = UnsupportedProcessEdges<Self::VM>;
}
