use super::global::Immix;
use crate::policy::gc_work::TraceKind;
use crate::scheduler::gc_work::PlanProcessEdges;
// #[cfg(feature = "thread_local_gc")]
// use crate::scheduler::thread_local_gc_work::PlanThreadlocalProcessEdges;
use crate::vm::VMBinding;

pub(super) struct ImmixGCWorkContext<VM: VMBinding, const KIND: TraceKind>(
    std::marker::PhantomData<VM>,
);
impl<VM: VMBinding, const KIND: TraceKind> crate::scheduler::GCWorkContext
    for ImmixGCWorkContext<VM, KIND>
{
    type VM = VM;
    type PlanType = Immix<VM>;
    type ProcessEdgesWorkType = PlanProcessEdges<VM, Immix<VM>, KIND>;
}
