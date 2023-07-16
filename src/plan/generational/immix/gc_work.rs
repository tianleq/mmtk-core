use super::global::GenImmix;
use crate::plan::generational::gc_work::GenNurseryProcessEdges;
use crate::policy::gc_work::TraceKind;
use crate::scheduler::gc_work::PlanProcessEdges;
use crate::scheduler::single_thread_gc_work::SingleThreadPlanProcessEdges;
use crate::vm::VMBinding;

pub struct GenImmixNurseryGCWorkContext<VM: VMBinding>(std::marker::PhantomData<VM>);
impl<VM: VMBinding> crate::scheduler::GCWorkContext for GenImmixNurseryGCWorkContext<VM> {
    type VM = VM;
    type PlanType = GenImmix<VM>;
    type ProcessEdgesWorkType = GenNurseryProcessEdges<VM, Self::PlanType>;

    type SingleThreadProcessEdgesWorkType = GenNurseryProcessEdges<VM, Self::PlanType>;

    type ThreadlocalProcessEdgesWorkType = GenNurseryProcessEdges<VM, Self::PlanType>;
}

pub(super) struct GenImmixMatureGCWorkContext<VM: VMBinding, const KIND: TraceKind>(
    std::marker::PhantomData<VM>,
);
impl<VM: VMBinding, const KIND: TraceKind> crate::scheduler::GCWorkContext
    for GenImmixMatureGCWorkContext<VM, KIND>
{
    type VM = VM;
    type PlanType = GenImmix<VM>;
    type ProcessEdgesWorkType = PlanProcessEdges<VM, GenImmix<VM>, KIND>;

    type SingleThreadProcessEdgesWorkType = SingleThreadPlanProcessEdges<VM, GenImmix<VM>, KIND>;

    type ThreadlocalProcessEdgesWorkType = SingleThreadPlanProcessEdges<VM, GenImmix<VM>, KIND>;
}
