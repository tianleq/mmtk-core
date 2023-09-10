use super::global::SemiSpace;
use crate::policy::gc_work::DEFAULT_TRACE;
use crate::scheduler::gc_work::PlanProcessEdges;
use crate::scheduler::single_thread_gc_work::SingleThreadPlanProcessEdges;
use crate::vm::VMBinding;

pub struct SSGCWorkContext<VM: VMBinding>(std::marker::PhantomData<VM>);
impl<VM: VMBinding> crate::scheduler::GCWorkContext for SSGCWorkContext<VM> {
    type VM = VM;
    type PlanType = SemiSpace<VM>;
    type ProcessEdgesWorkType = PlanProcessEdges<Self::VM, SemiSpace<VM>, DEFAULT_TRACE>;

    type SingleThreadProcessEdgesWorkType =
        SingleThreadPlanProcessEdges<Self::VM, SemiSpace<VM>, DEFAULT_TRACE>;

    #[cfg(feature = "thread_local_gc")]
    type ThreadlocalProcessEdgesWorkType =
        SingleThreadPlanProcessEdges<Self::VM, SemiSpace<VM>, DEFAULT_TRACE>;
}
