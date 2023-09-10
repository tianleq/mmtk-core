use super::MarkSweep;
use crate::policy::gc_work::DEFAULT_TRACE;
use crate::scheduler::gc_work::*;
use crate::scheduler::single_thread_gc_work::SingleThreadPlanProcessEdges;
use crate::vm::VMBinding;

pub struct MSGCWorkContext<VM: VMBinding>(std::marker::PhantomData<VM>);
impl<VM: VMBinding> crate::scheduler::GCWorkContext for MSGCWorkContext<VM> {
    type VM = VM;
    type PlanType = MarkSweep<VM>;
    type ProcessEdgesWorkType = PlanProcessEdges<Self::VM, MarkSweep<VM>, DEFAULT_TRACE>;

    type SingleThreadProcessEdgesWorkType =
        SingleThreadPlanProcessEdges<Self::VM, MarkSweep<VM>, DEFAULT_TRACE>;

    #[cfg(feature = "thread_local_gc")]
    type ThreadlocalProcessEdgesWorkType =
        SingleThreadPlanProcessEdges<Self::VM, MarkSweep<VM>, DEFAULT_TRACE>;
}
