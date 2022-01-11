use super::global::Immix;
use crate::plan::global::CommonPlan;
use crate::policy::space::Space;
use crate::scheduler::gc_work::*;
use crate::util::copy::CopySemantics;
use crate::util::{Address, ObjectReference};
use crate::vm::{ActivePlan, VMBinding};
use crate::MMTK;
use std::ops::{Deref, DerefMut};

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub(in crate::plan) enum TraceKind {
    Fast,
    Defrag,
}

pub(super) struct ImmixProcessEdges<VM: VMBinding, const KIND: TraceKind> {
    // Use a static ref to the specific plan to avoid overhead from dynamic dispatch or
    // downcast for each traced object.
    plan: &'static Immix<VM>,
    base: ProcessEdgesBase<Self>,
}

impl<VM: VMBinding, const KIND: TraceKind> ImmixProcessEdges<VM, KIND> {
    fn immix(&self) -> &'static Immix<VM> {
        self.plan
    }
}

impl<VM: VMBinding, const KIND: TraceKind> ProcessEdgesWork for ImmixProcessEdges<VM, KIND> {
    type VM = VM;

    const OVERWRITE_REFERENCE: bool = crate::policy::immix::DEFRAG;

    fn new(edges: Vec<Address>, roots: bool, mmtk: &'static MMTK<VM>) -> Self {
        let base = ProcessEdgesBase::new(edges, roots, mmtk);
        let plan = base.plan().downcast_ref::<Immix<VM>>().unwrap();
        Self { plan, base }
    }

    #[cold]
    fn flush(&mut self) {
        if self.nodes.is_empty() {
            return;
        }
        let scan_objects_work = crate::policy::immix::ScanObjectsAndMarkLines::<Self>::new(
            self.pop_nodes(),
            false,
            &self.immix().immix_space,
        );
        self.new_scan_work(scan_objects_work);
    }

    /// Trace  and evacuate objects.
    #[inline(always)]
    fn trace_object(&mut self, object: ObjectReference) -> ObjectReference {
        if object.is_null() {
            return object;
        }
        if self.immix().immix_space.in_space(object) {
            if KIND == TraceKind::Fast {
                self.immix().immix_space.fast_trace_object(self, object)
            } else {
                self.immix().immix_space.trace_object(
                    self,
                    object,
                    CopySemantics::DefaultCopy,
                    self.worker(),
                )
            }
        } else {
            self.immix().common.trace_object::<Self>(self, object)
        }
    }

    #[inline]
    fn process_edge(&mut self, slot: Address) {
        let object = unsafe { slot.load::<ObjectReference>() };
        let new_object = self.trace_object(object);
        if KIND == TraceKind::Defrag && Self::OVERWRITE_REFERENCE {
            unsafe { slot.store(new_object) };
        }
    }
}

impl<VM: VMBinding, const KIND: TraceKind> Deref for ImmixProcessEdges<VM, KIND> {
    type Target = ProcessEdgesBase<Self>;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl<VM: VMBinding, const KIND: TraceKind> DerefMut for ImmixProcessEdges<VM, KIND> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}

pub(super) struct ImmixEvacuationProcessEdges<VM: VMBinding> {
    // Use a static ref to the specific plan to avoid overhead from dynamic dispatch or
    // downcast for each traced object.
    plan: &'static Immix<VM>,
    base: ProcessEdgesBase<Self>,
}

impl<VM: VMBinding> ImmixEvacuationProcessEdges<VM> {
    fn immix(&self) -> &'static Immix<VM> {
        self.plan
    }
}

impl<VM: VMBinding> ProcessEdgesWork for ImmixEvacuationProcessEdges<VM> {
    type VM = VM;

    fn new(edges: Vec<Address>, roots: bool, mmtk: &'static MMTK<Self::VM>) -> Self {
        let base = ProcessEdgesBase::new(edges, roots, mmtk);
        let plan = base.plan().downcast_ref::<Immix<VM>>().unwrap();
        Self { plan, base }
    }

    fn trace_object(&mut self, object: ObjectReference) -> ObjectReference {
        if object.is_null() {
            return object;
        }
        if self.immix().immix_space.in_space(object) {
            self.immix().immix_space.evacuation(
                self,
                object,
                CopySemantics::DefaultCopy,
                self.worker(),
            )
        } else {
            self.immix().common.re_trace_object::<Self>(self, object)
        }
    }

    #[inline]
    fn process_edge(&mut self, slot: Address) {
        let object = unsafe { slot.load::<ObjectReference>() };
        let new_object = self.trace_object(object);
        if Self::OVERWRITE_REFERENCE {
            unsafe { slot.store(new_object) };
        }
    }
}

impl<VM: VMBinding> Deref for ImmixEvacuationProcessEdges<VM> {
    type Target = ProcessEdgesBase<Self>;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl<VM: VMBinding> DerefMut for ImmixEvacuationProcessEdges<VM> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}

pub struct Evacuation<VM: VMBinding> {
    p: std::marker::PhantomData<VM>,
}

impl<VM: VMBinding> crate::scheduler::GCWork<VM> for Evacuation<VM> {
    #[inline]
    fn do_work(&mut self, worker: &mut crate::scheduler::GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        // The following needs to be done right before the second round of root scanning
        {
            // objects in other spaces have been traversed and live objects have been marked
            // so need to reset the mark state before the second trace
            #[allow(clippy::cast_ref_to_mut)]
            let plan_mut: &mut CommonPlan<VM> =
                unsafe { &mut *(mmtk.plan.common() as *const _ as *mut _) };
            plan_mut.prepare_for_re_scanning(worker.tls, true);
        }

        #[cfg(feature = "extreme_assertions")]
        crate::util::edge_logger::reset();

        for mutator in VM::VMActivePlan::mutators() {
            mmtk.scheduler.work_buckets[crate::scheduler::WorkBucketStage::CalculateForwarding]
                .add(ScanStackRoot::<ImmixEvacuationProcessEdges<VM>>(mutator));
        }
        mmtk.scheduler.work_buckets[crate::scheduler::WorkBucketStage::CalculateForwarding]
            .add(ScanVMSpecificRoots::<ImmixEvacuationProcessEdges<VM>>::new());
    }
}

impl<VM: VMBinding> Evacuation<VM> {
    pub fn new() -> Self {
        Self {
            p: std::marker::PhantomData,
        }
    }
}

pub(super) struct ImmixGCWorkContext<VM: VMBinding, const KIND: TraceKind>(
    std::marker::PhantomData<VM>,
);
impl<VM: VMBinding, const KIND: TraceKind> crate::scheduler::GCWorkContext
    for ImmixGCWorkContext<VM, KIND>
{
    type VM = VM;
    type PlanType = Immix<VM>;
    type ProcessEdgesWorkType = ImmixProcessEdges<VM, KIND>;
}
