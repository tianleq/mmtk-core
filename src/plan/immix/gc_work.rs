use atomic::Ordering;

use super::global::Immix;
use crate::plan::global::CommonPlan;
use crate::policy::space::Space;
use crate::scheduler::{gc_work::*, GCWorker};
use crate::util::copy::CopySemantics;
use crate::util::{Address, ObjectReference, VMThread, VMWorkerThread};
use crate::vm::{ActivePlan, Scanning, VMBinding};
use crate::{Plan, TransitiveClosure, MMTK};
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
        if self.roots && !new_object.is_null() {
            self.plan.cache_roots(vec![new_object])
        }
        // println!(
        //     "roots: {}, slot: {:?} -> object: {:?}",
        //     self.roots, slot, object
        // );
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

pub(super) struct ImmixEvacuationClosure<VM: VMBinding> {
    // Use a static ref to the specific plan to avoid overhead from dynamic dispatch or
    // downcast for each traced object.
    plan: &'static Immix<VM>,
    mmtk: &'static MMTK<VM>,
    objects: Vec<ObjectReference>,
    // next_objects: Vec<ObjectReference>,
    worker: *mut crate::scheduler::GCWorker<VM>,
}
unsafe impl<VM: VMBinding> Send for ImmixEvacuationClosure<VM> {}

impl<VM: VMBinding> ImmixEvacuationClosure<VM> {
    const CAPACITY: usize = 4096;

    fn new(objects: Vec<ObjectReference>, roots: bool, mmtk: &'static MMTK<VM>) -> Self {
        let plan = mmtk.plan.downcast_ref::<Immix<VM>>().unwrap();
        Self {
            plan,
            mmtk,
            objects,
            worker: std::ptr::null_mut(),
        }
    }

    // #[inline(always)]
    // fn worker(&self) -> &mut crate::scheduler::GCWorker<VM> {
    //     unsafe { &mut *self.worker }
    // }

    pub fn set_worker(&mut self, worker: &mut GCWorker<VM>) {
        self.worker = worker;
    }

    fn trace_object(&mut self, object: ObjectReference) -> ObjectReference {
        if object.is_null() {
            return object;
        }
        if self.plan.immix_space.in_space(object) {
            let worker = unsafe { &mut *self.worker };
            self.plan
                .immix_space
                .evacuation(self, object, CopySemantics::DefaultCopy, worker)
        } else {
            self.plan.common.re_trace_object::<Self>(self, object)
        }
    }

    fn verify(&self) -> bool {
        return false;
    }
}

impl<VM: VMBinding> TransitiveClosure for ImmixEvacuationClosure<VM> {
    fn process_edge(&mut self, slot: Address) {
        let object = unsafe { slot.load::<ObjectReference>() };
        // println!("slot: {:?} -> object: {:?}", slot, object);
        self.trace_object(object);
    }

    fn process_node(&mut self, object: ObjectReference) {
        if self.objects.is_empty() {
            self.objects.reserve(Self::CAPACITY);
        }
        self.objects.push(object);
        // No need to flush this `nodes` local buffer to some global pool.
        // The max length of `nodes` buffer is equal to `CAPACITY` (when every edge produces a node)
        // So maximum 1 `ScanObjects` work can be created from `nodes` buffer
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

        // for mutator in VM::VMActivePlan::mutators() {
        //     mmtk.scheduler.work_buckets[crate::scheduler::WorkBucketStage::CalculateForwarding]
        //         .add(ScanStackRoot::<ImmixEvacuationProcessEdges<VM>>(mutator));
        // }
        let mut success = false;
        let mut update_copy_state = false;
        let mut cached_roots: Vec<ObjectReference> = vec![];
        let remember_set = mmtk
            .plan
            .downcast_ref::<Immix<VM>>()
            .unwrap()
            .remember_set
            .lock()
            .unwrap();
        for r in remember_set.iter() {
            cached_roots.push(*r);
        }
        for _ in 0..super::MAX_RETRY_TRANSACTION {
            // prepare/reset state

            let mut transitive_closure =
                ImmixEvacuationClosure::new(cached_roots.clone(), true, mmtk);
            transitive_closure.set_worker(worker);

            while !transitive_closure.objects.is_empty() {
                let obj = transitive_closure.objects.pop().unwrap();
                <VM as VMBinding>::VMScanning::scan_object(
                    &mut transitive_closure,
                    obj,
                    VMWorkerThread(VMThread::UNINITIALIZED),
                );
            }
            transitive_closure
                .plan
                .immix_space
                .evacuation_sets
                .lock()
                .unwrap()
                .extend(remember_set.iter());
            //
            let plan = VM::VMActivePlan::global()
                .downcast_ref::<Immix<VM>>()
                .unwrap();
            if update_copy_state {
                plan.copy_state.fetch_add(1, atomic::Ordering::SeqCst);
            }
            // set the flag threads will not be created or destroyed
            VM::VMActivePlan::request_safepoint();

            // need to somehow to make sure threads cannot be created or exit
            for mutator in VM::VMActivePlan::mutators() {
                // wait until all mutator threads acknowledge the update
                while mutator.copy_state != plan.copy_state.load(Ordering::SeqCst) {}
            }
            unsafe {
                let status = std::arch::x86_64::_xbegin();
                if status == std::arch::x86_64::_XBEGIN_STARTED {
                    let passed = transitive_closure.verify();
                    if !passed {
                        std::arch::x86_64::_xend();
                        // rtmLogPrefix();
                        // Log.write("transactional verification failed, attempt: ");
                        // Log.write(i);
                        // Log.write("/");
                        // Log.writeln(MAX_TRANSACTION_RETRY);
                        // FIXME update collectorCopyState here? Might cause mutator to update roots needlessly
                        update_copy_state = false;
                        continue;
                    }
                    // Need to update inter-region references in the remset
                    // reading cursor inside the transaction makes sure that no new entry is inserted

                    // trace.flushRemsetUnsync();
                    // trace.updateInterRegionReferences();

                    // XXX STATE EVEN
                    plan.copy_state.fetch_add(1, atomic::Ordering::SeqCst);
                    std::arch::x86_64::_xend();
                    success = true;
                    break;
                } else {
                    update_copy_state = false;
                }
            }
        }
        if success {
            // reset the flag so threads can be created or destroyed

            // request global safepoint so that mutators can acknowledge the latest update to copy state
            VM::VMActivePlan::request_safepoint();
            let plan = VM::VMActivePlan::global()
                .downcast_ref::<Immix<VM>>()
                .unwrap();
            // need to somehow to make sure threads cannot be created or exit
            for mutator in VM::VMActivePlan::mutators() {
                // wait until all mutator threads acknowledge the update
                while mutator.copy_state != plan.copy_state.load(Ordering::SeqCst) {}
            }
        } else {
            // concurrent copying fail, trigger the stop the world gc
        }
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
