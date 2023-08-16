use super::VMMutatorThread;
use crate::plan::is_nursery_gc;
use crate::scheduler::gc_work::ProcessEdgesWork;
use crate::scheduler::{GCWork, GCWorker};
use crate::util::ObjectReference;
use crate::util::VMWorkerThread;
use crate::vm::Finalizable;
use crate::vm::{ActivePlan, Collection, VMBinding};
use crate::MMTK;
// use std::collections::HashMap;
use std::marker::PhantomData;

/// A special processor for Finalizable objects.
// TODO: we should consider if we want to merge FinalizableProcessor with ReferenceProcessor,
// and treat final reference as a special reference type in ReferenceProcessor.
#[derive(Default)]
pub struct FinalizableProcessor<F: Finalizable> {
    /// Candidate objects that has finalizers with them
    candidates: Vec<(u32, F)>,

    // thread_local_candidates: HashMap<u32, Vec<F>>,
    // thread_local_ready_for_finalize: HashMap<u32, Vec<F>>,
    /// Index into candidates to record where we are up to in the last scan of the candidates.
    /// Index after nursery_index are new objects inserted after the last GC.
    nursery_index: usize,
    /// Objects that can be finalized. They are actually dead, but we keep them alive
    /// until the binding pops them from the queue.
    ready_for_finalize: Vec<(u32, F)>,

    pub local_finalizables_count: u32,
    pub public_finalizables_count: u32,
    pub total_finalizables_count: u32,
}

impl<F: Finalizable> FinalizableProcessor<F> {
    pub fn new() -> Self {
        Self {
            candidates: vec![],

            // thread_local_candidates: HashMap::new(),
            // thread_local_ready_for_finalize: HashMap::new(),
            nursery_index: 0,
            ready_for_finalize: vec![],
            local_finalizables_count: 0,
            public_finalizables_count: 0,
            total_finalizables_count: 0,
        }
    }

    pub fn add(&mut self, mutator_id: u32, object: F) {
        self.candidates.push((mutator_id, object));
        self.total_finalizables_count += 1;
    }

    fn forward_finalizable_reference<E: ProcessEdgesWork>(e: &mut E, finalizable: &mut F) {
        finalizable.keep_alive::<E>(e);
    }

    pub fn scan<E: ProcessEdgesWork>(
        &mut self,
        tls: VMWorkerThread,
        e: &mut E,
        nursery: bool,
        mutator_id: Option<u32>,
    ) {
        let start = if nursery { self.nursery_index } else { 0 };

        // We should go through ready_for_finalize objects and keep them alive.
        // Unlike candidates, those objects are known to be alive. This means
        // theoratically we could do the following loop at any time in a GC (not necessarily after closure phase).
        // But we have to iterate through candidates after closure.
        self.candidates.append(&mut self.ready_for_finalize);
        debug_assert!(self.ready_for_finalize.is_empty());
        let keep_public_alive = mutator_id.is_some();
        for mut f in self.candidates.drain(start..).collect::<Vec<(u32, F)>>() {
            let reff: ObjectReference = f.1.get_reference();
            if let Some(m) = mutator_id {
                if f.0 != m {
                    continue;
                }
            }
            debug_assert!(
                mutator_id.is_none() || mutator_id.unwrap() == f.0,
                "mutator id mismatches for thread local gc"
            );
            trace!("Pop {:?} for finalization", reff);
            if reff.is_live()
                || (keep_public_alive && crate::util::public_bit::is_public::<E::VM>(reff))
            {
                FinalizableProcessor::<F>::forward_finalizable_reference(e, &mut f.1);
                trace!("{:?} is live, push {:?} back to candidates", reff, f);
                self.candidates.push(f);
                continue;
            }

            // We should not at this point mark the object as live. A binding may register an object
            // multiple times with different finalizer methods. If we mark the object as live here, and encounter
            // the same object later in the candidates list (possibly with a different finalizer method),
            // we will erroneously think the object never died, and won't push it to the ready_to_finalize
            // queue.
            // So we simply push the object to the ready_for_finalize queue, and mark them as live objects later.
            self.ready_for_finalize.push(f);
        }

        // Keep the finalizable objects alive.
        self.forward_finalizable(e, nursery);

        // Set nursery_index to the end of the candidates (the candidates before the index are scanned)
        self.nursery_index = self.candidates.len();

        <<E as ProcessEdgesWork>::VM as VMBinding>::VMCollection::schedule_finalization(tls);
    }

    pub fn forward_candidate<E: ProcessEdgesWork>(&mut self, e: &mut E, _nursery: bool) {
        self.candidates
            .iter_mut()
            .for_each(|f| FinalizableProcessor::<F>::forward_finalizable_reference(e, &mut (*f).1));
        e.flush();
    }

    pub fn forward_finalizable<E: ProcessEdgesWork>(&mut self, e: &mut E, _nursery: bool) {
        self.ready_for_finalize
            .iter_mut()
            .for_each(|f| FinalizableProcessor::<F>::forward_finalizable_reference(e, &mut (*f).1));
        e.flush();
    }

    pub fn get_ready_object(&mut self) -> Option<F> {
        if let Some(f) = self.ready_for_finalize.pop() {
            Some(f.1)
        } else {
            Option::None
        }
    }

    pub fn get_all_finalizers(&mut self) -> Vec<F> {
        let mut ret = std::mem::take(&mut self.candidates);
        let ready_objects = std::mem::take(&mut self.ready_for_finalize);
        ret.extend(ready_objects);

        // We removed objects from candidates. Reset nursery_index
        self.nursery_index = 0;

        let mut result = vec![];
        for f in ret {
            result.push(f.1);
        }
        result
    }

    pub fn get_finalizers_for(&mut self, object: ObjectReference) -> Vec<F> {
        // Drain filter for finalizers that equal to 'object':
        // * for elements that equal to 'object', they will be removed from the original vec, and returned.
        // * for elements that do not equal to 'object', they will be left in the original vec.
        // TODO: We should replace this with `vec.drain_filter()` when it is stablized.
        let drain_filter = |vec: &mut Vec<(u32, F)>| -> Vec<F> {
            let mut i = 0;
            let mut ret = vec![];
            while i < vec.len() {
                if vec[i].1.get_reference() == object {
                    let val = vec.remove(i);
                    ret.push(val.1);
                } else {
                    i += 1;
                }
            }
            ret
        };
        let mut ret: Vec<F> = drain_filter(&mut self.candidates);
        ret.extend(drain_filter(&mut self.ready_for_finalize));

        // We removed objects from candidates. Reset nursery_index
        self.nursery_index = 0;

        ret
    }
}

#[derive(Default)]
pub struct Finalization<E: ProcessEdgesWork>(PhantomData<E>, Option<VMMutatorThread>);

impl<E: ProcessEdgesWork> GCWork<E::VM> for Finalization<E> {
    fn do_work(&mut self, worker: &mut GCWorker<E::VM>, mmtk: &'static MMTK<E::VM>) {
        let mut finalizable_processor = mmtk.finalizable_processor.lock().unwrap();
        debug!(
            "Finalization, {} objects in candidates, {} objects ready to finalize",
            finalizable_processor.candidates.len(),
            finalizable_processor.ready_for_finalize.len()
        );
        #[cfg(not(feature = "debug_publish_object"))]
        let mut w = E::new(vec![], false, mmtk, self.1);
        #[cfg(feature = "debug_publish_object")]
        let mut w = E::new(vec![], vec![], false, mmtk, self.1);
        w.set_worker(worker);
        if let Some(tls) = self.1 {
            let mutator_id = <E::VM as VMBinding>::VMActivePlan::mutator(tls).mutator_id;
            finalizable_processor.scan(worker.tls, &mut w, false, Some(mutator_id));
        } else {
            finalizable_processor.scan(
                worker.tls,
                &mut w,
                is_nursery_gc(&*mmtk.plan),
                Option::None,
            );
        }

        debug!(
            "Finished finalization, {} objects in candidates, {} objects ready to finalize",
            finalizable_processor.candidates.len(),
            finalizable_processor.ready_for_finalize.len()
        );
    }
}
impl<E: ProcessEdgesWork> Finalization<E> {
    pub fn new(tls: Option<VMMutatorThread>) -> Self {
        Self(PhantomData, tls)
    }
}

#[derive(Default)]
pub struct ForwardFinalization<E: ProcessEdgesWork>(PhantomData<E>, Option<VMMutatorThread>);

impl<E: ProcessEdgesWork> GCWork<E::VM> for ForwardFinalization<E> {
    fn do_work(&mut self, worker: &mut GCWorker<E::VM>, mmtk: &'static MMTK<E::VM>) {
        trace!("Forward finalization");
        let mut finalizable_processor = mmtk.finalizable_processor.lock().unwrap();
        #[cfg(not(feature = "debug_publish_object"))]
        let mut w = E::new(vec![], false, mmtk, Option::None);
        #[cfg(feature = "debug_publish_object")]
        let mut w = E::new(vec![], vec![], false, mmtk, Option::None);
        w.set_worker(worker);
        finalizable_processor.forward_candidate(&mut w, is_nursery_gc(&*mmtk.plan));

        finalizable_processor.forward_finalizable(&mut w, is_nursery_gc(&*mmtk.plan));
        trace!("Finished forwarding finlizable");
    }
}
impl<E: ProcessEdgesWork> ForwardFinalization<E> {
    pub fn new(tls: Option<VMMutatorThread>) -> Self {
        Self(PhantomData, tls)
    }
}
