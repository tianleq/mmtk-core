//! The fundamental mechanism for performing a transitive closure over an object graph.

use std::mem;

use crate::scheduler::gc_work::ProcessEdgesWork;
use crate::scheduler::{GCWorker, WorkBucketStage};
use crate::util::{Address, ObjectReference};
use crate::vm::EdgeVisitor;

/// This trait is the fundamental mechanism for performing a
/// transitive closure over an object graph.
pub trait TransitiveClosure {
    // The signature of this function changes during the port
    // because the argument `ObjectReference source` is never used in the original version
    // See issue #5
    fn process_node(&mut self, object: ObjectReference);
}

impl<T: ProcessEdgesWork> TransitiveClosure for T {
    #[inline]
    fn process_node(&mut self, object: ObjectReference) {
        ProcessEdgesWork::process_node(self, object);
    }
}

/// A transitive closure visitor to collect all the edges of an object.
pub struct ObjectsClosure<'a, E: ProcessEdgesWork> {
    edge_buffer: Vec<Address>,
    source_buffer: Vec<ObjectReference>,
    worker: &'a mut GCWorker<E::VM>,
}

impl<'a, E: ProcessEdgesWork> ObjectsClosure<'a, E> {
    pub fn new(worker: &'a mut GCWorker<E::VM>) -> Self {
        Self {
            edge_buffer: vec![],
            source_buffer: vec![],
            worker,
        }
    }

    fn flush(&mut self) {
        let mut new_edges = Vec::new();
        let mut new_sources = Vec::new();
        mem::swap(&mut new_edges, &mut self.edge_buffer);
        mem::swap(&mut new_sources, &mut self.source_buffer);
        self.worker.add_work(
            WorkBucketStage::Closure,
            E::new(new_sources, new_edges, false, self.worker.mmtk),
        );
    }
}

impl<'a, E: ProcessEdgesWork> EdgeVisitor for ObjectsClosure<'a, E> {
    #[inline(always)]
    fn visit_edge(&mut self, slot: Address) {
        if self.edge_buffer.is_empty() {
            self.edge_buffer.reserve(E::CAPACITY);
        }
        self.edge_buffer.push(slot);
        if self.edge_buffer.len() >= E::CAPACITY {
            let mut new_edges = Vec::new();
            mem::swap(&mut new_edges, &mut self.edge_buffer);
            self.worker.add_work(
                WorkBucketStage::Closure,
                E::new(vec![], new_edges, false, self.worker.mmtk),
            );
        }
    }

    #[inline(always)]
    fn visit_edge_with_source(&mut self, source: ObjectReference, slot: Address) {
        debug_assert!(
            self.edge_buffer.len() == self.source_buffer.len(),
            "source buffer and edge buffer are corrupted"
        );
        if self.edge_buffer.is_empty() {
            self.edge_buffer.reserve(E::CAPACITY);
            self.source_buffer.reserve(E::CAPACITY);
        }
        self.edge_buffer.push(slot);
        self.source_buffer.push(source);
        if self.edge_buffer.len() >= E::CAPACITY {
            let mut new_edges = Vec::new();
            let mut new_sources = Vec::new();
            mem::swap(&mut new_edges, &mut self.edge_buffer);
            mem::swap(&mut new_sources, &mut self.source_buffer);
            self.worker.add_work(
                WorkBucketStage::Closure,
                E::new(new_sources, new_edges, false, self.worker.mmtk),
            );
        }
    }
}

impl<'a, E: ProcessEdgesWork> Drop for ObjectsClosure<'a, E> {
    #[inline(always)]
    fn drop(&mut self) {
        self.flush();
    }
}
pub struct ThreadlocalObjectClosure<VM: crate::vm::VMBinding> {
    edge_buffer: std::collections::VecDeque<Address>,
    // mark: std::collections::HashSet<ObjectReference>,
    phantom: std::marker::PhantomData<VM>,
}

impl<VM: crate::vm::VMBinding> ThreadlocalObjectClosure<VM> {
    pub fn new(roots: Vec<Address>) -> Self {
        ThreadlocalObjectClosure {
            edge_buffer: std::collections::VecDeque::from(roots),
            // mark: std::collections::HashSet::new(),
            phantom: std::marker::PhantomData,
        }
    }

    pub fn do_closure(
        &mut self,
        mutator: &mut crate::Mutator<VM>,
        visited: &mut std::collections::HashSet<ObjectReference>,
    ) {
        use crate::vm::ActivePlan;
        use crate::vm::ObjectModel;
        use crate::vm::Scanning;

        const OWNER_MASK: usize = 0x00000000FFFFFFFF;
        while !self.edge_buffer.is_empty() {
            let slot = self.edge_buffer.pop_front().unwrap();
            let object = unsafe { slot.load::<ObjectReference>() };
            if object.is_null() {
                continue;
            }
            // if !crate::util::mark_bit::is_global_mark_set(object) {
            if !visited.contains(&object) {
                let owner = Self::get_header_object_owner(object);
                let request_id = mutator.request_id;
                assert!(
                    request_id != 0,
                    "request id is 0 (0 is reserved for non-critical)"
                );
                let mutator_id = VM::VMActivePlan::mutator_id(mutator.mutator_tls);
                let pattern = (request_id as usize) << 32 | (mutator_id & OWNER_MASK);
                // set mark bit on the object
                // crate::util::mark_bit::set_global_mark_bit(object);
                visited.insert(object);
                // self.mark.insert(object);
                mutator.critical_section_total_local_object_counter += 1;
                let object_size = VM::VMObjectModel::get_current_size(object);
                mutator.critical_section_total_local_object_bytes += object_size;

                // object is allocated in the request just finished
                if owner == pattern {
                    mutator.critical_section_local_live_object_counter += 1;
                    mutator.critical_section_local_live_object_bytes += object_size;
                    if !crate::util::public_bit::is_public(object) {
                        mutator.critical_section_local_live_private_object_counter += 1;
                        mutator.critical_section_local_live_private_object_bytes += object_size;
                    }
                }

                VM::VMScanning::scan_object(
                    crate::util::VMWorkerThread(crate::util::VMThread::UNINITIALIZED),
                    object,
                    self,
                );
            }
        }
    }

    #[inline(always)]
    fn header_object_owner_address(object: ObjectReference) -> Address {
        use crate::vm::ObjectModel;
        const GC_EXTRA_HEADER_BYTES: usize = 8;
        <VM as crate::vm::VMBinding>::VMObjectModel::object_start_ref(object)
            - GC_EXTRA_HEADER_BYTES
    }

    /// Get header forwarding pointer for an object
    #[inline(always)]
    fn get_header_object_owner(object: ObjectReference) -> usize {
        unsafe { Self::header_object_owner_address(object).load::<usize>() }
    }
}

impl<VM: crate::vm::VMBinding> crate::vm::EdgeVisitor for ThreadlocalObjectClosure<VM> {
    fn visit_edge(&mut self, edge: Address) {
        self.edge_buffer.push_back(edge);
    }

    fn visit_edge_with_source(&mut self, _source: ObjectReference, _edge: Address) {
        self.edge_buffer.push_back(_edge);
    }
}

impl<VM: crate::vm::VMBinding> Drop for ThreadlocalObjectClosure<VM> {
    #[inline(always)]
    fn drop(&mut self) {
        assert!(
            self.edge_buffer.is_empty(),
            "There are edges left over. Closure is not done correctly."
        );
    }
}
