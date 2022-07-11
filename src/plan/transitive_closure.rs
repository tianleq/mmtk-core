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
