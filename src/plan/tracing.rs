//! This module contains code useful for tracing,
//! i.e. visiting the reachable objects by traversing all or part of an object graph.

use crate::scheduler::gc_work::{EdgeOf, ProcessEdgesWork};
use crate::scheduler::{GCWorker, WorkBucketStage};
use crate::util::{ObjectReference, VMMutatorThread};
use crate::vm::edge_shape::Edge;
use crate::vm::EdgeVisitor;
use crate::vm::Scanning;
use crate::MMTK;

/// This trait represents an object queue to enqueue objects during tracing.
pub trait ObjectQueue {
    /// Enqueue an object into the queue.
    fn enqueue(&mut self, object: ObjectReference);
}

pub type VectorObjectQueue = VectorQueue<ObjectReference>;

/// An implementation of `ObjectQueue` using a `Vec`.
///
/// This can also be used as a buffer. For example, the mark stack or the write barrier mod-buffer.
pub struct VectorQueue<T> {
    /// Enqueued nodes.
    buffer: Vec<T>,
}

impl<T> VectorQueue<T> {
    /// Reserve a capacity of this on first enqueue to avoid frequent resizing.
    const CAPACITY: usize = 4096;

    /// Create an empty `VectorObjectQueue`.
    pub fn new() -> Self {
        Self { buffer: Vec::new() }
    }

    /// Return `true` if the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Return the contents of the underlying vector.  It will empty the queue.
    pub fn take(&mut self) -> Vec<T> {
        std::mem::take(&mut self.buffer)
    }

    /// Consume this `VectorObjectQueue` and return its underlying vector.
    pub fn into_vec(self) -> Vec<T> {
        self.buffer
    }

    /// Check if the buffer size reaches `CAPACITY`.
    pub fn is_full(&self) -> bool {
        self.buffer.len() >= Self::CAPACITY
    }

    pub fn push(&mut self, v: T) {
        if self.buffer.is_empty() {
            self.buffer.reserve(Self::CAPACITY);
        }
        self.buffer.push(v);
    }
}

impl<T> Default for VectorQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl ObjectQueue for VectorQueue<ObjectReference> {
    fn enqueue(&mut self, v: ObjectReference) {
        self.push(v);
    }
}

/// A transitive closure visitor to collect all the edges of an object.
pub struct ObjectsClosure<'a, E: ProcessEdgesWork> {
    buffer: VectorQueue<EdgeOf<E>>,
    #[cfg(feature = "debug_publish_object")]
    sources: VectorQueue<ObjectReference>,
    worker: &'a mut GCWorker<E::VM>,
    single_thread: bool,
    _mutator_tls: Option<VMMutatorThread>,
}

impl<'a, E: ProcessEdgesWork> ObjectsClosure<'a, E> {
    pub fn new(
        worker: &'a mut GCWorker<E::VM>,
        single_thread: bool,
        mutator_tls: Option<VMMutatorThread>,
    ) -> Self {
        Self {
            buffer: VectorQueue::new(),
            #[cfg(feature = "debug_publish_object")]
            sources: VectorQueue::new(),
            worker,
            single_thread,
            _mutator_tls: mutator_tls,
        }
    }

    fn flush(&mut self) {
        let buf = self.buffer.take();
        #[cfg(feature = "debug_publish_object")]
        let sources = self.sources.take();

        if !buf.is_empty() {
            if self.single_thread {
                #[cfg(not(feature = "debug_publish_object"))]
                self.worker.add_local_work(
                    WorkBucketStage::Unconstrained,
                    E::new(buf, false, self.worker.mmtk, None),
                );
                #[cfg(feature = "debug_publish_object")]
                {
                    debug_assert!(
                        sources.len() == buf.len(),
                        "The number of objects and slots do not equal"
                    );
                    self.worker.add_local_work(
                        WorkBucketStage::Unconstrained,
                        E::new(sources, buf, false, 0, self.worker.mmtk, self._mutator_tls),
                    );
                }
            } else {
                #[cfg(not(feature = "debug_publish_object"))]
                self.worker.add_work(
                    WorkBucketStage::Closure,
                    E::new(buf, false, self.worker.mmtk, Option::None),
                );
                #[cfg(feature = "debug_publish_object")]
                {
                    debug_assert!(
                        sources.len() == buf.len(),
                        "The number of objects and slots do not equal"
                    );
                    self.worker.add_work(
                        WorkBucketStage::Closure,
                        E::new(sources, buf, false, 0, self.worker.mmtk, Option::None),
                    );
                }
            }
        }
    }
}

impl<'a, E: ProcessEdgesWork> EdgeVisitor<EdgeOf<E>> for ObjectsClosure<'a, E> {
    #[cfg(not(feature = "debug_publish_object"))]
    fn visit_edge(&mut self, slot: EdgeOf<E>) {
        #[cfg(debug_assertions)]
        {
            trace!(
                "(ObjectsClosure) Visit edge {:?} (pointing to {})",
                slot,
                slot.load()
            );
        }
        self.buffer.push(slot);
        if self.buffer.is_full() {
            self.flush();
        }
    }

    #[cfg(feature = "debug_publish_object")]
    fn visit_edge(&mut self, object: ObjectReference, slot: EdgeOf<E>) {
        #[cfg(debug_assertions)]
        {
            trace!(
                "(ObjectsClosure) Visit edge {:?} of Object {:?} (pointing to {})",
                slot,
                object,
                slot.load()
            );
        }
        self.buffer.push(slot);
        self.sources.push(object);
        if self.buffer.is_full() {
            self.flush();
        }
    }
}

impl<'a, E: ProcessEdgesWork> Drop for ObjectsClosure<'a, E> {
    fn drop(&mut self) {
        self.flush();
    }
}

pub struct PublishObjectClosure<VM: crate::vm::VMBinding> {
    _mmtk: &'static MMTK<VM>,
    edge_buffer: std::collections::VecDeque<VM::VMEdge>,
}

impl<VM: crate::vm::VMBinding> PublishObjectClosure<VM> {
    pub fn new(mmtk: &'static MMTK<VM>) -> Self {
        PublishObjectClosure {
            _mmtk: mmtk,
            edge_buffer: std::collections::VecDeque::new(),
        }
    }

    pub fn do_closure(&mut self) {
        while !self.edge_buffer.is_empty() {
            let slot = self.edge_buffer.pop_front().unwrap();
            let object = slot.load();
            if object.is_null() {
                continue;
            }
            if !crate::util::public_bit::is_public::<VM>(object) {
                // #[cfg(all(debug_assertions, feature = "debug_publish_object"))]
                // info!("publish descendant object: {:?}", object);
                // set public bit on the object
                crate::util::public_bit::set_public_bit::<VM>(object);
                #[cfg(feature = "thread_local_gc")]
                self._mmtk.plan.publish_object(object);
                VM::VMScanning::scan_object(
                    crate::util::VMWorkerThread(crate::util::VMThread::UNINITIALIZED),
                    object,
                    self,
                );
            }
        }
    }
}

impl<VM: crate::vm::VMBinding> EdgeVisitor<VM::VMEdge> for PublishObjectClosure<VM> {
    #[cfg(not(feature = "debug_publish_object"))]
    fn visit_edge(&mut self, edge: VM::VMEdge) {
        self.edge_buffer.push_back(edge);
    }

    #[cfg(feature = "debug_publish_object")]
    fn visit_edge(&mut self, _object: ObjectReference, edge: VM::VMEdge) {
        self.edge_buffer.push_back(edge);
    }
}

impl<VM: crate::vm::VMBinding> Drop for PublishObjectClosure<VM> {
    #[inline(always)]
    fn drop(&mut self) {
        assert!(
            self.edge_buffer.is_empty(),
            "There are edges left over. Closure is not done correctly."
        );
    }
}
