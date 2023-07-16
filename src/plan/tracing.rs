//! This module contains code useful for tracing,
//! i.e. visiting the reachable objects by traversing all or part of an object graph.

use crate::scheduler::gc_work::{EdgeOf, ProcessEdgesWork};
use crate::scheduler::{GCWorker, WorkBucketStage};
use crate::util::{ObjectReference, VMMutatorThread, VMThread};
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
    worker: &'a mut GCWorker<E::VM>,
    single_thread: bool,
    mutator_tls: VMMutatorThread,
}

impl<'a, E: ProcessEdgesWork> ObjectsClosure<'a, E> {
    pub fn new(
        worker: &'a mut GCWorker<E::VM>,
        single_thread: bool,
        mutator_tls: VMMutatorThread,
    ) -> Self {
        Self {
            buffer: VectorQueue::new(),
            worker,
            single_thread,
            mutator_tls,
        }
    }

    fn flush(&mut self) {
        let buf = self.buffer.take();
        if !buf.is_empty() {
            if self.single_thread {
                let tls = if self.mutator_tls != VMMutatorThread(VMThread::UNINITIALIZED) {
                    Some(self.mutator_tls)
                } else {
                    Option::None
                };
                self.worker.add_local_work(
                    WorkBucketStage::Unconstrained,
                    E::new(buf, false, self.worker.mmtk, tls),
                );
            } else {
                self.worker.add_work(
                    WorkBucketStage::Closure,
                    E::new(buf, false, self.worker.mmtk, Option::None),
                );
            }
        }
    }
}

impl<'a, E: ProcessEdgesWork> EdgeVisitor<EdgeOf<E>> for ObjectsClosure<'a, E> {
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
}

impl<'a, E: ProcessEdgesWork> Drop for ObjectsClosure<'a, E> {
    fn drop(&mut self) {
        self.flush();
    }
}

// pub struct ThreadlocalObjectsClosure<
//     'a,
//     VM: VMBinding,
//     P: Plan<VM = VM> + PlanThreadlocalTraceObject<VM>,
//     const KIND: TraceKind,
// > {
//     mutator: &'a mut Mutator<VM>,
//     plan: &'static P,
//     edges: VecDeque<VM::VMEdge>,
//     nodes: VectorObjectQueue,
// }

// impl<
//         'a,
//         VM: VMBinding,
//         P: Plan<VM = VM> + PlanThreadlocalTraceObject<VM>,
//         const KIND: TraceKind,
//     > ThreadlocalObjectsClosure<'a, VM, P, KIND>
// {
//     pub fn new(mutator: &'a mut Mutator<VM>, plan: &'static P, edge: VM::VMEdge) -> Self {
//         ThreadlocalObjectsClosure {
//             mutator,
//             plan,
//             edges: VecDeque::from([edge]),
//             nodes: VectorObjectQueue::new(),
//         }
//     }

//     pub fn do_closure(&mut self) {
//         while !self.edges.is_empty() {
//             let slot = self.edges.pop_front().unwrap();
//             let object = slot.load();
//             if object.is_null() {
//                 continue;
//             }
//             let new_object = self
//                 .plan
//                 .thread_local_trace_object::<VectorObjectQueue, KIND>(
//                     &mut self.nodes,
//                     object,
//                     self.mutator,
//                 );
//             if P::thread_local_may_move_objects::<KIND>() {
//                 slot.store(new_object);
//             }
//             // only scan the object if it has not been visited before
//             if !self.nodes.is_empty() {
//                 VM::VMScanning::scan_object(
//                     crate::util::VMWorkerThread(crate::util::VMThread::UNINITIALIZED),
//                     object,
//                     self,
//                 );
//                 self.nodes.take();
//             }
//         }
//     }
// }

// impl<
//         'a,
//         VM: VMBinding,
//         P: Plan<VM = VM> + PlanThreadlocalTraceObject<VM>,
//         const KIND: TraceKind,
//     > EdgeVisitor<VM::VMEdge> for ThreadlocalObjectsClosure<'a, VM, P, KIND>
// {
//     fn visit_edge(&mut self, edge: VM::VMEdge) {
//         self.edges.push_back(edge);
//     }
// }

pub struct MarkingObjectPublicClosure<VM: crate::vm::VMBinding> {
    mmtk: &'static MMTK<VM>,
    edge_buffer: std::collections::VecDeque<VM::VMEdge>,
}

impl<VM: crate::vm::VMBinding> MarkingObjectPublicClosure<VM> {
    pub fn new(mmtk: &'static MMTK<VM>) -> Self {
        MarkingObjectPublicClosure {
            mmtk,
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
                // set public bit on the object
                crate::util::public_bit::set_public_bit::<VM>(object);
                self.mmtk.plan.publish_object(object);
                VM::VMScanning::scan_object(
                    crate::util::VMWorkerThread(crate::util::VMThread::UNINITIALIZED),
                    object,
                    self,
                );
            }
        }
    }
}

impl<VM: crate::vm::VMBinding> EdgeVisitor<VM::VMEdge> for MarkingObjectPublicClosure<VM> {
    fn visit_edge(&mut self, edge: VM::VMEdge) {
        self.edge_buffer.push_back(edge);
    }
}

impl<VM: crate::vm::VMBinding> Drop for MarkingObjectPublicClosure<VM> {
    #[inline(always)]
    fn drop(&mut self) {
        assert!(
            self.edge_buffer.is_empty(),
            "There are edges left over. Closure is not done correctly."
        );
    }
}

// pub struct MarkingObjectPublicWithAssertClosure<VM: crate::vm::VMBinding> {
//     edge_buffer: std::collections::VecDeque<VM::VMEdge>,
//     mutator_id: u32,
// }

// impl<VM: crate::vm::VMBinding> MarkingObjectPublicWithAssertClosure<VM> {
//     pub fn new(mutator_id: u32) -> Self {
//         MarkingObjectPublicWithAssertClosure {
//             edge_buffer: std::collections::VecDeque::new(),
//             mutator_id,
//         }
//     }

//     pub fn do_closure(&mut self) {
//         while !self.edge_buffer.is_empty() {
//             let slot = self.edge_buffer.pop_front().unwrap();
//             let object = slot.load();
//             if object.is_null() {
//                 continue;
//             }
//             if !crate::util::public_bit::is_public(object) {
//                 let owner = crate::util::object_metadata::get_header_object_owner::<VM>(object);
//                 let valid = owner == self.mutator_id;
//                 if !valid {
//                     VM::VMObjectModel::dump_object(object);
//                     assert!(
//                         valid,
//                         "public object {:?} escaped, created by {}, accessed by {}",
//                         object, owner, self.mutator_id
//                     );
//                 }

//                 // set public bit on the object
//                 crate::util::public_bit::set_public_bit(object);
//                 VM::VMScanning::scan_object(
//                     crate::util::VMWorkerThread(crate::util::VMThread::UNINITIALIZED),
//                     object,
//                     self,
//                 );
//             }
//         }
//     }
// }

// impl<VM: crate::vm::VMBinding> EdgeVisitor<VM::VMEdge>
//     for MarkingObjectPublicWithAssertClosure<VM>
// {
//     fn visit_edge(&mut self, edge: VM::VMEdge) {
//         self.edge_buffer.push_back(edge);
//     }
// }

// impl<VM: crate::vm::VMBinding> Drop for MarkingObjectPublicWithAssertClosure<VM> {
//     #[inline(always)]
//     fn drop(&mut self) {

//     }
// }

// pub struct DebugMarkingObjectClosure<VM: crate::vm::VMBinding> {
//     mmtk: &'static MMTK<VM>,
//     edge_buffer: std::collections::VecDeque<VM::VMEdge>,
// }

// impl<VM: crate::vm::VMBinding> DebugMarkingObjectClosure<VM> {
//     pub fn new(mmtk: &'static MMTK<VM>) -> Self {
//         DebugMarkingObjectClosure {
//             mmtk,
//             edge_buffer: std::collections::VecDeque::new(),
//         }
//     }

//     pub fn do_closure(&mut self) {
//         while !self.edge_buffer.is_empty() {
//             let slot = self.edge_buffer.pop_front().unwrap();
//             let object = slot.load();
//             if object.is_null() {
//                 continue;
//             }
//             if !crate::util::debug_bit::is_debug::<VM>(object) {
//                 // set public bit on the object
//                 crate::util::debug_bit::set_debug_bit::<VM>(object);
//                 VM::VMScanning::scan_object(
//                     crate::util::VMWorkerThread(crate::util::VMThread::UNINITIALIZED),
//                     object,
//                     self,
//                 );
//             }
//         }
//     }
// }

// impl<VM: crate::vm::VMBinding> EdgeVisitor<VM::VMEdge> for DebugMarkingObjectClosure<VM> {
//     fn visit_edge(&mut self, edge: VM::VMEdge) {
//         self.edge_buffer.push_back(edge);
//     }
// }

// impl<VM: crate::vm::VMBinding> Drop for DebugMarkingObjectClosure<VM> {
//     #[inline(always)]
//     fn drop(&mut self) {
//         assert!(
//             self.edge_buffer.is_empty(),
//             "There are edges left over. Closure is not done correctly."
//         );
//     }
// }
