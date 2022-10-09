//! This module contains code useful for tracing,
//! i.e. visiting the reachable objects by traversing all or part of an object graph.

use crate::scheduler::gc_work::{EdgeOf, ProcessEdgesWork};
use crate::scheduler::{GCWorker, WorkBucketStage};
use crate::util::{Address, ObjectReference};
use crate::vm::edge_shape::Edge;
use crate::vm::ActivePlan;
use crate::vm::EdgeVisitor;
use crate::vm::ObjectModel;
use crate::vm::Scanning;

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
    #[inline(always)]
    pub fn is_full(&self) -> bool {
        self.buffer.len() >= Self::CAPACITY
    }

    #[inline(always)]
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
    #[inline(always)]
    fn enqueue(&mut self, v: ObjectReference) {
        self.push(v);
    }
}

/// A transitive closure visitor to collect all the edges of an object.
pub struct ObjectsClosure<'a, E: ProcessEdgesWork> {
    buffer: VectorQueue<EdgeOf<E>>,
    source_buffer: VectorQueue<ObjectReference>,
    worker: &'a mut GCWorker<E::VM>,
}

impl<'a, E: ProcessEdgesWork> ObjectsClosure<'a, E> {
    pub fn new(worker: &'a mut GCWorker<E::VM>) -> Self {
        Self {
            buffer: VectorQueue::new(),
            source_buffer: VectorQueue::new(),
            worker,
        }
    }

    fn flush(&mut self) {
        let buf = self.buffer.take();
        let source_buf = self.source_buffer.take();
        if !buf.is_empty() {
            self.worker.add_work(
                WorkBucketStage::Closure,
                E::new(source_buf, buf, false, self.worker.mmtk),
            );
        }
    }
}

impl<'a, E: ProcessEdgesWork> EdgeVisitor<EdgeOf<E>> for ObjectsClosure<'a, E> {
    #[inline(always)]
    fn visit_edge(&mut self, slot: EdgeOf<E>) {
        self.buffer.push(slot);
        if self.buffer.is_full() {
            self.flush();
        }
    }

    fn visit_edge_with_source(&mut self, source: ObjectReference, slot: EdgeOf<E>) {
        self.buffer.push(slot);
        self.source_buffer.push(source);
        if self.buffer.is_full() {
            self.flush();
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
    edge_buffer: std::collections::VecDeque<VM::VMEdge>,
    // mark: std::collections::HashSet<ObjectReference>,
}

impl<VM: crate::vm::VMBinding> ThreadlocalObjectClosure<VM> {
    pub fn new(roots: Vec<VM::VMEdge>) -> Self {
        ThreadlocalObjectClosure {
            edge_buffer: std::collections::VecDeque::from(roots),
            // mark: std::collections::HashSet::new(),
        }
    }

    pub fn do_closure(
        &mut self,
        mutator: &mut crate::Mutator<VM>,
        visited: &mut std::collections::HashSet<ObjectReference>,
    ) {
        const OWNER_MASK: usize = 0x00000000FFFFFFFF;
        while !self.edge_buffer.is_empty() {
            let slot = self.edge_buffer.pop_front().unwrap();
            let object = slot.load();
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
        const GC_EXTRA_HEADER_OFFSET: usize = 8;
        <VM as crate::vm::VMBinding>::VMObjectModel::object_start_ref(object)
            - GC_EXTRA_HEADER_OFFSET
    }

    /// Get header forwarding pointer for an object
    #[inline(always)]
    fn get_header_object_owner(object: ObjectReference) -> usize {
        unsafe { Self::header_object_owner_address(object).load::<usize>() }
    }
}

impl<VM: crate::vm::VMBinding> crate::vm::EdgeVisitor<VM::VMEdge> for ThreadlocalObjectClosure<VM> {
    fn visit_edge(&mut self, edge: VM::VMEdge) {
        self.edge_buffer.push_back(edge);
    }

    fn visit_edge_with_source(&mut self, _source: ObjectReference, edge: VM::VMEdge) {
        self.edge_buffer.push_back(edge);
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

pub struct BlockingObjectClosure<VM: crate::vm::VMBinding> {
    edge_buffer: std::collections::VecDeque<VM::VMEdge>,
}

impl<VM: crate::vm::VMBinding> BlockingObjectClosure<VM> {
    pub fn new() -> Self {
        BlockingObjectClosure {
            edge_buffer: std::collections::VecDeque::new(),
        }
    }

    pub fn do_closure(&mut self, mutator_id: usize) -> (u32, usize) {
        let mut count = 0;
        let mut bytes: usize = 0;
        while !self.edge_buffer.is_empty() {
            let slot = self.edge_buffer.pop_front().unwrap();
            let object = slot.load();
            if object.is_null() {
                continue;
            }
            if !crate::util::public_bit::is_public(object) {
                let pattern = Self::get_header_object_owner(object);
                assert!(
                    pattern & 0xFFFFFFFF00000000 != 0,
                    "objects outside request scope is visited."
                );
                // if !self.mark.contains(&object) {
                // set public bit on the object
                crate::util::public_bit::set_public_bit(
                    object,
                    mutator_id,
                    Self::get_header_object_owner(object),
                    false,
                );
                // crate::util::mark_bit::set_global_mark_bit(object);
                // println!("{:?} is public", object);
                VM::VMScanning::scan_object(
                    crate::util::VMWorkerThread(crate::util::VMThread::UNINITIALIZED),
                    object,
                    self,
                );
                // self.mark.insert(object);
                bytes += VM::VMObjectModel::get_current_size(object);
                count += 1;
            }
        }
        (count, bytes)
        // self.mark.clear();
    }

    #[inline(always)]
    fn header_object_owner_address(object: ObjectReference) -> Address {
        const GC_EXTRA_HEADER_OFFSET: usize = 8;
        <VM as crate::vm::VMBinding>::VMObjectModel::object_start_ref(object)
            - GC_EXTRA_HEADER_OFFSET
    }

    /// Get header forwarding pointer for an object
    #[inline(always)]
    fn get_header_object_owner(object: ObjectReference) -> usize {
        unsafe { Self::header_object_owner_address(object).load::<usize>() }
    }
}

impl<VM: crate::vm::VMBinding> EdgeVisitor<VM::VMEdge> for BlockingObjectClosure<VM> {
    fn visit_edge(&mut self, edge: VM::VMEdge) {
        self.edge_buffer.push_back(edge);
    }

    fn visit_edge_with_source(&mut self, _source: ObjectReference, edge: VM::VMEdge) {
        self.edge_buffer.push_back(edge);
    }
}

impl<VM: crate::vm::VMBinding> Drop for BlockingObjectClosure<VM> {
    #[inline(always)]
    fn drop(&mut self) {
        assert!(
            self.edge_buffer.is_empty(),
            "There are edges left over. Closure is not done correctly."
        );
    }
}
