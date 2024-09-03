//! This module contains code useful for tracing,
//! i.e. visiting the reachable objects by traversing all or part of an object graph.

use crate::scheduler::gc_work::{ProcessEdgesWork, SlotOf};
use crate::scheduler::{GCWorker, WorkBucketStage};
use crate::util::ObjectReference;
use crate::vm::SlotVisitor;

/// This trait represents an object queue to enqueue objects during tracing.
pub trait ObjectQueue {
    /// Enqueue an object into the queue.
    fn enqueue(&mut self, object: ObjectReference);
}

/// A vector queue for object references.
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

    /// Push an element to the queue. If the queue is empty, it will reserve
    /// space to hold the number of elements defined by the capacity.
    /// The user of this method needs to make sure the queue length does
    /// not exceed the capacity to avoid allocating more space
    /// (this method will not check the length against the capacity).
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

/// A transitive closure visitor to collect the slots from objects.
/// It maintains a buffer for the slots, and flushes slots to a new work packet
/// if the buffer is full or if the type gets dropped.
pub struct ObjectsClosure<'a, E: ProcessEdgesWork> {
    buffer: VectorQueue<SlotOf<E>>,
    pub(crate) worker: &'a mut GCWorker<E::VM>,
    bucket: WorkBucketStage,
}

impl<'a, E: ProcessEdgesWork> ObjectsClosure<'a, E> {
    /// Create an [`ObjectsClosure`].
    ///
    /// Arguments:
    /// * `worker`: the current worker. The objects closure should not leave the context of this worker.
    /// * `bucket`: new work generated will be push ed to the bucket.
    pub fn new(worker: &'a mut GCWorker<E::VM>, bucket: WorkBucketStage) -> Self {
        Self {
            buffer: VectorQueue::new(),
            worker,
            bucket,
        }
    }

    fn flush(&mut self) {
        let buf = self.buffer.take();
        if !buf.is_empty() {
            self.worker.add_work(
                self.bucket,
                E::new(buf, false, self.worker.mmtk, self.bucket),
            );
        }
    }
}

impl<'a, E: ProcessEdgesWork> SlotVisitor<SlotOf<E>> for ObjectsClosure<'a, E> {
    fn visit_slot(&mut self, slot: SlotOf<E>) {
        #[cfg(debug_assertions)]
        {
            use crate::vm::slot::Slot;
            trace!(
                "(ObjectsClosure) Visit slot {:?} (pointing to {:?})",
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

#[cfg(feature = "public_bit")]
pub struct PublishObjectClosure<VM: crate::vm::VMBinding> {
    _mmtk: &'static crate::MMTK<VM>,
    slot_buffer: std::collections::VecDeque<VM::VMSlot>,
}

#[cfg(feature = "public_bit")]
impl<VM: crate::vm::VMBinding> PublishObjectClosure<VM> {
    pub fn new(mmtk: &'static crate::MMTK<VM>) -> Self {
        PublishObjectClosure {
            _mmtk: mmtk,
            slot_buffer: std::collections::VecDeque::new(),
        }
    }

    pub fn do_closure(&mut self, object: ObjectReference) {
        use crate::vm::Scanning;
        crate::util::metadata::public_bit::set_public_bit::<VM>(object);

        #[cfg(feature = "thread_local_gc")]
        self.mmtk.get_plan().publish_object(value);
        VM::VMScanning::scan_object(
            crate::util::VMWorkerThread(crate::util::VMThread::UNINITIALIZED),
            object,
            self,
        );
        #[cfg(feature = "publish_rate_analysis")]
        {
            use crate::vm::ObjectModel;
            use crate::PUBLICATION_COUNT;
            use crate::PUBLICATION_SIZE;
            use crate::REQUEST_SCOPE_PUBLICATION_COUNT;
            use crate::REQUEST_SCOPE_PUBLICATION_SIZE;

            let object_size = VM::VMObjectModel::get_current_size(object);

            PUBLICATION_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            PUBLICATION_SIZE.fetch_add(object_size, std::sync::atomic::Ordering::SeqCst);
            REQUEST_SCOPE_PUBLICATION_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            REQUEST_SCOPE_PUBLICATION_SIZE
                .fetch_add(object_size, std::sync::atomic::Ordering::SeqCst);
        }
        while !self.slot_buffer.is_empty() {
            let slot = self.slot_buffer.pop_front().unwrap();
            let object = crate::vm::slot::Slot::load(&slot);
            if object.is_none() {
                continue;
            }
            let object = object.unwrap();
            if !crate::util::metadata::public_bit::is_public::<VM>(object) {
                // set public bit on the object
                crate::util::metadata::public_bit::set_public_bit::<VM>(object);

                #[cfg(feature = "thread_local_gc")]
                self._mmtk.get_plan().publish_object(object);
                VM::VMScanning::scan_object(
                    crate::util::VMWorkerThread(crate::util::VMThread::UNINITIALIZED),
                    object,
                    self,
                );
                #[cfg(feature = "publish_rate_analysis")]
                {
                    use crate::vm::ObjectModel;
                    use crate::PUBLICATION_COUNT;
                    use crate::PUBLICATION_SIZE;
                    use crate::REQUEST_SCOPE_PUBLICATION_COUNT;
                    use crate::REQUEST_SCOPE_PUBLICATION_SIZE;

                    let object_size = VM::VMObjectModel::get_current_size(object);

                    PUBLICATION_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    PUBLICATION_SIZE.fetch_add(object_size, std::sync::atomic::Ordering::SeqCst);
                    REQUEST_SCOPE_PUBLICATION_COUNT
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    REQUEST_SCOPE_PUBLICATION_SIZE
                        .fetch_add(object_size, std::sync::atomic::Ordering::SeqCst);
                }
            }
        }
    }
}

#[cfg(feature = "public_bit")]
impl<VM: crate::vm::VMBinding> SlotVisitor<VM::VMSlot> for PublishObjectClosure<VM> {
    fn visit_slot(&mut self, slot: VM::VMSlot) {
        self.slot_buffer.push_back(slot);
    }
}

#[cfg(feature = "public_bit")]
impl<VM: crate::vm::VMBinding> Drop for PublishObjectClosure<VM> {
    #[inline(always)]
    fn drop(&mut self) {
        assert!(
            self.slot_buffer.is_empty(),
            "There are edges left over. Closure is not done correctly."
        );
    }
}
