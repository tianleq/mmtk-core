use std::sync::Arc;

use crate::policy::largeobjectspace::LargeObjectSpace;
use crate::policy::space::Space;
use crate::util::alloc::{allocator, Allocator};
use crate::util::opaque_pointer::*;
use crate::util::Address;
use crate::vm::VMBinding;

use super::allocator::AllocatorContext;

/// An allocator that only allocates at page granularity.
/// This is intended for large objects.
#[repr(C)]
pub struct LargeObjectAllocator<VM: VMBinding> {
    /// [`VMThread`] associated with this allocator instance
    pub tls: VMThread,
    /// [`Space`](src/policy/space/Space) instance associated with this allocator instance.
    space: &'static LargeObjectSpace<VM>,
    context: Arc<AllocatorContext<VM>>,
    #[cfg(feature = "thread_local_gc")]
    local_los_objects: Box<std::collections::HashSet<crate::util::ObjectReference>>,
}

impl<VM: VMBinding> Allocator<VM> for LargeObjectAllocator<VM> {
    fn get_tls(&self) -> VMThread {
        self.tls
    }

    fn get_context(&self) -> &AllocatorContext<VM> {
        &self.context
    }

    fn get_space(&self) -> &'static dyn Space<VM> {
        // Casting the interior of the Option: from &LargeObjectSpace to &dyn Space
        self.space as &'static dyn Space<VM>
    }

    fn does_thread_local_allocation(&self) -> bool {
        false
    }

    fn alloc(&mut self, size: usize, align: usize, offset: usize) -> Address {
        let cell: Address = self.alloc_slow(size, align, offset);
        // We may get a null ptr from alloc due to the VM being OOM
        if !cell.is_zero() {
            allocator::align_allocation::<VM>(cell, align, offset)
        } else {
            cell
        }
    }

    fn alloc_slow_once(&mut self, size: usize, align: usize, _offset: usize) -> Address {
        if self.space.will_oom_on_acquire(self.tls, size) {
            return Address::ZERO;
        }

        let maxbytes = allocator::get_maximum_aligned_size::<VM>(size, align);
        let pages = crate::util::conversions::bytes_to_pages_up(maxbytes);
        self.space.allocate_pages(self.tls, pages)
    }
}

impl<VM: VMBinding> LargeObjectAllocator<VM> {
    pub(crate) fn new(
        tls: VMThread,
        space: &'static LargeObjectSpace<VM>,
        context: Arc<AllocatorContext<VM>>,
    ) -> Self {
        LargeObjectAllocator {
            tls,
            space,
            context,
            #[cfg(feature = "thread_local_gc")]
            local_los_objects: Box::new(std::collections::HashSet::new()),
        }
    }

    #[cfg(feature = "thread_local_gc")]
    pub fn add_los_objects(&mut self, object: crate::util::ObjectReference) {
        self.local_los_objects.insert(object);
    }

    #[cfg(feature = "thread_local_gc")]
    pub fn prepare(&mut self) {
        // Remove public los objects from local los set
        // Those public los objects have been added to the global tredmill
        // and are managed there.
        self.local_los_objects
            .retain(|object| !crate::util::metadata::public_bit::is_public::<VM>(*object))
    }

    #[cfg(feature = "thread_local_gc")]
    pub fn release(&mut self) {
        // This is a global gc, needs to remove dead objects from local los objects set
        use crate::policy::sft::SFT;
        let mut live_objects = vec![];
        for object in self.local_los_objects.drain() {
            debug_assert!(
                !crate::util::metadata::public_bit::is_public::<VM>(object),
                "Public Object:{:?} found in local los set",
                object
            );

            if self.space.is_live(object) {
                live_objects.push(object);
            } else {
                // local/private objects also need to be reclaimed in a global gc
                self.space.thread_local_sweep_large_object(object);
            }
        }
        self.local_los_objects.extend(live_objects);
    }
}
