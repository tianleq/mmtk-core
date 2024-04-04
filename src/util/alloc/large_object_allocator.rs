use std::collections::HashSet;

use std::sync::Arc;

use crate::policy::largeobjectspace::LargeObjectSpace;
use crate::policy::space::Space;
use crate::util::alloc::{allocator, Allocator};
use crate::util::{conversions, opaque_pointer::*};
use crate::util::{Address, ObjectReference};
use crate::vm::VMBinding;

#[cfg(feature = "thread_local_gc")]
const LOS_OBJECT_OWNER_BYTES: usize = 8;

#[cfg(feature = "thread_local_gc")]
const LOS_OBJECT_OWNER_BYTES: usize = 8;

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
    local_los_objects: Box<HashSet<ObjectReference>>,
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

    #[cfg(not(feature = "extra_header"))]
    fn alloc(&mut self, size: usize, align: usize, offset: usize) -> Address {
        #[cfg(not(feature = "thread_local_gc"))]
        let rtn = self.alloc_impl(size, align, offset);
        #[cfg(feature = "thread_local_gc")]
        let rtn = self.alloc_impl(size + LOS_OBJECT_OWNER_BYTES, align, offset);
        if !rtn.is_zero() {
            #[cfg(not(feature = "thread_local_gc"))]
            return rtn;
            #[cfg(feature = "thread_local_gc")]
            return rtn + LOS_OBJECT_OWNER_BYTES;
        } else {
            rtn
        }
    }

    #[cfg(feature = "extra_header")]
    fn alloc(&mut self, size: usize, align: usize, offset: usize) -> Address {
        #[cfg(not(feature = "thread_local_gc"))]
        let rtn = self.alloc_impl(size + VM::EXTRA_HEADER_BYTES, align, offset);
        #[cfg(feature = "thread_local_gc")]
        let rtn = self.alloc_impl(
            size + LOS_OBJECT_OWNER_BYTES + VM::EXTRA_HEADER_BYTES,
            align,
            offset,
        );
        if !rtn.is_zero() {
            #[cfg(not(feature = "thread_local_gc"))]
            return rtn + VM::EXTRA_HEADER_BYTES;
            #[cfg(feature = "thread_local_gc")]
            return rtn + LOS_OBJECT_OWNER_BYTES + VM::EXTRA_HEADER_BYTES;
        } else {
            rtn
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
            local_los_objects: Box::new(HashSet::new()),
        }
    }

    fn alloc_impl(&mut self, size: usize, align: usize, offset: usize) -> Address {
        let cell: Address = self.alloc_slow(size, align, offset);
        // We may get a null ptr from alloc due to the VM being OOM
        if !cell.is_zero() {
            let rtn = allocator::align_allocation::<VM>(cell, align, offset);
            debug_assert!(
                !crate::util::public_bit::is_public_object(rtn),
                "public bit is not cleared properly"
            );
            debug_assert!(
                conversions::is_page_aligned(rtn),
                "los allocation is not page-aligned"
            );
            rtn
        } else {
            cell
        }
    }

    #[cfg(feature = "thread_local_gc")]
    pub fn add_los_objects(&mut self, object: ObjectReference) {
        self.local_los_objects.insert(object);
    }

    #[cfg(feature = "thread_local_gc")]
    pub fn remove_los_objects(&mut self, object: ObjectReference) {
        self.local_los_objects.remove(&object);
    }

    #[cfg(feature = "thread_local_gc")]
    pub fn prepare(&mut self) {
        // Remove public los objects from local los set
        // Those public los objects have been added to the global tredmill
        // and are managed there.
        self.local_los_objects
            .retain(|object| !crate::util::public_bit::is_public::<VM>(*object))
    }

    #[cfg(feature = "thread_local_gc")]
    pub fn release(&mut self) {
        // This is a global gc, needs to remove dead objects from local los objects set
        use crate::policy::sft::SFT;
        let mut live_objects = vec![];
        for object in self.local_los_objects.drain() {
            #[cfg(debug_assertions)]
            if crate::util::public_bit::is_public::<VM>(object) {
                panic!("Public Object:{:?} found in local los set", object);
            }

            if self.space.is_live(object) {
                live_objects.push(object);
            } else {
                // local/private objects also need to be reclaimed in a global gc
                self.space.thread_local_sweep_large_object(object);
            }
        }
        self.local_los_objects.extend(live_objects);
    }

    #[cfg(feature = "thread_local_gc")]
    pub fn thread_local_prepare(&mut self) {
        self.prepare();
    }

    #[cfg(feature = "thread_local_gc")]
    pub fn thread_local_release(&mut self) {
        let mut live_objects = vec![];
        for object in self.local_los_objects.drain() {
            #[cfg(debug_assertions)]
            if crate::util::public_bit::is_public::<VM>(object) {
                panic!("Public Object:{:?} found in local los set", object);
            }

            if self.space.is_live_in_thread_local_gc(object) {
                // clear the local mark state
                self.space.clear_thread_local_mark(object);
                live_objects.push(object);
            } else {
                self.space.thread_local_sweep_large_object(object);
            }
        }

        self.local_los_objects.extend(live_objects);
    }
}
