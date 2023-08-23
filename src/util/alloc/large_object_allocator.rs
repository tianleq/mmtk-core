use crate::plan::Plan;
use crate::policy::largeobjectspace::LargeObjectSpace;
use crate::policy::space::Space;
use crate::util::alloc::{allocator, Allocator};
use crate::util::Address;
use crate::util::{conversions, opaque_pointer::*};
use crate::vm::VMBinding;

#[cfg(feature = "thread_local_gc")]
const LOS_OBJECT_OWNER_BYTES: usize = 8;

#[repr(C)]
pub struct LargeObjectAllocator<VM: VMBinding> {
    /// [`VMThread`] associated with this allocator instance
    pub tls: VMThread,
    /// [`Space`](src/policy/space/Space) instance associated with this allocator instance.
    space: &'static LargeObjectSpace<VM>,
    /// [`Plan`] instance that this allocator instance is associated with.
    plan: &'static dyn Plan<VM = VM>,
}

impl<VM: VMBinding> Allocator<VM> for LargeObjectAllocator<VM> {
    fn get_tls(&self) -> VMThread {
        self.tls
    }

    fn get_plan(&self) -> &'static dyn Plan<VM = VM> {
        self.plan
    }

    fn get_space(&self) -> &'static dyn Space<VM> {
        // Casting the interior of the Option: from &LargeObjectSpace to &dyn Space
        self.space as &'static dyn Space<VM>
    }

    #[cfg(feature = "thread_local_gc")]
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
        let header = 0; // HashSet is used instead of DoublyLinkedList
        let maxbytes = allocator::get_maximum_aligned_size::<VM>(size + header, align);
        let pages = crate::util::conversions::bytes_to_pages_up(maxbytes);
        let sp = self.space.allocate_pages(self.tls, pages);
        if sp.is_zero() {
            sp
        } else {
            sp + header
        }
    }
}

impl<VM: VMBinding> LargeObjectAllocator<VM> {
    pub fn new(
        tls: VMThread,
        space: &'static LargeObjectSpace<VM>,
        plan: &'static dyn Plan<VM = VM>,
    ) -> Self {
        LargeObjectAllocator { tls, space, plan }
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
}
