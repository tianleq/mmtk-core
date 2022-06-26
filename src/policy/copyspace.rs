use crate::plan::TransitiveClosure;
use crate::policy::copy_context::PolicyCopyContext;
use crate::policy::space::SpaceOptions;
use crate::policy::space::*;
use crate::policy::space::{CommonSpace, Space, SFT};
use crate::scheduler::GCWorker;
use crate::util::constants::CARD_META_PAGES_PER_REGION;
use crate::util::copy::*;
use crate::util::heap::layout::heap_layout::{Mmapper, VMMap};
#[cfg(feature = "global_alloc_bit")]
use crate::util::heap::layout::vm_layout_constants::BYTES_IN_CHUNK;
use crate::util::heap::HeapMeta;
use crate::util::heap::VMRequest;
use crate::util::heap::{MonotonePageResource, PageResource};
use crate::util::metadata::side_metadata::{SideMetadataContext, SideMetadataSpec};
use crate::util::metadata::{extract_side_metadata, side_metadata, MetadataSpec};
use crate::util::object_forwarding;
use crate::util::{Address, ObjectReference};
use crate::vm::*;
use libc::{mprotect, PROT_EXEC, PROT_NONE, PROT_READ, PROT_WRITE};
use std::sync::atomic::{AtomicBool, Ordering};

const META_DATA_PAGES_PER_REGION: usize = CARD_META_PAGES_PER_REGION;

pub const GC_EXTRA_HEADER_WORD: usize = 1;
const GC_EXTRA_HEADER_BYTES: usize =
    GC_EXTRA_HEADER_WORD << crate::util::constants::LOG_BYTES_IN_WORD;

/// This type implements a simple copying space.
pub struct CopySpace<VM: VMBinding> {
    common: CommonSpace<VM>,
    pr: MonotonePageResource<VM>,
    from_space: AtomicBool,
}

impl<VM: VMBinding> SFT for CopySpace<VM> {
    fn name(&self) -> &str {
        self.get_name()
    }

    fn is_live(&self, object: ObjectReference) -> bool {
        !self.is_from_space() || object_forwarding::is_forwarded::<VM>(object)
    }

    fn is_movable(&self) -> bool {
        true
    }

    #[cfg(feature = "sanity")]
    fn is_sane(&self) -> bool {
        !self.is_from_space()
    }

    fn initialize_object_metadata(&self, _object: ObjectReference, _alloc: bool) {
        #[cfg(feature = "global_alloc_bit")]
        crate::util::alloc_bit::set_alloc_bit(_object);
    }

    #[inline(always)]
    fn get_forwarded_object(&self, object: ObjectReference) -> Option<ObjectReference> {
        if !self.is_from_space() {
            return None;
        }

        if object_forwarding::is_forwarded::<VM>(object) {
            Some(object_forwarding::read_forwarding_pointer::<VM>(object))
        } else {
            None
        }
    }

    #[inline(always)]
    fn set_object_owner(&self, object: ObjectReference, object_owner: usize) {
        Self::store_header_object_owner(object, object_owner);
    }

    #[inline(always)]
    fn sft_trace_object(
        &self,
        trace: SFTProcessEdgesMutRef,
        object: ObjectReference,
        worker: GCWorkerMutRef,
    ) -> ObjectReference {
        let trace = trace.into_mut::<VM>();
        let worker = worker.into_mut::<VM>();
        self.trace_object(trace, object, self.common.copy, worker)
    }
}

impl<VM: VMBinding> Space<VM> for CopySpace<VM> {
    fn as_space(&self) -> &dyn Space<VM> {
        self
    }

    fn as_sft(&self) -> &(dyn SFT + Sync + 'static) {
        self
    }

    fn get_page_resource(&self) -> &dyn PageResource<VM> {
        &self.pr
    }

    fn common(&self) -> &CommonSpace<VM> {
        &self.common
    }

    fn init(&mut self, _vm_map: &'static VMMap) {
        self.common().init(self.as_space());
    }

    fn release_multiple_pages(&mut self, _start: Address) {
        panic!("copyspace only releases pages enmasse")
    }

    fn set_copy_for_sft_trace(&mut self, semantics: Option<CopySemantics>) {
        self.common.copy = semantics;
    }
}

impl<VM: VMBinding> crate::policy::gc_work::PolicyTraceObject<VM> for CopySpace<VM> {
    #[inline(always)]
    fn trace_object<T: TransitiveClosure, const KIND: crate::policy::gc_work::TraceKind>(
        &self,
        trace: &mut T,
        object: ObjectReference,
        copy: Option<CopySemantics>,
        worker: &mut GCWorker<VM>,
    ) -> ObjectReference {
        self.trace_object(trace, object, copy, worker)
    }

    #[inline(always)]
    fn may_move_objects<const KIND: crate::policy::gc_work::TraceKind>() -> bool {
        true
    }
}

impl<VM: VMBinding> CopySpace<VM> {
    /// We need one extra header word for each object. Considering the alignment requirement, this is
    /// the actual bytes we need to reserve for each allocation.
    pub const HEADER_RESERVED_IN_BYTES: usize = if VM::MAX_ALIGNMENT > GC_EXTRA_HEADER_BYTES {
        VM::MAX_ALIGNMENT
    } else {
        GC_EXTRA_HEADER_BYTES
    }
    .next_power_of_two();

    // The following are a few functions for manipulating header forwarding poiner.
    // Basically for each allocation request, we allocate extra bytes of [`HEADER_RESERVED_IN_BYTES`].
    // From the allocation result we get (e.g. `alloc_res`), `alloc_res + HEADER_RESERVED_IN_BYTES` is the cell
    // address we return to the binding. It ensures we have at least one word (`GC_EXTRA_HEADER_WORD`) before
    // the cell address, and ensures the cell address is properly aligned.
    // From the cell address, `cell - GC_EXTRA_HEADER_WORD` is where we store the header forwarding pointer.

    /// Get the address for header forwarding pointer
    #[inline(always)]
    fn header_object_owner_address(object: ObjectReference) -> Address {
        VM::VMObjectModel::object_start_ref(object) - GC_EXTRA_HEADER_BYTES
    }

    /// Get header forwarding pointer for an object
    #[inline(always)]
    fn get_header_object_owner(object: ObjectReference) -> usize {
        unsafe { Self::header_object_owner_address(object).load::<usize>() }
    }

    /// Store header forwarding pointer for an object
    #[inline(always)]
    fn store_header_object_owner(object: ObjectReference, object_owner: usize) {
        unsafe {
            Self::header_object_owner_address(object).store::<usize>(object_owner);
        }
    }

    // Clear header forwarding pointer for an object
    #[inline(always)]
    fn clear_header_object_owner(object: ObjectReference) {
        crate::util::memory::zero(
            Self::header_object_owner_address(object),
            GC_EXTRA_HEADER_BYTES,
        );
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: &'static str,
        from_space: bool,
        zeroed: bool,
        vmrequest: VMRequest,
        global_side_metadata_specs: Vec<SideMetadataSpec>,
        vm_map: &'static VMMap,
        mmapper: &'static Mmapper,
        heap: &mut HeapMeta,
    ) -> Self {
        let local_specs = extract_side_metadata(&[
            *VM::VMObjectModel::LOCAL_FORWARDING_BITS_SPEC,
            *VM::VMObjectModel::LOCAL_FORWARDING_POINTER_SPEC,
        ]);
        let common = CommonSpace::new(
            SpaceOptions {
                name,
                movable: true,
                immortal: false,
                needs_log_bit: false,
                zeroed,
                vmrequest,
                side_metadata_specs: SideMetadataContext {
                    global: global_side_metadata_specs,
                    local: local_specs,
                },
            },
            vm_map,
            mmapper,
            heap,
        );
        CopySpace {
            pr: if vmrequest.is_discontiguous() {
                MonotonePageResource::new_discontiguous(META_DATA_PAGES_PER_REGION, vm_map)
            } else {
                MonotonePageResource::new_contiguous(
                    common.start,
                    common.extent,
                    META_DATA_PAGES_PER_REGION,
                    vm_map,
                )
            },
            common,
            from_space: AtomicBool::new(from_space),
        }
    }

    pub fn prepare(&self, from_space: bool) {
        self.from_space.store(from_space, Ordering::SeqCst);
        // Clear the metadata if we are using side forwarding status table. Otherwise
        // objects may inherit forwarding status from the previous GC.
        // TODO: Fix performance.
        if let MetadataSpec::OnSide(side_forwarding_status_table) =
            *<VM::VMObjectModel as ObjectModel<VM>>::LOCAL_FORWARDING_BITS_SPEC
        {
            side_metadata::bzero_metadata(
                &side_forwarding_status_table,
                self.common.start,
                self.pr.cursor() - self.common.start,
            );
        }
    }

    pub fn release(&self) {
        unsafe {
            #[cfg(feature = "global_alloc_bit")]
            self.reset_alloc_bit();
            self.reset_critical_bit();
            self.pr.reset();
        }
        self.common.metadata.reset();
        self.from_space.store(false, Ordering::SeqCst);
    }

    unsafe fn reset_critical_bit(&self) {
        let current_chunk = self.pr.get_current_chunk();
        if self.common.contiguous {
            crate::util::critical_bit::bzero_critical_bit(
                self.common.start,
                current_chunk + crate::util::heap::layout::vm_layout_constants::BYTES_IN_CHUNK
                    - self.common.start,
            );
        } else {
            unimplemented!();
        }
    }

    #[cfg(feature = "global_alloc_bit")]
    unsafe fn reset_alloc_bit(&self) {
        let current_chunk = self.pr.get_current_chunk();
        if self.common.contiguous {
            crate::util::alloc_bit::bzero_alloc_bit(
                self.common.start,
                current_chunk + BYTES_IN_CHUNK - self.common.start,
            );
        } else {
            unimplemented!();
        }
    }

    fn is_from_space(&self) -> bool {
        self.from_space.load(Ordering::SeqCst)
    }

    #[inline]
    pub fn trace_object<T: TransitiveClosure>(
        &self,
        trace: &mut T,
        object: ObjectReference,
        semantics: Option<CopySemantics>,
        worker: &mut GCWorker<VM>,
    ) -> ObjectReference {
        trace!("copyspace.trace_object(, {:?}, {:?})", object, semantics,);

        // If this is not from space, we do not need to trace it (the object has been copied to the tosapce)
        if !self.is_from_space() {
            // The copy semantics for tospace should be none.
            return object;
        }

        // This object is in from space, we will copy. Make sure we have a valid copy semantic.
        debug_assert!(semantics.is_some());

        #[cfg(feature = "global_alloc_bit")]
        debug_assert!(
            crate::util::alloc_bit::is_alloced(object),
            "{:x}: alloc bit not set",
            object
        );

        trace!("attempting to forward");
        let forwarding_status = object_forwarding::attempt_to_forward::<VM>(object);

        trace!("checking if object is being forwarded");
        if object_forwarding::state_is_forwarded_or_being_forwarded(forwarding_status) {
            trace!("... yes it is");
            let new_object =
                object_forwarding::spin_and_get_forwarded_object::<VM>(object, forwarding_status);
            trace!("Returning");
            new_object
        } else {
            trace!("... no it isn't. Copying");
            let new_object = object_forwarding::forward_object::<VM>(
                object,
                semantics.unwrap(),
                worker.get_copy_context_mut(),
            );
            let object_owner = Self::get_header_object_owner(object);

            Self::store_header_object_owner(new_object, object_owner);
            trace!("Forwarding pointer");
            trace.process_node(new_object);
            trace!("Copied [{:?} -> {:?}]", object, new_object);
            new_object
        }
    }

    #[allow(dead_code)] // Only used with certain features (such as sanity)
    pub fn protect(&self) {
        if !self.common().contiguous {
            panic!(
                "Implement Options.protectOnRelease for MonotonePageResource.release_pages_extent"
            )
        }
        let start = self.common().start;
        let extent = self.common().extent;
        unsafe {
            mprotect(start.to_mut_ptr(), extent, PROT_NONE);
        }
        trace!("Protect {:x} {:x}", start, start + extent);
    }

    #[allow(dead_code)] // Only used with certain features (such as sanity)
    pub fn unprotect(&self) {
        if !self.common().contiguous {
            panic!(
                "Implement Options.protectOnRelease for MonotonePageResource.release_pages_extent"
            )
        }
        let start = self.common().start;
        let extent = self.common().extent;
        unsafe {
            mprotect(
                start.to_mut_ptr(),
                extent,
                PROT_READ | PROT_WRITE | PROT_EXEC,
            );
        }
        trace!("Unprotect {:x} {:x}", start, start + extent);
    }
}

use crate::plan::Plan;
use crate::util::alloc::{Allocator, MarkCompactAllocator};
use crate::util::opaque_pointer::VMWorkerThread;

/// Copy allocator for CopySpace
pub struct CopySpaceCopyContext<VM: VMBinding> {
    copy_allocator: MarkCompactAllocator<VM>,
}

impl<VM: VMBinding> PolicyCopyContext for CopySpaceCopyContext<VM> {
    type VM = VM;

    fn prepare(&mut self) {}

    fn release(&mut self) {}

    #[inline(always)]
    fn alloc_copy(
        &mut self,
        _original: ObjectReference,
        bytes: usize,
        align: usize,
        offset: isize,
    ) -> Address {
        self.copy_allocator.alloc(bytes, align, offset)
    }
}

impl<VM: VMBinding> CopySpaceCopyContext<VM> {
    pub fn new(
        tls: VMWorkerThread,
        plan: &'static dyn Plan<VM = VM>,
        tospace: &'static CopySpace<VM>,
    ) -> Self {
        CopySpaceCopyContext {
            copy_allocator: MarkCompactAllocator::new(tls.0, tospace, plan),
        }
    }
}

impl<VM: VMBinding> CopySpaceCopyContext<VM> {
    pub fn rebind(&mut self, space: &CopySpace<VM>) {
        self.copy_allocator
            .rebind(unsafe { &*{ space as *const _ } });
    }
}
