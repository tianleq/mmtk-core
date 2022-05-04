use std::io::Write;
use std::sync::atomic::AtomicUsize;

use super::space::{CommonSpace, Space, SpaceOptions, SFT};
use crate::policy::space::*;
use crate::util::alloc::allocator::align_allocation_no_fill;
use crate::util::constants::LOG_BYTES_IN_WORD;
use crate::util::heap::layout::heap_layout::{Mmapper, VMMap};
use crate::util::heap::layout::vm_layout_constants::HEAP_START;
use crate::util::heap::{HeapMeta, MonotonePageResource, PageResource, VMRequest};
use crate::util::metadata::load_metadata;
use crate::util::metadata::side_metadata::{SideMetadataContext, SideMetadataSpec};
use crate::util::metadata::{compare_exchange_metadata, extract_side_metadata};
use crate::util::{alloc_bit, Address, ObjectReference, VMMutatorThread};
use crate::{vm::*, TransitiveClosure};
use atomic::Ordering;

pub struct MarkCompactSpace<VM: VMBinding> {
    common: CommonSpace<VM>,
    pr: MonotonePageResource<VM>,
    pub birth: std::sync::Mutex<std::collections::HashMap<ObjectReference, (usize, usize)>>,
    pub live: std::sync::Mutex<std::collections::HashMap<ObjectReference, (usize, usize, i32)>>,
    pub live_object_depth_info: std::sync::Mutex<std::collections::HashMap<ObjectReference, i32>>,
    pub death: std::sync::Mutex<std::collections::HashMap<usize, i32>>,
    pub counter: AtomicUsize,
    round: AtomicUsize,
    allocation_per_gc: AtomicUsize,
}

const GC_MARK_BIT_MASK: usize = 1;

/// For each MarkCompact object, we need one extra word for storing forwarding pointer (Lisp-2 implementation).
/// Note that considering the object alignment, we may end up allocating/reserving more than one word per object.
/// See [`MarkCompactSpace::HEADER_RESERVED_IN_BYTES`].
pub const GC_EXTRA_HEADER_WORD: usize = 1;
const GC_EXTRA_HEADER_BYTES: usize = GC_EXTRA_HEADER_WORD << LOG_BYTES_IN_WORD;

impl<VM: VMBinding> SFT for MarkCompactSpace<VM> {
    fn name(&self) -> &str {
        self.get_name()
    }

    #[inline(always)]
    fn get_forwarded_object(&self, object: ObjectReference) -> Option<ObjectReference> {
        let forwarding_pointer = Self::get_header_forwarding_pointer(object);
        if forwarding_pointer.is_null() {
            None
        } else {
            Some(forwarding_pointer)
        }
    }

    fn is_live(&self, object: ObjectReference) -> bool {
        // Sanity checker cannot use this method to do the verification
        // since the mark bit will be cleared during the second trace(update forwarding pointer)
        Self::is_marked(object)
    }

    fn is_movable(&self) -> bool {
        true
    }

    fn initialize_object_metadata(&self, object: ObjectReference, bytes: usize, log_alloc: bool) {
        crate::util::alloc_bit::set_alloc_bit(object);
        if log_alloc {
            let count = self.counter.fetch_add(bytes, Ordering::SeqCst);

            self.birth
                .lock()
                .unwrap()
                .insert(object, (count + bytes, bytes));
            let rtn = self
                .live
                .lock()
                .unwrap()
                .insert(object, (count + bytes, bytes, i32::MAX));
            assert!(rtn == None);
            self.allocation_per_gc.fetch_add(bytes, Ordering::SeqCst);
        }
    }

    #[cfg(feature = "sanity")]
    fn is_sane(&self) -> bool {
        true
    }

    #[inline(always)]
    fn sft_trace_object(
        &self,
        _trace: SFTProcessEdgesMutRef,
        _object: ObjectReference,
        _worker: GCWorkerMutRef,
    ) -> ObjectReference {
        // We should not use trace_object for markcompact space.
        // Depending on which trace it is, we should manually call either trace_mark or trace_forward.
        panic!("sft_trace_object() cannot be used with mark compact space")
    }
}

impl<VM: VMBinding> Space<VM> for MarkCompactSpace<VM> {
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
        panic!("markcompactspace only releases pages enmasse")
    }
}

impl<VM: VMBinding> MarkCompactSpace<VM> {
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
    fn header_forwarding_pointer_address(object: ObjectReference) -> Address {
        VM::VMObjectModel::object_start_ref(object) - GC_EXTRA_HEADER_BYTES
    }

    /// Get header forwarding pointer for an object
    #[inline(always)]
    fn get_header_forwarding_pointer(object: ObjectReference) -> ObjectReference {
        unsafe { Self::header_forwarding_pointer_address(object).load::<ObjectReference>() }
    }

    /// Store header forwarding pointer for an object
    #[inline(always)]
    fn store_header_forwarding_pointer(
        object: ObjectReference,
        forwarding_pointer: ObjectReference,
    ) {
        unsafe {
            Self::header_forwarding_pointer_address(object)
                .store::<ObjectReference>(forwarding_pointer);
        }
    }

    // Clear header forwarding pointer for an object
    #[inline(always)]
    fn clear_header_forwarding_pointer(object: ObjectReference) {
        crate::util::memory::zero(
            Self::header_forwarding_pointer_address(object),
            GC_EXTRA_HEADER_BYTES,
        );
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: &'static str,
        zeroed: bool,
        vmrequest: VMRequest,
        global_side_metadata_specs: Vec<SideMetadataSpec>,
        vm_map: &'static VMMap,
        mmapper: &'static Mmapper,
        heap: &mut HeapMeta,
    ) -> Self {
        let local_specs = extract_side_metadata(&[*VM::VMObjectModel::LOCAL_MARK_BIT_SPEC]);
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
        MarkCompactSpace {
            pr: if vmrequest.is_discontiguous() {
                MonotonePageResource::new_discontiguous(0, vm_map)
            } else {
                MonotonePageResource::new_contiguous(common.start, common.extent, 0, vm_map)
            },
            common,
            birth: std::sync::Mutex::new(std::collections::HashMap::new()),
            live: std::sync::Mutex::new(std::collections::HashMap::new()),
            death: std::sync::Mutex::new(std::collections::HashMap::new()),
            live_object_depth_info: std::sync::Mutex::new(std::collections::HashMap::new()),
            counter: AtomicUsize::new(0),
            round: AtomicUsize::new(0),
            allocation_per_gc: AtomicUsize::new(0),
        }
    }

    pub fn prepare(&self) {
        info!(
            "allocation_per_gc: {}, counter: {}",
            self.allocation_per_gc.load(Ordering::SeqCst),
            self.counter.load(Ordering::SeqCst)
        );
        self.allocation_per_gc.store(0, Ordering::SeqCst);
        self.round.fetch_add(1, Ordering::SeqCst);
    }

    pub fn release(&self) {}

    pub fn flush_object_lifetime_info(&self, log_file_name: &str) {
        use std::fs::OpenOptions;
        let mut file = OpenOptions::new()
            .append(true)
            .create(false)
            .open(format!("{}", log_file_name))
            .unwrap();

        for e in self.live.lock().unwrap().values() {
            file.write(format!("l: {} {}\n", e.0, e.2).as_bytes())
                .unwrap();
        }
    }

    pub fn log_object_lifetime_info(
        &self,
        log_file_name: &str,
        harness: bool,
        counter: usize,
        threads_stack_info: &Vec<(VMMutatorThread, usize, usize, usize)>,
        thread_stack_roots_count: usize,
    ) {
        use std::fs::OpenOptions;
        let mut file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(format!("{}", log_file_name))
            .unwrap();
        const KB: usize = 1 << 10;

        let mut size = 0;
        for e in self.live.lock().unwrap().values() {
            size += e.1
        }
        write!(
            file,
            "heap size: {},{}\n",
            (self.pr.cursor().as_usize() - HEAP_START.as_usize()) / KB,
            size / KB
        )
        .unwrap();

        {
            let mut thread_stack_roots_count = 0;
            for e in threads_stack_info.iter() {
                thread_stack_roots_count += e.3;
                file.write(
                    format!(
                        "thread: {:?} roots: {} depth: {} size: {}\n",
                        e.0, e.3, e.1, e.2
                    )
                    .as_bytes(),
                )
                .unwrap();
            }
            info!("Number of thread stack roots: {}", thread_stack_roots_count);
            file.write(
                format!(
                    "Number of thread stack roots: {}\n",
                    thread_stack_roots_count
                )
                .as_bytes(),
            )
            .unwrap();
        }

        for (k, v) in self.death.lock().unwrap().iter() {
            file.write(format!("d: {} {}\n", *k, *v).as_bytes())
                .unwrap();
        }
        let mut live_object_depth_info = self.live_object_depth_info.lock().unwrap();
        let mut max_depth = 0;
        let mut total_depth: u64 = 0;
        let median_depth;
        {
            let mut val: Vec<i32> = live_object_depth_info.values().map(|x| *x).collect();
            val.sort();
            let size = val.len();
            match size {
                even if even % 2 == 0 => {
                    let fst_med = val[(even / 2) - 1];
                    let snd_med = val[even / 2];
                    median_depth = ((fst_med + snd_med) as f64 / 2.0).ceil() as usize;
                }
                odd => median_depth = val[odd / 2] as usize,
            }
        }
        // let mut n = 0;
        {
            let mut depth_info = std::collections::HashMap::new();
            for (_k, v) in live_object_depth_info.iter() {
                max_depth = std::cmp::max(max_depth, *v);
                // if max_depth > 10000 {
                //     VM::VMObjectModel::dump_object(*k);
                // }
                if depth_info.contains_key(&v) {
                    *depth_info.get_mut(&v).unwrap() += 1;
                } else {
                    depth_info.insert(v, 1);
                }
                debug_assert!(*v != -1, "object depth is corrupted");
                total_depth += *v as u64;
            }
            for (k, v) in depth_info.iter() {
                file.write(format!("object of depth: {} count: {}\n", *k, *v).as_bytes())
                    .unwrap();
            }
        }

        file.write(
            format!(
                "max depth: {} avg depth: {} median depth: {}\n",
                max_depth,
                (total_depth as f64 / (live_object_depth_info.len()) as f64).ceil(),
                median_depth
            )
            .as_bytes(),
        )
        .unwrap();
        for (_k, v) in self.birth.lock().unwrap().iter() {
            file.write(format!("n: {} {}\n", v.0, v.1).as_bytes())
                .unwrap();
        }
        if harness {
            file.write(b"--h--\n").unwrap();
        } else {
            file.write(b"----\n").unwrap();
        }

        info!("Total new object: {}", self.birth.lock().unwrap().len());
        self.death.lock().unwrap().clear();
        self.birth.lock().unwrap().clear();
        live_object_depth_info.clear();
    }

    pub fn trace_mark_object<T: TransitiveClosure>(
        &self,
        depth: i32,
        trace: &mut T,
        object: ObjectReference,
        log: bool,
    ) -> ObjectReference {
        debug_assert!(
            crate::util::alloc_bit::is_alloced(object),
            "{:x}: alloc bit not set",
            object
        );
        if log {
            debug_assert!(
                self.live.lock().unwrap().contains_key(&object),
                "live objects collection is corrupted"
            );
            // let mut t = self.live.lock().unwrap();
            // if depth != -1 {
            //     let v = t.get_mut(&object).unwrap();
            //     v.2 = std::cmp::min(depth, v.2);
            // }
            // if t.get(&object).unwrap().2 == i32::MAX {
            //     assert!(false, "weird");
            // }
            let mut t = self.live_object_depth_info.lock().unwrap();
            if depth != -1 {
                if t.contains_key(&object) {
                    let v = t.get_mut(&object).unwrap();
                    *v = std::cmp::min(depth, *v);
                } else {
                    t.insert(object, depth);
                }
            }
        }
        if MarkCompactSpace::<VM>::test_and_mark(object) {
            trace.process_node(object);
        }
        object
    }

    pub fn trace_forward_object<T: TransitiveClosure>(
        &self,
        trace: &mut T,
        object: ObjectReference,
    ) -> ObjectReference {
        debug_assert!(
            crate::util::alloc_bit::is_alloced(object),
            "{:x}: alloc bit not set",
            object
        );
        // from this stage and onwards, mark bit is no longer needed
        // therefore, it can be reused to save one extra bit in metadata
        if MarkCompactSpace::<VM>::test_and_clear_mark(object) {
            trace.process_node(object);
        }

        Self::get_header_forwarding_pointer(object)
    }

    pub fn test_and_mark(object: ObjectReference) -> bool {
        loop {
            let old_value = load_metadata::<VM>(
                &VM::VMObjectModel::LOCAL_MARK_BIT_SPEC,
                object,
                None,
                Some(Ordering::SeqCst),
            );
            let mark_bit = old_value & GC_MARK_BIT_MASK;
            if mark_bit != 0 {
                return false;
            }
            if compare_exchange_metadata::<VM>(
                &VM::VMObjectModel::LOCAL_MARK_BIT_SPEC,
                object,
                old_value,
                1,
                None,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                break;
            }
        }
        true
    }

    pub fn test_and_clear_mark(object: ObjectReference) -> bool {
        loop {
            let old_value = load_metadata::<VM>(
                &VM::VMObjectModel::LOCAL_MARK_BIT_SPEC,
                object,
                None,
                Some(Ordering::SeqCst),
            );
            let mark_bit = old_value & GC_MARK_BIT_MASK;
            if mark_bit == 0 {
                return false;
            }

            if compare_exchange_metadata::<VM>(
                &VM::VMObjectModel::LOCAL_MARK_BIT_SPEC,
                object,
                old_value,
                0,
                None,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                break;
            }
        }
        true
    }

    pub fn is_marked(object: ObjectReference) -> bool {
        let old_value = load_metadata::<VM>(
            &VM::VMObjectModel::LOCAL_MARK_BIT_SPEC,
            object,
            None,
            Some(Ordering::SeqCst),
        );
        let mark_bit = old_value & GC_MARK_BIT_MASK;
        mark_bit != 0
    }

    pub fn to_be_compacted(object: ObjectReference) -> bool {
        Self::is_marked(object)
    }

    pub fn calculate_forwarding_pointer(&self, log_lifetime: bool) {
        let start = self.common.start;
        let end = self.pr.cursor();
        let mut to = start;
        let linear_scan =
            crate::util::linear_scan::ObjectIterator::<VM, MarkCompactObjectSize<VM>, true>::new(
                start, end,
            );
        if log_lifetime {
            let mut new_live =
                std::collections::HashMap::<ObjectReference, (usize, usize, i32)>::new();
            let depth_info = self.live_object_depth_info.lock().unwrap();
            // for obj in linear_scan.filter(|obj| Self::to_be_compacted(*obj)) {
            for obj in linear_scan {
                let mut l = self.live.lock().unwrap();
                let o = l.get(&obj);
                // let id = *(self.live.lock().unwrap().get(&obj).unwrap());
                if !Self::to_be_compacted(obj) {
                    if !o.is_none() {
                        let id = *o.unwrap();
                        self.death.lock().unwrap().insert(id.0, o.unwrap().2);
                        l.remove(&obj);
                    }
                    continue;
                }

                let copied_size =
                    VM::VMObjectModel::get_size_when_copied(obj) + Self::HEADER_RESERVED_IN_BYTES;
                let align = VM::VMObjectModel::get_align_when_copied(obj);
                let offset = VM::VMObjectModel::get_align_offset_when_copied(obj);
                to = align_allocation_no_fill::<VM>(to, align, offset);

                // let forwarding_pointer_addr = obj.to_address() - GC_EXTRA_HEADER_BYTES;
                // unsafe { forwarding_pointer_addr.store(to) }
                let new_obj = VM::VMObjectModel::get_reference_when_copied_to(
                    obj,
                    to + Self::HEADER_RESERVED_IN_BYTES,
                );

                Self::store_header_forwarding_pointer(obj, new_obj);

                // let object_addr = to + Self::HEADER_RESERVED_IN_BYTES;
                if !o.is_none() {
                    // if the object is not newly allocated, at this round, it may have a different depth
                    // calculate the average depth/keep the max depth/median depth(how to do this efficiently)

                    // let id = *o.unwrap();
                    // let depth = depth_info.get(&obj).unwrap();
                    // let rtn = new_live.insert(new_obj, (id.0, id.1, *depth));
                    // assert!(rtn == None);
                    let id = *o.unwrap();
                    let depth = depth_info.get(&obj).unwrap();
                    if !self.birth.lock().unwrap().contains_key(&obj) {
                        assert!(id.2 != i32::MAX, "live objects have invalid depth");
                        // let number_of_gc = self.round.load(Ordering::SeqCst);
                        // let average_depth = (id.2 * number_of_gc as i32 + *depth) as f64
                        //     / (number_of_gc + 1) as f64;
                        let max_depth = std::cmp::max(*depth, id.2);
                        let rtn = new_live.insert(new_obj, (id.0, id.1, max_depth));
                        assert!(rtn == None);
                    } else {
                        let rtn = new_live.insert(new_obj, (id.0, id.1, *depth));
                        assert!(rtn == None);
                    }
                }
                to += copied_size;
            }
            self.live.lock().unwrap().clear();
            self.live.lock().unwrap().extend(new_live.iter());
        } else {
            for obj in linear_scan.filter(|obj| Self::to_be_compacted(*obj)) {
                let copied_size =
                    VM::VMObjectModel::get_size_when_copied(obj) + Self::HEADER_RESERVED_IN_BYTES;
                let align = VM::VMObjectModel::get_align_when_copied(obj);
                let offset = VM::VMObjectModel::get_align_offset_when_copied(obj);
                to = align_allocation_no_fill::<VM>(to, align, offset);
                let new_obj = VM::VMObjectModel::get_reference_when_copied_to(
                    obj,
                    to + Self::HEADER_RESERVED_IN_BYTES,
                );

                Self::store_header_forwarding_pointer(obj, new_obj);

                trace!(
                    "Calculate forward: {} (size when copied = {}) ~> {} (size = {})",
                    obj,
                    VM::VMObjectModel::get_size_when_copied(obj),
                    to,
                    copied_size
                );

                to += copied_size;
            }
        }
    }

    pub fn compact(&self) {
        let start = self.common.start;
        let end = self.pr.cursor();
        let mut to = end;
        let linear_scan =
            crate::util::linear_scan::ObjectIterator::<VM, MarkCompactObjectSize<VM>, true>::new(
                start, end,
            );
        for obj in linear_scan {
            // clear the alloc bit
            alloc_bit::unset_addr_alloc_bit(obj.to_address());

            let forwarding_pointer = Self::get_header_forwarding_pointer(obj);

            trace!("Compact {} to {}", obj, forwarding_pointer);
            if !forwarding_pointer.is_null() {
                let copied_size = VM::VMObjectModel::get_size_when_copied(obj);
                let new_object = forwarding_pointer;
                Self::clear_header_forwarding_pointer(new_object);

                // copy object
                trace!(" copy from {} to {}", obj, new_object);
                let end_of_new_object = VM::VMObjectModel::copy_to(obj, new_object, Address::ZERO);
                // update alloc_bit,
                alloc_bit::set_alloc_bit(new_object);
                to = new_object.to_address() + copied_size;
                debug_assert_eq!(end_of_new_object, to);
            }
        }

        debug!("Compact end: to = {}", to);

        // reset the bump pointer
        self.pr.reset_cursor(to);
    }
}

struct MarkCompactObjectSize<VM>(std::marker::PhantomData<VM>);
impl<VM: VMBinding> crate::util::linear_scan::LinearScanObjectSize for MarkCompactObjectSize<VM> {
    #[inline(always)]
    fn size(object: ObjectReference) -> usize {
        VM::VMObjectModel::get_current_size(object)
    }
}
