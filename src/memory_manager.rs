//! VM-to-MMTk interface: safe Rust APIs.
//!
//! This module provides a safe Rust API for mmtk-core.
//! We expect the VM binding to inherit and extend this API by:
//! 1. adding their VM-specific functions
//! 2. exposing the functions to native if necessary. And the VM binding needs to manage the unsafety
//!    for exposing this safe API to FFI.
//!
//! For example, for mutators, this API provides a `Box<Mutator>`, and requires a `&mut Mutator` for allocation.
//! A VM binding can borrow a mutable reference directly from `Box<Mutator>`, and call `alloc()`. Alternatively,
//! it can turn the `Box` pointer to a native pointer (`*mut Mutator`), and forge a mut reference from the native
//! pointer. Either way, the VM binding code needs to guarantee the safety.

use crate::mmtk::MMTKBuilder;
use crate::mmtk::MMTK;
use crate::plan::AllocationSemantics;
use crate::plan::{Mutator, MutatorContext};
use crate::scheduler::WorkBucketStage;
use crate::scheduler::{GCWork, GCWorker};
use crate::util::alloc::allocators::AllocatorSelector;
use crate::util::constants::{LOG_BYTES_IN_PAGE, MIN_OBJECT_SIZE};
use crate::util::heap::layout::vm_layout::vm_layout;
use crate::util::opaque_pointer::*;
use crate::util::{Address, ObjectReference};
use crate::vm::slot::MemorySlice;
use crate::vm::ReferenceGlue;
use crate::vm::VMBinding;

/// Initialize an MMTk instance. A VM should call this method after creating an [`crate::MMTK`]
/// instance but before using any of the methods provided in MMTk (except `process()` and `process_bulk()`).
///
/// We expect a binding to ininitialize MMTk in the following steps:
///
/// 1. Create an [`crate::MMTKBuilder`] instance.
/// 2. Set command line options for MMTKBuilder by [`crate::memory_manager::process`] or [`crate::memory_manager::process_bulk`].
/// 3. Initialize MMTk by calling this function, `mmtk_init()`, and pass the builder earlier. This call will return an MMTK instance.
///    Usually a binding store the MMTK instance statically as a singleton. We plan to allow multiple instances, but this is not yet fully
///    supported. Currently we assume a binding will only need one MMTk instance. Note that GC is enabled by default and the binding should
///    implement `VMCollection::is_collection_enabled()` if it requires that the GC should be disabled at a particular time.
///
/// Note that this method will attempt to initialize a logger. If the VM would like to use its own logger, it should initialize the logger before calling this method.
/// Note that, to allow MMTk to do GC properly, `initialize_collection()` needs to be called after this call when
/// the VM's thread system is ready to spawn GC workers.
///
/// Note that this method returns a boxed pointer of MMTK, which means MMTk has a bound lifetime with the box pointer. However, some of our current APIs assume
/// that MMTk has a static lifetime, which presents a mismatch with this API. We plan to address the lifetime issue in the future. At this point, we recommend a binding
/// to 'expand' the lifetime for the boxed pointer to static. There could be multiple ways to achieve it: 1. `Box::leak()` will turn the box pointer to raw pointer
/// which has static lifetime, 2. create MMTK as a lazily initialized static variable
/// (see [what we do for our dummy binding](https://github.com/mmtk/mmtk-core/blob/master/vmbindings/dummyvm/src/lib.rs#L42))
///
/// Arguments:
/// * `builder`: The reference to a MMTk builder.
pub fn mmtk_init<VM: VMBinding>(builder: &MMTKBuilder) -> Box<MMTK<VM>> {
    match crate::util::logger::try_init() {
        Ok(_) => debug!("MMTk initialized the logger."),
        Err(_) => debug!(
            "MMTk failed to initialize the logger. Possibly a logger has been initialized by user."
        ),
    }
    #[cfg(all(feature = "perf_counter", target_os = "linux"))]
    {
        use std::fs::File;
        use std::io::Read;
        let mut status = File::open("/proc/self/status").unwrap();
        let mut contents = String::new();
        status.read_to_string(&mut contents).unwrap();
        for line in contents.lines() {
            let split: Vec<&str> = line.split('\t').collect();
            if split[0] == "Threads:" {
                let threads = split[1].parse::<i32>().unwrap();
                if threads != 1 {
                    warn!("Current process has {} threads, process-wide perf event measurement will only include child threads spawned from this thread", threads);
                }
            }
        }
    }
    let mmtk = builder.build();

    info!(
        "Initialized MMTk with {:?} ({:?})",
        *mmtk.options.plan, *mmtk.options.gc_trigger
    );
    #[cfg(feature = "extreme_assertions")]
    warn!("The feature 'extreme_assertions' is enabled. MMTk will run expensive run-time checks. Slow performance should be expected.");
    Box::new(mmtk)
}

/// Add an externally mmapped region to the VM space. A VM space can be set through MMTk options (`vm_space_start` and `vm_space_size`),
/// and can also be set through this function call. A VM space can be discontiguous. This function can be called multiple times,
/// and all the address ranges passed as arguments in the function will be considered as part of the VM space.
/// Currently we do not allow removing regions from VM space.
#[cfg(feature = "vm_space")]
pub fn set_vm_space<VM: VMBinding>(mmtk: &'static mut MMTK<VM>, start: Address, size: usize) {
    unsafe { mmtk.get_plan_mut() }
        .base_mut()
        .vm_space
        .set_vm_region(start, size);
}

/// Request MMTk to create a mutator for the given thread. The ownership
/// of returned boxed mutator is transferred to the binding, and the binding needs to take care of its
/// lifetime. For performance reasons, A VM should store the returned mutator in a thread local storage
/// that can be accessed efficiently. A VM may also copy and embed the mutator stucture to a thread-local data
/// structure, and use that as a reference to the mutator (it is okay to drop the box once the content is copied --
/// Note that `Mutator` may contain pointers so a binding may drop the box only if they perform a deep copy).
///
/// Arguments:
/// * `mmtk`: A reference to an MMTk instance.
/// * `tls`: The thread that will be associated with the mutator.
pub fn bind_mutator<VM: VMBinding>(
    mmtk: &'static MMTK<VM>,
    tls: VMMutatorThread,
) -> Box<Mutator<VM>> {
    let mutator = crate::plan::create_mutator(tls, mmtk);

    const LOG_ALLOCATOR_MAPPING: bool = false;
    if LOG_ALLOCATOR_MAPPING {
        info!("{:?}", mutator.config);
    }
    mutator
}

/// Report to MMTk that a mutator is no longer needed. All mutator state is flushed before it is
/// destroyed. A binding should not attempt to use the mutator after this call. MMTk will not
/// attempt to reclaim the memory for the mutator, so a binding should properly reclaim the memory
/// for the mutator after this call.
///
/// Arguments:
/// * `mutator`: A reference to the mutator to be destroyed.
pub fn destroy_mutator<VM: VMBinding>(mutator: &mut Mutator<VM>) {
    mutator.flush();
    mutator.on_destroy();
}

/// Flush the mutator's local states.
///
/// Arguments:
/// * `mutator`: A reference to the mutator.
pub fn flush_mutator<VM: VMBinding>(mutator: &mut Mutator<VM>) {
    mutator.flush()
}

/// Allocate memory for an object. For performance reasons, a VM should
/// implement the allocation fast-path on their side rather than just calling this function.
///
/// If the VM provides a non-zero `offset` parameter, then the returned address will be
/// such that the `RETURNED_ADDRESS + offset` is aligned to the `align` parameter.
///
/// Arguments:
/// * `mutator`: The mutator to perform this allocation request.
/// * `size`: The number of bytes required for the object.
/// * `align`: Required alignment for the object.
/// * `offset`: Offset associated with the alignment.
/// * `semantics`: The allocation semantic required for the allocation.
pub fn alloc<VM: VMBinding>(
    mutator: &mut Mutator<VM>,
    size: usize,
    align: usize,
    offset: usize,
    semantics: AllocationSemantics,
) -> Address {
    // MMTk has assumptions about minimal object size.
    // We need to make sure that all allocations comply with the min object size.
    // Ideally, we check the allocation size, and if it is smaller, we transparently allocate the min
    // object size (the VM does not need to know this). However, for the VM bindings we support at the moment,
    // their object sizes are all larger than MMTk's min object size, so we simply put an assertion here.
    // If you plan to use MMTk with a VM with its object size smaller than MMTk's min object size, you should
    // meet the min object size in the fastpath.
    debug_assert!(size >= MIN_OBJECT_SIZE);
    // Assert alignment
    debug_assert!(align >= VM::MIN_ALIGNMENT);
    debug_assert!(align <= VM::MAX_ALIGNMENT);
    // Assert offset
    debug_assert!(VM::USE_ALLOCATION_OFFSET || offset == 0);

    mutator.alloc(size, align, offset, semantics)
}

/// Invoke the allocation slow path. This is only intended for use when a binding implements the fastpath on
/// the binding side. When the binding handles fast path allocation and the fast path fails, it can use this
/// method for slow path allocation. Calling before exhausting fast path allocaiton buffer will lead to bad
/// performance.
///
/// Arguments:
/// * `mutator`: The mutator to perform this allocation request.
/// * `size`: The number of bytes required for the object.
/// * `align`: Required alignment for the object.
/// * `offset`: Offset associated with the alignment.
/// * `semantics`: The allocation semantic required for the allocation.
pub fn alloc_slow<VM: VMBinding>(
    mutator: &mut Mutator<VM>,
    size: usize,
    align: usize,
    offset: usize,
    semantics: AllocationSemantics,
) -> Address {
    mutator.alloc_slow(size, align, offset, semantics)
}

/// Perform post-allocation actions, usually initializing object metadata. For many allocators none are
/// required. For performance reasons, a VM should implement the post alloc fast-path on their side
/// rather than just calling this function.
///
/// Arguments:
/// * `mutator`: The mutator to perform post-alloc actions.
/// * `refer`: The newly allocated object.
/// * `bytes`: The size of the space allocated for the object (in bytes).
/// * `semantics`: The allocation semantics used for the allocation.
pub fn post_alloc<VM: VMBinding>(
    mutator: &mut Mutator<VM>,
    refer: ObjectReference,
    bytes: usize,
    semantics: AllocationSemantics,
) {
    mutator.post_alloc(refer, bytes, semantics);
}

/// The *subsuming* write barrier by MMTk. For performance reasons, a VM should implement the write barrier
/// fast-path on their side rather than just calling this function.
///
/// For a correct barrier implementation, a VM binding needs to choose one of the following options:
/// * Use subsuming barrier `object_reference_write`
/// * Use both `object_reference_write_pre` and `object_reference_write_post`, or both, if the binding has difficulty delegating the store to mmtk-core with the subsuming barrier.
/// * Implement fast-path on the VM side, and call the generic api `object_reference_write_slow` as barrier slow-path call.
/// * Implement fast-path on the VM side, and do a specialized slow-path call.
///
/// Arguments:
/// * `mutator`: The mutator for the current thread.
/// * `src`: The modified source object.
/// * `slot`: The location of the field to be modified.
/// * `target`: The target for the write operation.
///
/// # Deprecated
///
/// This function needs to be redesigned.  Its current form has multiple issues.
///
/// -   It is only able to write non-null object references into the slot.  But dynamic language
///     VMs may write non-reference values, such as tagged small integers, special values such as
///     `null`, `undefined`, `true`, `false`, etc. into a field that previous contains an object
///     reference.
/// -   It relies on `slot.store` to write `target` into the slot, but `slot.store` is designed for
///     forwarding references when an object is moved by GC, and is supposed to preserve tagged
///     type information, the offset (if it is an interior pointer), etc.  A write barrier is
///     associated to an assignment operation, which usually updates such information instead.
///
/// We will redesign a more general subsuming write barrier to address those problems and replace
/// the current `object_reference_write`.  Before that happens, VM bindings should use
/// `object_reference_write_pre` and `object_reference_write_post` instead.
#[deprecated = "Use `object_reference_write_pre` and `object_reference_write_post` instead, until this function is redesigned"]
pub fn object_reference_write<VM: VMBinding>(
    mutator: &mut Mutator<VM>,
    src: ObjectReference,
    slot: VM::VMSlot,
    target: ObjectReference,
) {
    mutator.barrier().object_reference_write(src, slot, target);
}

/// The write barrier by MMTk. This is a *pre* write barrier, which we expect a binding to call
/// *before* it modifies an object. For performance reasons, a VM should implement the write barrier
/// fast-path on their side rather than just calling this function.
///
/// For a correct barrier implementation, a VM binding needs to choose one of the following options:
/// * Use subsuming barrier `object_reference_write`
/// * Use both `object_reference_write_pre` and `object_reference_write_post`, or both, if the binding has difficulty delegating the store to mmtk-core with the subsuming barrier.
/// * Implement fast-path on the VM side, and call the generic api `object_reference_write_slow` as barrier slow-path call.
/// * Implement fast-path on the VM side, and do a specialized slow-path call.
///
/// Arguments:
/// * `mutator`: The mutator for the current thread.
/// * `src`: The modified source object.
/// * `slot`: The location of the field to be modified.
/// * `target`: The target for the write operation.  `None` if the slot did not hold an object
///   reference before the write operation.  For example, the slot may be holding a `null`
///   reference, a small integer, or special values such as `true`, `false`, `undefined`, etc.
pub fn object_reference_write_pre<VM: VMBinding>(
    mutator: &mut Mutator<VM>,
    src: ObjectReference,
    slot: VM::VMSlot,
    target: Option<ObjectReference>,
) {
    mutator
        .barrier()
        .object_reference_write_pre(src, slot, target);
}

/// The write barrier by MMTk. This is a *post* write barrier, which we expect a binding to call
/// *after* it modifies an object. For performance reasons, a VM should implement the write barrier
/// fast-path on their side rather than just calling this function.
///
/// For a correct barrier implementation, a VM binding needs to choose one of the following options:
/// * Use subsuming barrier `object_reference_write`
/// * Use both `object_reference_write_pre` and `object_reference_write_post`, or both, if the binding has difficulty delegating the store to mmtk-core with the subsuming barrier.
/// * Implement fast-path on the VM side, and call the generic api `object_reference_write_slow` as barrier slow-path call.
/// * Implement fast-path on the VM side, and do a specialized slow-path call.
///
/// Arguments:
/// * `mutator`: The mutator for the current thread.
/// * `src`: The modified source object.
/// * `slot`: The location of the field to be modified.
/// * `target`: The target for the write operation.  `None` if the slot no longer hold an object
///   reference after the write operation.  This may happen when writing a `null` reference, a small
///   integers, or a special value such as`true`, `false`, `undefined`, etc., into the slot.
pub fn object_reference_write_post<VM: VMBinding>(
    mutator: &mut Mutator<VM>,
    src: ObjectReference,
    slot: VM::VMSlot,
    target: Option<ObjectReference>,
) {
    mutator
        .barrier()
        .object_reference_write_post(src, slot, target);
}

/// The *subsuming* memory region copy barrier by MMTk.
/// This is called when the VM tries to copy a piece of heap memory to another.
/// The data within the slice does not necessarily to be all valid pointers,
/// but the VM binding will be able to filter out non-reference values on slot iteration.
///
/// For VMs that performs a heap memory copy operation, for example OpenJDK's array copy operation, the binding needs to
/// call `memory_region_copy*` APIs. Same as `object_reference_write*`, the binding can choose either the subsuming barrier,
/// or the pre/post barrier.
///
/// Arguments:
/// * `mutator`: The mutator for the current thread.
/// * `src`: Source memory slice to copy from.
/// * `dst`: Destination memory slice to copy to.
///
/// The size of `src` and `dst` shoule be equal
pub fn memory_region_copy<VM: VMBinding>(
    mutator: &'static mut Mutator<VM>,
    src: VM::VMMemorySlice,
    dst: VM::VMMemorySlice,
) {
    debug_assert_eq!(src.bytes(), dst.bytes());
    mutator.barrier().memory_region_copy(src, dst);
}

/// The *generic* memory region copy *pre* barrier by MMTk, which we expect a binding to call
/// *before* it performs memory copy.
/// This is called when the VM tries to copy a piece of heap memory to another.
/// The data within the slice does not necessarily to be all valid pointers,
/// but the VM binding will be able to filter out non-reference values on slot iteration.
///
/// For VMs that performs a heap memory copy operation, for example OpenJDK's array copy operation, the binding needs to
/// call `memory_region_copy*` APIs. Same as `object_reference_write*`, the binding can choose either the subsuming barrier,
/// or the pre/post barrier.
///
/// Arguments:
/// * `mutator`: The mutator for the current thread.
/// * `src`: Source memory slice to copy from.
/// * `dst`: Destination memory slice to copy to.
///
/// The size of `src` and `dst` shoule be equal
pub fn memory_region_copy_pre<VM: VMBinding>(
    mutator: &'static mut Mutator<VM>,
    src: VM::VMMemorySlice,
    dst: VM::VMMemorySlice,
) {
    debug_assert_eq!(src.bytes(), dst.bytes());
    mutator.barrier().memory_region_copy_pre(src, dst);
}

/// The *generic* memory region copy *post* barrier by MMTk, which we expect a binding to call
/// *after* it performs memory copy.
/// This is called when the VM tries to copy a piece of heap memory to another.
/// The data within the slice does not necessarily to be all valid pointers,
/// but the VM binding will be able to filter out non-reference values on slot iteration.
///
/// For VMs that performs a heap memory copy operation, for example OpenJDK's array copy operation, the binding needs to
/// call `memory_region_copy*` APIs. Same as `object_reference_write*`, the binding can choose either the subsuming barrier,
/// or the pre/post barrier.
///
/// Arguments:
/// * `mutator`: The mutator for the current thread.
/// * `src`: Source memory slice to copy from.
/// * `dst`: Destination memory slice to copy to.
///
/// The size of `src` and `dst` shoule be equal
pub fn memory_region_copy_post<VM: VMBinding>(
    mutator: &'static mut Mutator<VM>,
    src: VM::VMMemorySlice,
    dst: VM::VMMemorySlice,
) {
    debug_assert_eq!(src.bytes(), dst.bytes());
    mutator.barrier().memory_region_copy_post(src, dst);
}

/// Return an AllocatorSelector for the given allocation semantic. This method is provided
/// so that VM compilers may call it to help generate allocation fast-path.
///
/// Arguments:
/// * `mmtk`: The reference to an MMTk instance.
/// * `semantics`: The allocation semantic to query.
pub fn get_allocator_mapping<VM: VMBinding>(
    mmtk: &MMTK<VM>,
    semantics: AllocationSemantics,
) -> AllocatorSelector {
    mmtk.get_plan().get_allocator_mapping()[semantics]
}

/// The standard malloc. MMTk either uses its own allocator, or forward the call to a
/// library malloc.
pub fn malloc(size: usize) -> Address {
    crate::util::malloc::malloc(size)
}

/// The standard malloc except that with the feature `malloc_counted_size`, MMTk will count the allocated memory into its heap size.
/// Thus the method requires a reference to an MMTk instance. MMTk either uses its own allocator, or forward the call to a
/// library malloc.
#[cfg(feature = "malloc_counted_size")]
pub fn counted_malloc<VM: VMBinding>(mmtk: &MMTK<VM>, size: usize) -> Address {
    crate::util::malloc::counted_malloc(mmtk, size)
}

/// The standard calloc.
pub fn calloc(num: usize, size: usize) -> Address {
    crate::util::malloc::calloc(num, size)
}

/// The standard calloc except that with the feature `malloc_counted_size`, MMTk will count the allocated memory into its heap size.
/// Thus the method requires a reference to an MMTk instance.
#[cfg(feature = "malloc_counted_size")]
pub fn counted_calloc<VM: VMBinding>(mmtk: &MMTK<VM>, num: usize, size: usize) -> Address {
    crate::util::malloc::counted_calloc(mmtk, num, size)
}

/// The standard realloc.
pub fn realloc(addr: Address, size: usize) -> Address {
    crate::util::malloc::realloc(addr, size)
}

/// The standard realloc except that with the feature `malloc_counted_size`, MMTk will count the allocated memory into its heap size.
/// Thus the method requires a reference to an MMTk instance, and the size of the existing memory that will be reallocated.
/// The `addr` in the arguments must be an address that is earlier returned from MMTk's `malloc()`, `calloc()` or `realloc()`.
#[cfg(feature = "malloc_counted_size")]
pub fn realloc_with_old_size<VM: VMBinding>(
    mmtk: &MMTK<VM>,
    addr: Address,
    size: usize,
    old_size: usize,
) -> Address {
    crate::util::malloc::realloc_with_old_size(mmtk, addr, size, old_size)
}

/// The standard free.
/// The `addr` in the arguments must be an address that is earlier returned from MMTk's `malloc()`, `calloc()` or `realloc()`.
pub fn free(addr: Address) {
    crate::util::malloc::free(addr)
}

/// The standard free except that with the feature `malloc_counted_size`, MMTk will count the allocated memory into its heap size.
/// Thus the method requires a reference to an MMTk instance, and the size of the memory to free.
/// The `addr` in the arguments must be an address that is earlier returned from MMTk's `malloc()`, `calloc()` or `realloc()`.
#[cfg(feature = "malloc_counted_size")]
pub fn free_with_size<VM: VMBinding>(mmtk: &MMTK<VM>, addr: Address, old_size: usize) {
    crate::util::malloc::free_with_size(mmtk, addr, old_size)
}

/// Get the current active malloc'd bytes. Here MMTk only accounts for bytes that are done through those 'counted malloc' functions.
#[cfg(feature = "malloc_counted_size")]
pub fn get_malloc_bytes<VM: VMBinding>(mmtk: &MMTK<VM>) -> usize {
    use std::sync::atomic::Ordering;
    mmtk.state.malloc_bytes.load(Ordering::SeqCst)
}

/// Poll for GC. MMTk will decide if a GC is needed. If so, this call will block
/// the current thread, and trigger a GC. Otherwise, it will simply return.
/// Usually a binding does not need to call this function. MMTk will poll for GC during its allocation.
/// However, if a binding uses counted malloc (which won't poll for GC), they may want to poll for GC manually.
/// This function should only be used by mutator threads.
pub fn gc_poll<VM: VMBinding>(mmtk: &MMTK<VM>, tls: VMMutatorThread) {
    use crate::vm::{ActivePlan, Collection};
    debug_assert!(
        VM::VMActivePlan::is_mutator(tls.0),
        "gc_poll() can only be called by a mutator thread."
    );

    if VM::VMCollection::is_collection_enabled() && mmtk.gc_trigger.poll(false, None) {
        debug!("Collection required");
        assert!(mmtk.state.is_initialized(), "GC is not allowed here: collection is not initialized (did you call initialize_collection()?).");
        VM::VMCollection::block_for_gc(tls);
    }
}

/// Wrapper for [`crate::scheduler::GCWorker::run`].
pub fn start_worker<VM: VMBinding>(
    mmtk: &'static MMTK<VM>,
    tls: VMWorkerThread,
    worker: Box<GCWorker<VM>>,
) {
    worker.run(tls, mmtk);
}

/// Wrapper for [`crate::mmtk::MMTK::initialize_collection`].
pub fn initialize_collection<VM: VMBinding>(mmtk: &'static MMTK<VM>, tls: VMThread) {
    mmtk.initialize_collection(tls);
}

/// Process MMTk run-time options. Returns true if the option is processed successfully.
///
/// Arguments:
/// * `mmtk`: A reference to an MMTk instance.
/// * `name`: The name of the option.
/// * `value`: The value of the option (as a string).
pub fn process(builder: &mut MMTKBuilder, name: &str, value: &str) -> bool {
    builder.set_option(name, value)
}

/// Process multiple MMTk run-time options. Returns true if all the options are processed successfully.
///
/// Arguments:
/// * `mmtk`: A reference to an MMTk instance.
/// * `options`: a string that is key value pairs separated by white spaces, e.g. "threads=1 stress_factor=4096"
pub fn process_bulk(builder: &mut MMTKBuilder, options: &str) -> bool {
    builder.set_options_bulk_by_str(options)
}

/// Return used memory in bytes. MMTk accounts for memory in pages, thus this method always returns a value in
/// page granularity.
///
/// Arguments:
/// * `mmtk`: A reference to an MMTk instance.
pub fn used_bytes<VM: VMBinding>(mmtk: &MMTK<VM>) -> usize {
    mmtk.get_plan().get_used_pages() << LOG_BYTES_IN_PAGE
}

/// Return free memory in bytes. MMTk accounts for memory in pages, thus this method always returns a value in
/// page granularity.
///
/// Arguments:
/// * `mmtk`: A reference to an MMTk instance.
pub fn free_bytes<VM: VMBinding>(mmtk: &MMTK<VM>) -> usize {
    mmtk.get_plan().get_free_pages() << LOG_BYTES_IN_PAGE
}

/// Return the size of all the live objects in bytes in the last GC. MMTk usually accounts for memory in pages.
/// This is a special method that we count the size of every live object in a GC, and sum up the total bytes.
/// We provide this method so users can compare with `used_bytes` (which does page accounting), and know if
/// the heap is fragmented.
/// The value returned by this method is only updated when we finish tracing in a GC. A recommended timing
/// to call this method is at the end of a GC (e.g. when the runtime is about to resume threads).
#[cfg(feature = "count_live_bytes_in_gc")]
pub fn live_bytes_in_last_gc<VM: VMBinding>(mmtk: &MMTK<VM>) -> usize {
    use std::sync::atomic::Ordering;
    mmtk.state.live_bytes_in_last_gc.load(Ordering::SeqCst)
}

/// Return the starting address of the heap. *Note that currently MMTk uses
/// a fixed address range as heap.*
pub fn starting_heap_address() -> Address {
    vm_layout().heap_start
}

/// Return the ending address of the heap. *Note that currently MMTk uses
/// a fixed address range as heap.*
pub fn last_heap_address() -> Address {
    vm_layout().heap_end
}

/// Return the total memory in bytes.
///
/// Arguments:
/// * `mmtk`: A reference to an MMTk instance.
pub fn total_bytes<VM: VMBinding>(mmtk: &MMTK<VM>) -> usize {
    mmtk.get_plan().get_total_pages() << LOG_BYTES_IN_PAGE
}

/// Trigger a garbage collection as requested by the user.
///
/// Arguments:
/// * `mmtk`: A reference to an MMTk instance.
/// * `tls`: The thread that triggers this collection request.
pub fn handle_user_collection_request<VM: VMBinding>(mmtk: &MMTK<VM>, tls: VMMutatorThread) {
    mmtk.handle_user_collection_request(tls, false, false);
}

/// Is the object alive?
///
/// Arguments:
/// * `object`: The object reference to query.
pub fn is_live_object<VM: VMBinding>(object: ObjectReference) -> bool {
    object.is_live::<VM>()
}

/// Check if `addr` is the address of an object reference to an MMTk object.
///
/// Concretely:
/// 1.  Return true if `ObjectReference::from_raw_address(addr)` is a valid object reference to an
///     object in any space in MMTk.
/// 2.  Return false otherwise.
///
/// This function is useful for conservative root scanning.  The VM can iterate through all words in
/// a stack, filter out zeros, misaligned words, obviously out-of-range words (such as addresses
/// greater than `0x0000_7fff_ffff_ffff` on Linux on x86_64), and use this function to deside if the
/// word is really a reference.
///
/// This function does not handle internal pointers. If a binding may have internal pointers on
/// the stack, and requires identifying the base reference for an internal pointer, they should use
/// [`find_object_from_internal_pointer`] instead.
///
/// Note: This function has special behaviors if the VM space (enabled by the `vm_space` feature)
/// is present.  See `crate::plan::global::BasePlan::vm_space`.
///
/// Argument:
/// * `addr`: An arbitrary address.
#[cfg(feature = "is_mmtk_object")]
pub fn is_mmtk_object(addr: Address) -> Option<ObjectReference> {
    crate::util::is_mmtk_object::check_object_reference(addr)
}

/// Find if there is an object with VO bit set for the given address range.
/// This should be used instead of [`crate::memory_manager::is_mmtk_object`] for conservative stack scanning if
/// the binding may have internal pointers on the stack.
///
/// Note that, we only consider pointers that point to addresses that are equal or greater than the in-object addresss
/// (i.e. [`crate::util::ObjectReference::to_address()`] which is the same as `object_ref.to_raw_address() + ObjectModel::IN_OBJECT_ADDRESS_OFFSET`),
/// and within the allocation as 'internal pointers'. To be precise, for each object ref `obj_ref`, internal pointers are in the range
/// `[obj_ref + ObjectModel::IN_OBJECT_ADDRESS_OFFSET, ObjectModel::ref_to_object_start(obj_ref) + ObjectModel::get_current_size(obj_ref))`.
/// If a binding defines internal pointers differently, calling this method is undefined behavior.
/// If this is the case for you, please submit an issue or engage us on Zulip to discuss more.
///
/// Note that, in the similar situation as [`crate::memory_manager::is_mmtk_object`], the binding should filter
/// out obvious non-pointers (e.g. alignment check, bound check, etc) before calling this function to avoid unnecessary
/// cost. This method is not cheap.
///
/// To minimize the cost, the user should also use a small `max_search_bytes`.
///
/// Note: This function has special behaviors if the VM space (enabled by the `vm_space` feature)
/// is present.  See `crate::plan::global::BasePlan::vm_space`.
///
/// Argument:
/// * `internal_ptr`: The address to start searching. We search backwards from this address (including this address) to find the base reference.
/// * `max_search_bytes`: The maximum number of bytes we may search for an object with VO bit set. `internal_ptr - max_search_bytes` is not included.
#[cfg(feature = "is_mmtk_object")]
pub fn find_object_from_internal_pointer<VM: VMBinding>(
    internal_ptr: Address,
    max_search_bytes: usize,
) -> Option<ObjectReference> {
    crate::util::is_mmtk_object::check_internal_reference(internal_ptr, max_search_bytes)
}

/// Return true if the `object` lies in a region of memory where
/// -   only MMTk can allocate into, or
/// -   only MMTk's delegated memory allocator (such as a malloc implementation) can allocate into
///     for allocation requests from MMTk.
///
/// Return false otherwise.  This function never panics.
///
/// Particularly, if this function returns true, `object` cannot be an object allocated by the VM
/// itself.
///
/// If this function returns true, the object cannot be allocate by the `malloc` function called by
/// the VM, either. In other words, if the `MallocSpace` of MMTk called `malloc` to allocate the
/// object for the VM in response to `memory_manager::alloc`, this function will return true; but
/// if the VM directly called `malloc` to allocate the object, this function will return false.
///
/// If `is_mmtk_object(object.to_address())` returns true, `is_in_mmtk_spaces(object)` must also
/// return true.
///
/// This function is useful if an object reference in the VM can be either a pointer into the MMTk
/// heap, or a pointer to non-MMTk objects.  If the VM has a pre-built boot image that contains
/// primordial objects, or if the VM has its own allocator or uses any third-party allocators, or
/// if the VM allows an object reference to point to native objects such as C++ objects, this
/// function can distinguish between MMTk-allocated objects and other objects.
///
/// Note: This function has special behaviors if the VM space (enabled by the `vm_space` feature)
/// is present.  See `crate::plan::global::BasePlan::vm_space`.
///
/// Arguments:
/// * `object`: The object reference to query.
pub fn is_in_mmtk_spaces<VM: VMBinding>(object: ObjectReference) -> bool {
    use crate::mmtk::SFT_MAP;
    SFT_MAP
        .get_checked(object.to_address::<VM>())
        .is_in_space(object)
}

/// Is the address in the mapped memory? The runtime can use this function to check
/// if an address is mapped by MMTk. Note that this is different than is_in_mmtk_spaces().
/// For malloc spaces, MMTk does not map those addresses (malloc does the mmap), so
/// this function will return false, but is_in_mmtk_spaces will return true if the address
/// is actually a valid object in malloc spaces. To check if an object is in our heap,
/// the runtime should always use is_in_mmtk_spaces(). This function is_mapped_address()
/// may get removed at some point.
///
/// Arguments:
/// * `address`: The address to query.
// TODO: Do we really need this function? Can a runtime always use is_mapped_object()?
pub fn is_mapped_address(address: Address) -> bool {
    address.is_mapped()
}

/// Add a reference to the list of weak references. A binding may
/// call this either when a weak reference is created, or when a weak reference is traced during GC.
///
/// Arguments:
/// * `mmtk`: A reference to an MMTk instance.
/// * `reff`: The weak reference to add.
pub fn add_weak_candidate<VM: VMBinding>(mmtk: &MMTK<VM>, reff: ObjectReference) {
    mmtk.reference_processors.add_weak_candidate(reff);
}

/// Add a reference to the list of soft references. A binding may
/// call this either when a weak reference is created, or when a weak reference is traced during GC.
///
/// Arguments:
/// * `mmtk`: A reference to an MMTk instance.
/// * `reff`: The soft reference to add.
pub fn add_soft_candidate<VM: VMBinding>(mmtk: &MMTK<VM>, reff: ObjectReference) {
    mmtk.reference_processors.add_soft_candidate(reff);
}

/// Add a reference to the list of phantom references. A binding may
/// call this either when a weak reference is created, or when a weak reference is traced during GC.
///
/// Arguments:
/// * `mmtk`: A reference to an MMTk instance.
/// * `reff`: The phantom reference to add.
pub fn add_phantom_candidate<VM: VMBinding>(mmtk: &MMTK<VM>, reff: ObjectReference) {
    mmtk.reference_processors.add_phantom_candidate(reff);
}

/// Generic hook to allow benchmarks to be harnessed. We do a full heap
/// GC, and then start recording statistics for MMTk.
///
/// Arguments:
/// * `mmtk`: A reference to an MMTk instance.
/// * `tls`: The thread that calls the function (and triggers a collection).
pub fn harness_begin<VM: VMBinding>(mmtk: &MMTK<VM>, tls: VMMutatorThread) {
    mmtk.harness_begin(tls);
    #[cfg(any(feature = "publish_rate_analysis", feature = "allocation_stats"))]
    {
        use crate::ALLOCATION_COUNT;
        use crate::ALLOCATION_SIZE;
        use crate::PUBLICATION_COUNT;
        use crate::PUBLICATION_SIZE;
        use std::sync::atomic::Ordering::Release;

        ALLOCATION_COUNT.store(0, Release);
        ALLOCATION_SIZE.store(0, Release);
        PUBLICATION_COUNT.store(0, Release);
        PUBLICATION_SIZE.store(0, Release);
        // crate::PUBLIC_KLASS_MAP.lock().unwrap().clear();

        // let mut stats = crate::PER_THREAD_PUBLICATION_STATS_MAP.lock().unwrap();
        // for s in stats.values_mut() {
        //     s.allocation_count = 0;
        //     s.allocation_size = 0;
        //     s.publication_count = 0;
        //     s.publication_size = 0;
        // }
    }
}

/// Generic hook to allow benchmarks to be harnessed. We stop collecting
/// statistics, and print stats values.
///
/// Arguments:
/// * `mmtk`: A reference to an MMTk instance.
pub fn harness_end<VM: VMBinding>(mmtk: &'static MMTK<VM>) {
    mmtk.harness_end();
    #[cfg(feature = "publish_rate_analysis")]
    {
        // use crate::MUTATOR_NAME_MAP;
        // use crate::PUBLIC_KLASS_MAP;
        // use std::io::Write;

        // let mut file = std::fs::OpenOptions::new()
        //     .create(true)
        //     .write(true)
        //     .open("/home/tianleq/misc/tmp/public-klass.csv")
        //     .unwrap();
        // let tmp = PUBLIC_KLASS_MAP.lock().unwrap();
        // let map: std::collections::BTreeMap<&usize, &String> =
        //     tmp.iter().map(|(k, v)| (v, k)).collect();
        // for (&count, &name) in &map {
        //     let content = format!("{name},{count}\n");
        //     file.write(content.as_bytes()).unwrap();
        // }
        // file.write("\n".as_bytes()).unwrap();
        // file.flush().unwrap();
        // println!(
        //     "============================ MMTk Publication Stats ============================"
        // );
        // let mut name_map = crate::MUTATOR_NAME_MAP.lock().unwrap();

        // for (mutator_id, stats) in crate::PER_THREAD_PUBLICATION_STATS_MAP
        //     .lock()
        //     .unwrap()
        //     .iter()
        // {
        //     let byte_ratio = stats.request_scope_publication_size as f64
        //         / stats.request_scope_allocation_size as f64;
        //     let object_ratio = stats.request_scope_publication_count as f64
        //         / stats.request_scope_allocation_count as f64;
        //     let publication_size = stats.request_scope_publication_size;
        //     let allocation_size = stats.request_scope_allocation_size;
        //     let name = if name_map.get(&mutator_id).is_some() {
        //         name_map.get(&mutator_id).unwrap()
        //     } else {
        //         "unknown"
        //     };
        //     let content = format!(
        //         "{name},{mutator_id},{byte_ratio},{object_ratio}|{publication_size}, {allocation_size}"
        //     );
        //     // file.write(content.as_bytes()).unwrap();
        //     // file.flush().unwrap();
        //     println!("{}", content);
        // }
        // crate::PER_THREAD_PUBLICATION_STATS_MAP
        //     .lock()
        //     .unwrap()
        //     .clear();
        // name_map.clear();
        // // MUTATOR_NAME_MAP.lock().unwrap().clear();
        // println!(
        //     "-------------------------- End MMTk Publication Stats --------------------------"
        // );
    }
}

/// Register a finalizable object. MMTk will retain the liveness of
/// the object even if it is not reachable from the program.
/// Note that finalization upon exit is not supported.
///
/// Arguments:
/// * `mmtk`: A reference to an MMTk instance
/// * `object`: The object that has a finalizer
pub fn add_finalizer<VM: VMBinding>(
    mmtk: &'static MMTK<VM>,
    object: <VM::VMReferenceGlue as ReferenceGlue<VM>>::FinalizableType,
) {
    if *mmtk.options.no_finalizer {
        warn!("add_finalizer() is called when no_finalizer = true");
    }

    mmtk.finalizable_processor.lock().unwrap().add(object);
}

/// Pin an object. MMTk will make sure that the object does not move
/// during GC. Note that action cannot happen in some plans, eg, semispace.
/// It returns true if the pinning operation has been performed, i.e.,
/// the object status changed from non-pinned to pinned
///
/// Arguments:
/// * `object`: The object to be pinned
#[cfg(feature = "object_pinning")]
pub fn pin_object<VM: VMBinding>(object: ObjectReference) -> bool {
    use crate::mmtk::SFT_MAP;
    SFT_MAP
        .get_checked(object.to_address::<VM>())
        .pin_object(object)
}

/// Unpin an object.
/// Returns true if the unpinning operation has been performed, i.e.,
/// the object status changed from pinned to non-pinned
///
/// Arguments:
/// * `object`: The object to be pinned
#[cfg(feature = "object_pinning")]
pub fn unpin_object<VM: VMBinding>(object: ObjectReference) -> bool {
    use crate::mmtk::SFT_MAP;
    SFT_MAP
        .get_checked(object.to_address::<VM>())
        .unpin_object(object)
}

/// Check whether an object is currently pinned
///
/// Arguments:
/// * `object`: The object to be checked
#[cfg(feature = "object_pinning")]
pub fn is_pinned<VM: VMBinding>(object: ObjectReference) -> bool {
    use crate::mmtk::SFT_MAP;
    SFT_MAP
        .get_checked(object.to_address::<VM>())
        .is_object_pinned(object)
}

/// Get an object that is ready for finalization. After each GC, if any registered object is not
/// alive, this call will return one of the objects. MMTk will retain the liveness of those objects
/// until they are popped through this call. Once an object is popped, it is the responsibility of
/// the VM to make sure they are properly finalized before reclaimed by the GC. This call is non-blocking,
/// and will return None if no object is ready for finalization.
///
/// Arguments:
/// * `mmtk`: A reference to an MMTk instance.
pub fn get_finalized_object<VM: VMBinding>(
    mmtk: &'static MMTK<VM>,
) -> Option<<VM::VMReferenceGlue as ReferenceGlue<VM>>::FinalizableType> {
    if *mmtk.options.no_finalizer {
        warn!("get_finalized_object() is called when no_finalizer = true");
    }

    mmtk.finalizable_processor
        .lock()
        .unwrap()
        .get_ready_object()
}

/// Pop all the finalizers that were registered for finalization. The returned objects may or may not be ready for
/// finalization. After this call, MMTk's finalizer processor should have no registered finalizer any more.
///
/// This is useful for some VMs which require all finalizable objects to be finalized on exit.
///
/// Arguments:
/// * `mmtk`: A reference to an MMTk instance.
pub fn get_all_finalizers<VM: VMBinding>(
    mmtk: &'static MMTK<VM>,
) -> Vec<<VM::VMReferenceGlue as ReferenceGlue<VM>>::FinalizableType> {
    if *mmtk.options.no_finalizer {
        warn!("get_all_finalizers() is called when no_finalizer = true");
    }

    mmtk.finalizable_processor
        .lock()
        .unwrap()
        .get_all_finalizers()
}

/// Pop finalizers that were registered and associated with a certain object. The returned objects may or may not be ready for finalization.
/// This is useful for some VMs that may manually execute finalize method for an object.
///
/// Arguments:
/// * `mmtk`: A reference to an MMTk instance.
/// * `object`: the given object that MMTk will pop its finalizers
pub fn get_finalizers_for<VM: VMBinding>(
    mmtk: &'static MMTK<VM>,
    object: ObjectReference,
) -> Vec<<VM::VMReferenceGlue as ReferenceGlue<VM>>::FinalizableType> {
    if *mmtk.options.no_finalizer {
        warn!("get_finalizers() is called when no_finalizer = true");
    }

    mmtk.finalizable_processor
        .lock()
        .unwrap()
        .get_finalizers_for(object)
}

/// Get the number of workers. MMTk spawns worker threads for the 'threads' defined in the options.
/// So the number of workers is derived from the threads option. Note the feature single_worker overwrites
/// the threads option, and force one worker thread.
///
/// Arguments:
/// * `mmtk`: A reference to an MMTk instance.
pub fn num_of_workers<VM: VMBinding>(mmtk: &'static MMTK<VM>) -> usize {
    mmtk.scheduler.num_workers()
}

/// Add a work packet to the given work bucket. Note that this simply adds the work packet to the given
/// work bucket, and the scheduler will decide when to execute the work packet.
///
/// Arguments:
/// * `mmtk`: A reference to an MMTk instance.
/// * `bucket`: Which work bucket to add this packet to.
/// * `packet`: The work packet to be added.
pub fn add_work_packet<VM: VMBinding, W: GCWork<VM>>(
    mmtk: &'static MMTK<VM>,
    bucket: WorkBucketStage,
    packet: W,
) {
    mmtk.scheduler.work_buckets[bucket].add(packet)
}

/// Bulk add a number of work packets to the given work bucket. Note that this simply adds the work packets
/// to the given work bucket, and the scheduler will decide when to execute the work packets.
///
/// Arguments:
/// * `mmtk`: A reference to an MMTk instance.
/// * `bucket`: Which work bucket to add these packets to.
/// * `packet`: The work packets to be added.
pub fn add_work_packets<VM: VMBinding>(
    mmtk: &'static MMTK<VM>,
    bucket: WorkBucketStage,
    packets: Vec<Box<dyn GCWork<VM>>>,
) {
    mmtk.scheduler.work_buckets[bucket].bulk_add(packets)
}

#[cfg(feature = "public_bit")]
pub fn mmtk_set_public_bit<VM: VMBinding>(
    #[cfg(feature = "publish_rate_analysis")] _tls: VMMutatorThread,
    _mmtk: &'static MMTK<VM>,
    object: ObjectReference,
) {
    crate::util::metadata::public_bit::set_public_bit::<VM>(object);
    #[cfg(feature = "thread_local_gc")]
    _mmtk.get_plan().publish_object(object);
    #[cfg(any(eature = "publish_rate_analysis", feature = "allocation_strats"))]
    {
        // use crate::vm::ActivePlan;
        use crate::vm::ObjectModel;
        use crate::PUBLICATION_COUNT;
        use crate::PUBLICATION_SIZE;
        use crate::REQUEST_SCOPE_BARRIER_SLOW_PATH_COUNT;
        use crate::REQUEST_SCOPE_PUBLICATION_COUNT;
        use crate::REQUEST_SCOPE_PUBLICATION_SIZE;

        let object_size = VM::VMObjectModel::get_current_size(object);
        // let mutator_id = if VM::VMActivePlan::is_mutator(tls.0) {
        //     VM::VMActivePlan::mutator(tls).mutator_id
        // } else {
        //     0
        // };

        PUBLICATION_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        PUBLICATION_SIZE.fetch_add(object_size, std::sync::atomic::Ordering::SeqCst);
        REQUEST_SCOPE_PUBLICATION_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        REQUEST_SCOPE_PUBLICATION_SIZE.fetch_add(object_size, std::sync::atomic::Ordering::SeqCst);
        REQUEST_SCOPE_BARRIER_SLOW_PATH_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        // let mut stats = crate::PER_THREAD_PUBLICATION_STATS_MAP.lock().unwrap();
        // let s = stats.get_mut(&mutator_id).unwrap();
        // s.publication_count += 1;
        // s.publication_size += object_size;
        // s.request_scope_publication_count += 1;
        // s.request_scope_publication_size += object_size;
    }
}

#[cfg(feature = "public_bit")]
pub fn mmtk_publish_object<VM: VMBinding>(
    #[cfg(feature = "publish_rate_analysis")] tls: VMMutatorThread,
    _mmtk: &'static MMTK<VM>,
    _object: Option<ObjectReference>,
) {
    // use crate::vm::Scanning;

    if let Some(object) = _object {
        if crate::util::metadata::public_bit::is_public::<VM>(object) {
            return;
        }
        #[cfg(feature = "publish_rate_analysis")]
        crate::REQUEST_SCOPE_BARRIER_SLOW_PATH_COUNT
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let mut closure: crate::plan::PublishObjectClosure<VM> =
            crate::plan::PublishObjectClosure::<VM>::new(
                _mmtk,
                #[cfg(feature = "debug_publish_object")]
                u32::MAX,
            );

        // mmtk_set_public_bit(_mmtk, object);
        // #[cfg(feature = "thread_local_gc")]
        // _mmtk.get_plan().publish_object(object);
        // // Publish all the descendants
        // VM::VMScanning::scan_object(
        //     VMWorkerThread(VMThread::UNINITIALIZED),
        //     object,
        //     &mut closure,
        // );
        closure.do_closure(
            object,
            #[cfg(feature = "publish_rate_analysis")]
            tls,
        );
    }
}

#[cfg(feature = "public_bit")]
pub fn mmtk_is_object_published<VM: VMBinding>(_object: Option<ObjectReference>) -> bool {
    if let Some(object) = _object {
        crate::util::metadata::public_bit::is_public::<VM>(object)
    } else {
        false
    }
}

#[cfg(feature = "publish_rate_analysis")]
pub fn mmtk_register_mutator_name<VM: VMBinding>(tls: VMMutatorThread) {
    use crate::vm::ActivePlan;

    let name = VM::VMActivePlan::get_mutator_name(tls);
    let mutator = VM::VMActivePlan::mutator(tls);
    crate::MUTATOR_NAME_MAP
        .lock()
        .unwrap()
        .insert(mutator.mutator_id, name);
}

#[cfg(feature = "publish_rate_analysis")]
pub fn mmtk_mutator_exit<VM: VMBinding>(_tls: VMMutatorThread, _mmtk: &'static MMTK<VM>) {
    use crate::vm::ActivePlan;
    if _mmtk
        .inside_harness
        .load(std::sync::atomic::Ordering::Acquire)
        == false
    {
        let mutator = VM::VMActivePlan::mutator(_tls);
        crate::MUTATOR_NAME_MAP
            .lock()
            .unwrap()
            .remove(&mutator.mutator_id);
        // crate::PER_THREAD_PUBLICATION_STATS_MAP
        //     .lock()
        //     .unwrap()
        //     .remove(&mutator.mutator_id);
    }
}
/// Generic hook to allow benchmarks to be harnessed. We do a full heap
/// GC, and then start recording statistics for MMTk.
///
/// Arguments:
/// * `mmtk`: A reference to an MMTk instance.
/// * `tls`: The thread that calls the function (and triggers a collection).
pub fn request_starting<VM: VMBinding>(_mmtk: &'static MMTK<VM>) {
    #[cfg(any(feature = "publish_rate_analysis", feature = "allocation_stats"))]
    {
        use crate::REQUEST_SCOPE_ALLOCATION_COUNT;
        use crate::REQUEST_SCOPE_ALLOCATION_SIZE;
        use crate::REQUEST_SCOPE_BARRIER_SLOW_PATH_COUNT;
        use crate::REQUEST_SCOPE_PUBLICATION_COUNT;
        use crate::REQUEST_SCOPE_PUBLICATION_SIZE;
        use std::sync::atomic::Ordering::Release;

        REQUEST_SCOPE_ALLOCATION_COUNT.store(0, Release);
        REQUEST_SCOPE_ALLOCATION_SIZE.store(0, Release);
        REQUEST_SCOPE_PUBLICATION_COUNT.store(0, Release);
        REQUEST_SCOPE_PUBLICATION_SIZE.store(0, Release);
        REQUEST_SCOPE_BARRIER_SLOW_PATH_COUNT.store(0, Release);

        // let mut stats = crate::PER_THREAD_PUBLICATION_STATS_MAP.lock().unwrap();
        // for s in stats.values_mut() {
        //     s.request_scope_allocation_count = 0;
        //     s.request_scope_allocation_size = 0;
        //     s.request_scope_publication_count = 0;
        //     s.request_scope_publication_size = 0;
        // }
    }
    _mmtk.request_starting();
}

/// Generic hook to allow benchmarks to be harnessed. We stop collecting
/// statistics, and print stats values.
///
/// Arguments:
/// * `mmtk`: A reference to an MMTk instance.
pub fn request_finished<VM: VMBinding>(_mmtk: &'static MMTK<VM>) {
    _mmtk.request_finished();
}
