use super::defrag::StatsForDefrag;
use super::line::*;
use super::{block::*, defrag::Defrag};
#[cfg(feature = "thread_local_gc")]
use crate::plan::ThreadlocalTracedObjectType;
use crate::plan::VectorObjectQueue;
use crate::policy::gc_work::{TraceKind, TRACE_KIND_TRANSITIVE_PIN};
use crate::policy::sft::GCWorkerMutRef;
use crate::policy::sft::SFT;
use crate::policy::sft_map::SFTMap;
use crate::policy::space::{CommonSpace, Space};
use crate::util::alloc::allocator::AllocatorContext;
use crate::util::alloc::immix_allocator::ImmixAllocSemantics;
use crate::util::constants::LOG_BYTES_IN_PAGE;
use crate::util::copy::*;
use crate::util::heap::chunk_map::*;
use crate::util::heap::BlockPageResource;
use crate::util::heap::PageResource;
use crate::util::linear_scan::{Region, RegionIterator};
use crate::util::metadata::side_metadata::SideMetadataSpec;
#[cfg(feature = "vo_bit")]
use crate::util::metadata::vo_bit;
use crate::util::metadata::{self, MetadataSpec};
use crate::util::object_forwarding;
use crate::util::{Address, ObjectReference};
use crate::vm::*;
#[cfg(feature = "thread_local_gc")]
use crate::Mutator;
use crate::{
    plan::ObjectQueue,
    scheduler::{GCWork, GCWorkScheduler, GCWorker, WorkBucketStage},
    util::opaque_pointer::{VMThread, VMWorkerThread},
    MMTK,
};
use atomic::Ordering;
use std::sync::{atomic::AtomicU8, atomic::AtomicUsize, Arc};

pub(crate) const TRACE_KIND_FAST: TraceKind = 0;
pub(crate) const TRACE_KIND_DEFRAG: TraceKind = 1;
pub(crate) const TRACE_THREAD_LOCAL_FAST: TraceKind = 2;
pub(crate) const TRACE_THREAD_LOCAL_DEFRAG: TraceKind = 3;

pub struct ImmixSpace<VM: VMBinding> {
    common: CommonSpace<VM>,
    pr: BlockPageResource<VM, Block>,
    /// Allocation status for all chunks in immix space
    pub chunk_map: ChunkMap,
    /// Current line mark state
    pub line_mark_state: AtomicU8,
    /// Line mark state in previous GC
    pub line_unavail_state: AtomicU8,
    /// A list of all reusable blocks
    pub reusable_blocks: ReusableBlockPool,
    /// Defrag utilities
    pub defrag: Defrag,
    /// How many lines have been consumed since last GC?
    lines_consumed: AtomicUsize,
    /// Object mark state
    mark_state: u8,
    /// Work packet scheduler
    scheduler: Arc<GCWorkScheduler<VM>>,
    /// Some settings for this space
    space_args: ImmixSpaceArgs,
    #[cfg(feature = "thread_local_gc_copying")]
    pub(crate) bytes_published: AtomicUsize,
    #[cfg(feature = "thread_local_gc_copying")]
    pub sparse_reusable_blocks: ReusableBlockPool,
    // #[cfg(feature = "thread_local_gc_copying")]
    // pub(super) bytes_copied: AtomicUsize,
    // #[cfg(feature = "thread_local_gc_copying")]
    // blocks_acquired_for_evacuation: AtomicUsize,
    // #[cfg(feature = "thread_local_gc_copying")]
    // blocks_freed: AtomicUsize,
    // pub log_buffer: crossbeam::queue::SegQueue<(
    //     usize,
    //     ObjectReference,
    //     Address,
    //     bool,
    //     ObjectReference,
    //     Address,
    //     bool,
    // )>,
}

/// Some arguments for Immix Space.
pub struct ImmixSpaceArgs {
    /// Mark an object as unlogged when we trace an object.
    /// Normally we set the log bit when we copy an object with [`crate::util::copy::CopySemantics::PromoteToMature`].
    /// In sticky immix, we 'promote' an object to mature when we trace the object
    /// (no matter we copy an object or not). So we have to use `PromoteToMature`, and instead
    /// just set the log bit in the space when an object is traced.
    pub unlog_object_when_traced: bool,
    /// Reset log bit at the start of a major GC.
    /// Normally we do not need to do this. When immix is used as the mature space,
    /// any object should be set as unlogged, and that bit does not need to be cleared
    /// even if the object is dead. But in sticky Immix, the mature object and
    /// the nursery object are in the same space, we will have to use the
    /// bit to differentiate them. So we reset all the log bits in major GCs,
    /// and unlogged the objects when they are traced (alive).
    pub reset_log_bit_in_major_gc: bool,
    /// Whether this ImmixSpace instance contains both young and old objects.
    /// This affects the updating of valid-object bits.  If some lines or blocks of this ImmixSpace
    /// instance contain young objects, their VO bits need to be updated during this GC.  Currently
    /// only StickyImmix is affected.  GenImmix allocates young objects in a separete CopySpace
    /// nursery and its VO bits can be cleared in bulk.
    pub mixed_age: bool,
}

unsafe impl<VM: VMBinding> Sync for ImmixSpace<VM> {}

impl<VM: VMBinding> SFT for ImmixSpace<VM> {
    fn name(&self) -> &str {
        self.get_name()
    }

    fn get_forwarded_object(&self, object: ObjectReference) -> Option<ObjectReference> {
        // If we never move objects, look no further.
        if super::NEVER_MOVE_OBJECTS {
            return None;
        }

        if object_forwarding::is_forwarded::<VM>(object) {
            Some(object_forwarding::read_forwarding_pointer::<VM>(object))
        } else {
            None
        }
    }

    fn is_live(&self, object: ObjectReference) -> bool {
        // If the mark bit is set, it is live.
        if self.is_marked(object) {
            return true;
        }

        // If we never move objects, look no further.
        if super::NEVER_MOVE_OBJECTS {
            return false;
        }

        // If the object is forwarded, it is live, too.
        object_forwarding::is_forwarded::<VM>(object)
    }
    #[cfg(feature = "object_pinning")]
    fn pin_object(&self, object: ObjectReference) -> bool {
        VM::VMObjectModel::LOCAL_PINNING_BIT_SPEC.pin_object::<VM>(object)
    }
    #[cfg(feature = "object_pinning")]
    fn unpin_object(&self, object: ObjectReference) -> bool {
        VM::VMObjectModel::LOCAL_PINNING_BIT_SPEC.unpin_object::<VM>(object)
    }
    #[cfg(feature = "object_pinning")]
    fn is_object_pinned(&self, object: ObjectReference) -> bool {
        VM::VMObjectModel::LOCAL_PINNING_BIT_SPEC.is_object_pinned::<VM>(object)
    }
    fn is_movable(&self) -> bool {
        !super::NEVER_MOVE_OBJECTS
    }

    #[cfg(feature = "sanity")]
    fn is_sane(&self) -> bool {
        true
    }
    fn initialize_object_metadata(&self, _object: ObjectReference, _alloc: bool) {
        #[cfg(feature = "vo_bit")]
        crate::util::metadata::vo_bit::set_vo_bit::<VM>(_object);
    }
    #[cfg(feature = "is_mmtk_object")]
    fn is_mmtk_object(&self, addr: Address) -> bool {
        crate::util::metadata::vo_bit::is_vo_bit_set_for_addr::<VM>(addr).is_some()
    }
    fn sft_trace_object(
        &self,
        _queue: &mut VectorObjectQueue,
        _object: ObjectReference,
        _worker: GCWorkerMutRef,
    ) -> ObjectReference {
        panic!("We do not use SFT to trace objects for Immix. sft_trace_object() cannot be used.")
    }
}

impl<VM: VMBinding> Space<VM> for ImmixSpace<VM> {
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
    fn initialize_sft(&self, sft_map: &mut dyn SFTMap) {
        self.common().initialize_sft(self.as_sft(), sft_map)
    }
    fn release_multiple_pages(&mut self, _start: Address) {
        panic!("immixspace only releases pages enmasse")
    }
    fn set_copy_for_sft_trace(&mut self, _semantics: Option<CopySemantics>) {
        panic!("We do not use SFT to trace objects for Immix. set_copy_context() cannot be used.")
    }
}

impl<VM: VMBinding> crate::policy::gc_work::PolicyTraceObject<VM> for ImmixSpace<VM> {
    fn trace_object<Q: ObjectQueue, const KIND: TraceKind>(
        &self,
        queue: &mut Q,
        object: ObjectReference,
        copy: Option<CopySemantics>,
        worker: &mut GCWorker<VM>,
    ) -> ObjectReference {
        debug_assert!(!object.is_null());
        if KIND == TRACE_KIND_TRANSITIVE_PIN {
            self.trace_object_without_moving(queue, object, worker)
        } else if KIND == TRACE_KIND_DEFRAG {
            if Block::containing::<VM>(object).is_defrag_source() {
                #[cfg(feature = "thread_local_gc_copying")]
                debug_assert!(Block::containing::<VM>(object).is_block_published());

                debug_assert!(self.in_defrag());
                debug_assert!(
                    !crate::plan::is_nursery_gc(worker.mmtk.get_plan()),
                    "Calling PolicyTraceObject on Immix in nursery GC"
                );
                self.trace_object_with_opportunistic_copy(
                    queue,
                    object,
                    copy.unwrap(),
                    worker,
                    // This should not be nursery collection. Nursery collection does not use PolicyTraceObject.
                    false,
                )
            } else {
                self.trace_object_without_moving(queue, object, worker)
            }
        } else if KIND == TRACE_KIND_FAST {
            #[cfg(feature = "thread_local_gc")]
            assert!(
                crate::policy::immix::NEVER_MOVE_OBJECTS,
                "Moving thread-local gc is incompatible with non-moving global gc"
            );
            self.trace_object_without_moving(queue, object, worker)
        } else {
            unreachable!()
        }
    }

    fn post_scan_object(&self, object: ObjectReference) {
        if super::MARK_LINE_AT_SCAN_TIME && !super::BLOCK_ONLY {
            debug_assert!(self.in_space(object));
            self.mark_lines(object);
        }
    }

    fn may_move_objects<const KIND: TraceKind>() -> bool {
        if KIND == TRACE_KIND_DEFRAG {
            true
        } else if KIND == TRACE_KIND_FAST || KIND == TRACE_KIND_TRANSITIVE_PIN {
            false
        } else if KIND == TRACE_THREAD_LOCAL_FAST {
            false
        } else if KIND == TRACE_THREAD_LOCAL_DEFRAG {
            true
        } else {
            unreachable!()
        }
    }
}

impl<VM: VMBinding> ImmixSpace<VM> {
    #[allow(unused)]
    const UNMARKED_STATE: u8 = 0;
    const MARKED_STATE: u8 = 1;

    /// Get side metadata specs
    fn side_metadata_specs() -> Vec<SideMetadataSpec> {
        metadata::extract_side_metadata(&if super::BLOCK_ONLY {
            vec![
                #[cfg(feature = "thread_local_gc")]
                MetadataSpec::OnSide(Block::METADATA_TABLE),
                #[cfg(feature = "thread_local_gc_copying")]
                MetadataSpec::OnSide(Block::HOLE_SIZE),
                #[cfg(all(feature = "thread_local_gc", debug_assertions))]
                MetadataSpec::OnSide(Block::OWNER_TABLE),
                MetadataSpec::OnSide(Block::DEFRAG_STATE_TABLE),
                MetadataSpec::OnSide(Block::MARK_TABLE),
                MetadataSpec::OnSide(ChunkMap::ALLOC_TABLE),
                *VM::VMObjectModel::LOCAL_MARK_BIT_SPEC,
                *VM::VMObjectModel::LOCAL_FORWARDING_BITS_SPEC,
                *VM::VMObjectModel::LOCAL_FORWARDING_POINTER_SPEC,
                #[cfg(feature = "object_pinning")]
                *VM::VMObjectModel::LOCAL_PINNING_BIT_SPEC,
            ]
        } else {
            vec![
                #[cfg(feature = "thread_local_gc")]
                MetadataSpec::OnSide(Block::METADATA_TABLE),
                #[cfg(feature = "thread_local_gc_copying")]
                MetadataSpec::OnSide(Block::HOLE_SIZE),
                #[cfg(all(feature = "thread_local_gc", debug_assertions))]
                MetadataSpec::OnSide(Block::OWNER_TABLE),
                #[cfg(feature = "thread_local_gc")]
                MetadataSpec::OnSide(Line::LINE_PUBLICATION_TABLE),
                MetadataSpec::OnSide(Line::MARK_TABLE),
                MetadataSpec::OnSide(Block::DEFRAG_STATE_TABLE),
                MetadataSpec::OnSide(Block::MARK_TABLE),
                MetadataSpec::OnSide(ChunkMap::ALLOC_TABLE),
                *VM::VMObjectModel::LOCAL_MARK_BIT_SPEC,
                *VM::VMObjectModel::LOCAL_FORWARDING_BITS_SPEC,
                *VM::VMObjectModel::LOCAL_FORWARDING_POINTER_SPEC,
                #[cfg(feature = "object_pinning")]
                *VM::VMObjectModel::LOCAL_PINNING_BIT_SPEC,
            ]
        })
    }

    pub fn new(
        args: crate::policy::space::PlanCreateSpaceArgs<VM>,
        space_args: ImmixSpaceArgs,
    ) -> Self {
        #[cfg(feature = "immix_non_moving")]
        info!(
            "Creating non-moving ImmixSpace: {}. Block size: 2^{}",
            args.name,
            Block::LOG_BYTES
        );

        if space_args.unlog_object_when_traced || space_args.reset_log_bit_in_major_gc {
            assert!(
                args.constraints.needs_log_bit,
                "Invalid args when the plan does not use log bit"
            );
        }

        super::validate_features();
        #[cfg(feature = "vo_bit")]
        vo_bit::helper::validate_config::<VM>();
        let vm_map = args.vm_map;
        let scheduler = args.scheduler.clone();
        let common =
            CommonSpace::new(args.into_policy_args(true, false, Self::side_metadata_specs()));
        #[cfg(not(feature = "thread_local_gc"))]
        return ImmixSpace {
            pr: if common.vmrequest.is_discontiguous() {
                BlockPageResource::new_discontiguous(
                    Block::LOG_PAGES,
                    vm_map,
                    scheduler.num_workers(),
                )
            } else {
                BlockPageResource::new_contiguous(
                    Block::LOG_PAGES,
                    common.start,
                    common.extent,
                    vm_map,
                    scheduler.num_workers(),
                )
            },
            common,
            chunk_map: ChunkMap::new(),
            line_mark_state: AtomicU8::new(Line::RESET_MARK_STATE),
            line_unavail_state: AtomicU8::new(Line::RESET_MARK_STATE),
            lines_consumed: AtomicUsize::new(0),
            reusable_blocks: ReusableBlockPool::new(scheduler.num_workers()),
            defrag: Defrag::default(),
            // Set to the correct mark state when inititialized. We cannot rely on prepare to set it (prepare may get skipped in nursery GCs).
            mark_state: Self::MARKED_STATE,
            scheduler: scheduler.clone(),
            space_args,
        };
        #[cfg(feature = "thread_local_gc")]
        ImmixSpace {
            pr: if common.vmrequest.is_discontiguous() {
                BlockPageResource::new_discontiguous(
                    Block::LOG_PAGES,
                    vm_map,
                    scheduler.num_workers(),
                )
            } else {
                BlockPageResource::new_contiguous(
                    Block::LOG_PAGES,
                    common.start,
                    common.extent,
                    vm_map,
                    scheduler.num_workers(),
                )
            },
            common,
            chunk_map: ChunkMap::new(),
            line_mark_state: AtomicU8::new(Line::RESET_MARK_STATE),
            line_unavail_state: AtomicU8::new(Line::RESET_MARK_STATE),
            lines_consumed: AtomicUsize::new(0),
            reusable_blocks: ReusableBlockPool::new(scheduler.num_workers()),
            defrag: Defrag::default(),
            // Set to the correct mark state when inititialized. We cannot rely on prepare to set it (prepare may get skipped in nursery GCs).
            mark_state: Self::MARKED_STATE,
            scheduler: scheduler.clone(),
            space_args,
            #[cfg(feature = "thread_local_gc_copying")]
            bytes_published: AtomicUsize::new(0),
            #[cfg(feature = "thread_local_gc_copying")]
            sparse_reusable_blocks: ReusableBlockPool::new(scheduler.num_workers()),
            // #[cfg(feature = "thread_local_gc_copying")]
            // bytes_copied: AtomicUsize::new(0),
            // #[cfg(feature = "thread_local_gc_copying")]
            // blocks_acquired_for_evacuation: AtomicUsize::new(0),
            // #[cfg(feature = "thread_local_gc_copying")]
            // blocks_freed: AtomicUsize::new(0),
            // log_buffer: crossbeam::queue::SegQueue::new(),
        }
    }

    /// Flush the thread-local queues in BlockPageResource
    pub fn flush_page_resource(&self) {
        self.reusable_blocks.flush_all();
        #[cfg(feature = "thread_local_gc_copying")]
        self.sparse_reusable_blocks.flush_all();
        #[cfg(target_pointer_width = "64")]
        self.pr.flush_all()
    }

    /// Get the number of defrag headroom pages.
    pub fn defrag_headroom_pages(&self) -> usize {
        self.defrag.defrag_headroom_pages(self)
    }

    /// Check if current GC is a defrag GC.
    pub fn in_defrag(&self) -> bool {
        self.defrag.in_defrag()
    }

    /// check if the current GC should do defragmentation.
    pub fn decide_whether_to_defrag(
        &self,
        emergency_collection: bool,
        collect_whole_heap: bool,
        collection_attempts: usize,
        user_triggered_collection: bool,
        full_heap_system_gc: bool,
    ) -> bool {
        self.defrag.decide_whether_to_defrag(
            emergency_collection,
            collect_whole_heap,
            collection_attempts,
            user_triggered_collection,
            self.reusable_blocks.len() == 0,
            full_heap_system_gc,
        );
        self.defrag.in_defrag()
    }

    // #[cfg(feature = "thread_local_gc_copying")]
    // pub fn thread_local_gc_copy_reserve_pages(&self) -> usize {
    //     use super::LOCAL_GC_COPY_RESERVE_PAGES;
    //     LOCAL_GC_COPY_RESERVE_PAGES.load(Ordering::SeqCst)
    // }

    // #[cfg(feature = "thread_local_gc_copying")]
    // pub fn public_object_reserved_pages(&self) -> usize {
    //     let bytes_copied = self.bytes_copied.load(Ordering::Relaxed);
    //     let bytes_published = self.bytes_published.load(Ordering::SeqCst);
    //     if bytes_copied != 0 {
    //         bytes_copied / crate::util::constants::BYTES_IN_PAGE
    //     } else {
    //         bytes_published / crate::util::constants::BYTES_IN_PAGE
    //     }
    // }

    /// Get work packet scheduler
    fn scheduler(&self) -> &GCWorkScheduler<VM> {
        &self.scheduler
    }

    pub fn prepare(&mut self, major_gc: bool, plan_stats: StatsForDefrag) {
        if major_gc {
            // Update mark_state
            if VM::VMObjectModel::LOCAL_MARK_BIT_SPEC.is_on_side() {
                self.mark_state = Self::MARKED_STATE;
            } else {
                // For header metadata, we use cyclic mark bits.
                unimplemented!("cyclic mark bits is not supported at the moment");
            }

            // Prepare defrag info
            if super::DEFRAG {
                self.defrag.prepare(self, plan_stats);
            }

            // Prepare each block for GC
            let threshold = self.defrag.defrag_spill_threshold.load(Ordering::Acquire);
            // # Safety: ImmixSpace reference is always valid within this collection cycle.
            let space = unsafe { &*(self as *const Self) };
            let work_packets = self.chunk_map.generate_tasks(|chunk| {
                Box::new(PrepareBlockState {
                    space,
                    chunk,
                    defrag_threshold: if space.in_defrag() {
                        Some(threshold)
                    } else {
                        None
                    },
                })
            });
            self.scheduler().work_buckets[WorkBucketStage::Prepare].bulk_add(work_packets);

            if !super::BLOCK_ONLY {
                self.line_mark_state.fetch_add(1, Ordering::AcqRel);
                if self.line_mark_state.load(Ordering::Acquire) > Line::MAX_MARK_STATE {
                    self.line_mark_state
                        .store(Line::RESET_MARK_STATE, Ordering::Release);
                }
            }
        }

        #[cfg(feature = "thread_local_gc_copying")]
        {
            self.bytes_published.store(0, Ordering::SeqCst);
            // self.blocks_acquired_for_evacuation
            //     .store(0, Ordering::Relaxed);
            // self.blocks_freed.store(0, Ordering::Relaxed);
            // reset local copy reserve, it will be recalculated at the end of the gc
            crate::policy::immix::LOCAL_GC_COPY_RESERVE_PAGES.store(0, Ordering::Relaxed);
        }

        #[cfg(feature = "thread_local_gc_copying")]
        // self.bytes_copied.store(0, Ordering::SeqCst);
        #[cfg(feature = "vo_bit")]
        if vo_bit::helper::need_to_clear_vo_bits_before_tracing::<VM>() {
            let maybe_scope = if major_gc {
                // If it is major GC, we always clear all VO bits because we are doing full-heap
                // tracing.
                Some(VOBitsClearingScope::FullGC)
            } else if self.space_args.mixed_age {
                // StickyImmix nursery GC.
                // Some lines (or blocks) contain only young objects,
                // while other lines (or blocks) contain only old objects.
                if super::BLOCK_ONLY {
                    // Block only.  Young objects are only allocated into fully empty blocks.
                    // Only clear unmarked blocks.
                    Some(VOBitsClearingScope::BlockOnly)
                } else {
                    // Young objects are allocated into empty lines.
                    // Only clear unmarked lines.
                    let line_mark_state = self.line_mark_state.load(Ordering::SeqCst);
                    Some(VOBitsClearingScope::Line {
                        state: line_mark_state,
                    })
                }
            } else {
                // GenImmix nursery GC.  We do nothing to the ImmixSpace because the nursery is a
                // separate CopySpace.  It'll clear its own VO bits.
                None
            };

            if let Some(scope) = maybe_scope {
                let work_packets = self
                    .chunk_map
                    .generate_tasks(|chunk| Box::new(ClearVOBitsAfterPrepare { chunk, scope }));
                self.scheduler.work_buckets[WorkBucketStage::ClearVOBits].bulk_add(work_packets);
            }
        }
    }

    /// Release for the immix space. This is called when a GC finished.
    /// Return whether this GC was a defrag GC, as a plan may want to know this.
    pub fn release(&mut self, major_gc: bool) -> bool {
        let did_defrag = self.defrag.in_defrag();
        if major_gc {
            // Update line_unavail_state for hole searching afte this GC.
            if !super::BLOCK_ONLY {
                self.line_unavail_state.store(
                    self.line_mark_state.load(Ordering::Acquire),
                    Ordering::Release,
                );
            }
        }
        // Clear reusable blocks list
        if !super::BLOCK_ONLY {
            self.reusable_blocks.reset();
            self.sparse_reusable_blocks.reset();
        }
        // Sweep chunks and blocks
        let work_packets = self.generate_sweep_tasks();
        self.scheduler().work_buckets[WorkBucketStage::Release].bulk_add(work_packets);
        if super::DEFRAG {
            self.defrag.release(self);
        }

        self.lines_consumed.store(0, Ordering::Relaxed);

        // {
        //     use std::{fs::OpenOptions, io::Write};
        //     use std::time::UNIX_EPOCH;
        //     let mut f = OpenOptions::new()
        //         .create(true)
        //         .append(true)
        //         .open(format!(
        //             "/home/tianleq/misc/global-gc-debug-logs/gc-log-{:?}",
        //             std::time::SystemTime::now()
        //                 .duration_since(UNIX_EPOCH)
        //                 .unwrap()
        //                 .as_millis()
        //         ))
        //         .unwrap();
        //     while self.log_buffer.is_empty() == false {
        //         let log = self.log_buffer.pop().unwrap();
        //         writeln!(
        //             f,
        //             "w: {:?}, old: {:?}|{:?}|{}, new: {:?}|{:?}|{}",
        //             log.0, log.1, log.2, log.3, log.4, log.5, log.6
        //         )
        //         .unwrap();
        //     }
        // }

        #[cfg(feature = "debug_thread_local_gc_copying")]
        {
            use crate::util::GLOBAL_GC_STATISTICS;

            let mut guard = GLOBAL_GC_STATISTICS.lock().unwrap();
            guard.bytes_copied += self.bytes_copied.load(Ordering::SeqCst);
        }
        // info!(
        //     "{} bytes copied, that is roughly {} pages | {} clean pages acquired",
        //     self.bytes_copied.load(Ordering::Relaxed),
        //     1 + self.bytes_copied.load(Ordering::Relaxed) / BYTES_IN_PAGE,
        //     self.blocks_acquired_for_evacuation.load(Ordering::Relaxed) << Block::LOG_PAGES,
        // );
        did_defrag
    }

    /// Generate chunk sweep tasks
    fn generate_sweep_tasks(&self) -> Vec<Box<dyn GCWork<VM>>> {
        self.defrag.mark_histograms.lock().clear();
        // # Safety: ImmixSpace reference is always valid within this collection cycle.
        let space = unsafe { &*(self as *const Self) };
        let epilogue = Arc::new(FlushPageResource {
            space,
            counter: AtomicUsize::new(0),
            #[cfg(feature = "thread_local_gc")]
            scheduler: self.scheduler.clone(),
        });
        let tasks = self.chunk_map.generate_tasks(|chunk| {
            Box::new(SweepChunk {
                space,
                chunk,
                epilogue: epilogue.clone(),
            })
        });
        epilogue.counter.store(tasks.len(), Ordering::SeqCst);
        tasks
    }

    /// Release a block.
    pub fn release_block(&self, block: Block) {
        #[cfg(debug_assertions)]
        debug_assert!(block.owner() == 0);
        block.deinit();
        self.pr.release_block(block);
    }

    #[cfg(feature = "thread_local_gc")]
    pub fn thread_local_release_blocks(&self, blocks: impl IntoIterator<Item = Block>) {
        self.pr.thread_local_flush(blocks);
    }

    /// Allocate a clean block.
    pub fn get_clean_block(&self, tls: VMThread, copy: bool) -> Option<Block> {
        let block_address = self.acquire(tls, Block::PAGES);
        if block_address.is_zero() {
            return None;
        }
        // #[cfg(feature = "thread_local_gc_copying")]
        // {
        //     if copy {
        //         self.blocks_acquired_for_evacuation
        //             .fetch_add(1, Ordering::SeqCst);
        //     }
        // }
        self.defrag.notify_new_clean_block(copy);
        let block = Block::from_aligned_address(block_address);
        block.init(copy);
        self.chunk_map.set(block.chunk(), ChunkState::Allocated);
        self.lines_consumed
            .fetch_add(Block::LINES, Ordering::SeqCst);

        Some(block)
    }

    #[cfg(feature = "thread_local_gc_copying")]
    /// Allocate n clean blocks.
    pub fn get_clean_blocks(&self, tls: VMThread, copy: bool, n: usize) -> Option<Vec<Block>> {
        let address = self.acquire(tls, n * Block::PAGES);
        if address.is_zero() {
            return None;
        }
        // #[cfg(feature = "thread_local_gc_copying")]
        // {
        //     if copy {
        //         self.blocks_acquired_for_evacuation
        //             .fetch_add(n, Ordering::SeqCst);
        //     }
        // }
        let mut blocks = Vec::with_capacity(n);
        for i in 0..n {
            self.defrag.notify_new_clean_block(copy);
            let block = Block::from_aligned_address(address + i * Block::BYTES);
            block.init(copy);
            self.chunk_map.set(block.chunk(), ChunkState::Allocated);
            self.lines_consumed
                .fetch_add(Block::LINES, Ordering::SeqCst);
            blocks.push(block);
        }

        Some(blocks)
    }

    #[cfg(not(feature = "thread_local_gc_ibm_style"))]
    pub fn get_reusable_block(&self, copy: bool) -> Option<Block> {
        self.get_reusable_block_impl(copy, false)
    }

    #[cfg(feature = "thread_local_gc_copying")]
    pub fn get_sparse_reusable_block(&self, copy: bool) -> Option<Block> {
        self.get_reusable_block_impl(copy, true)
    }

    #[cfg(not(feature = "thread_local_gc_ibm_style"))]
    fn get_reusable_block_impl(&self, copy: bool, sparse: bool) -> Option<Block> {
        if super::BLOCK_ONLY {
            return None;
        }
        loop {
            let b = if sparse {
                self.sparse_reusable_blocks.pop()
            } else {
                self.reusable_blocks.pop()
            };
            if let Some(block) = b {
                // Skip blocks that should be evacuated.
                if copy && block.is_defrag_source() {
                    continue;
                }

                // Get available lines. Do this before block.init which will reset block state.
                let lines_delta = match block.get_state() {
                    BlockState::Reusable { unavailable_lines } => {
                        Block::LINES - unavailable_lines as usize
                    }
                    BlockState::Unmarked => Block::LINES,
                    _ => unreachable!("{:?} {:?}", block, block.get_state()),
                };
                self.lines_consumed.fetch_add(lines_delta, Ordering::SeqCst);
                #[cfg(all(feature = "thread_local_gc_copying", debug_assertions))]
                {
                    if !copy {
                        block.get_reusable_block_info(
                            self.line_unavail_state.load(Ordering::Acquire),
                            self.line_mark_state.load(Ordering::Acquire),
                        );
                    }
                }

                block.init(copy);
                return Some(block);
            } else {
                return None;
            }
        }
    }

    /// Trace and mark objects without evacuation.
    pub fn trace_object_without_moving(
        &self,
        queue: &mut impl ObjectQueue,
        object: ObjectReference,
        _worker: &mut crate::scheduler::GCWorker<VM>,
    ) -> ObjectReference {
        #[cfg(feature = "vo_bit")]
        vo_bit::helper::on_trace_object::<VM>(object);

        #[cfg(all(debug_assertions, feature = "thread_local_gc"))]
        {
            if crate::util::metadata::public_bit::is_public::<VM>(object) {
                debug_assert!(
                    Block::containing::<VM>(object).is_block_published(),
                    "public block is corrupted"
                );
            }
        }

        // if object.to_address::<VM>().class_is_valid::<VM>() == false {
        //     println!(
        //         "worker: {:?}, object: {:?}, klass: {:?}",
        //         _worker.ordinal,
        //         object,
        //         object.to_address::<VM>().class_pointer::<VM>()
        //     );
        //     // while self.log_buffer.is_empty() == false {
        //     //     let log = self.log_buffer.pop().unwrap();
        //     //     println!(
        //     //         "w: {:?}, old: {:?}|{:?}|{}, new: {:?}|{:?}|{}",
        //     //         log.0, log.1, log.2, log.3, log.4, log.5, log.6
        //     //     )
        //     // }
        //     panic!();
        // }

        if self.attempt_mark(object, self.mark_state) {
            let block: Block = Block::containing::<VM>(object);

            // Mark block and lines
            if !super::BLOCK_ONLY {
                if !super::MARK_LINE_AT_SCAN_TIME {
                    self.mark_lines(object);
                }
            } else {
                block.set_state(BlockState::Marked);
            }

            #[cfg(feature = "thread_local_gc_copying")]
            if crate::util::metadata::public_bit::is_public::<VM>(object) {
                self.bytes_published.fetch_add(
                    VM::VMObjectModel::get_current_size(object),
                    Ordering::SeqCst,
                );
            }

            // Visit node
            queue.enqueue(object);
            self.unlog_object_if_needed(object);
            return object;
        }
        object
    }

    /// Trace object and do evacuation if required.
    #[allow(clippy::assertions_on_constants)]
    pub fn trace_object_with_opportunistic_copy(
        &self,
        queue: &mut impl ObjectQueue,
        object: ObjectReference,
        semantics: CopySemantics,
        worker: &mut GCWorker<VM>,
        nursery_collection: bool,
    ) -> ObjectReference {
        let copy_context = worker.get_copy_context_mut();
        debug_assert!(!super::BLOCK_ONLY);

        #[cfg(feature = "vo_bit")]
        vo_bit::helper::on_trace_object::<VM>(object);

        #[cfg(feature = "thread_local_gc")]
        let is_private_object = !crate::util::metadata::public_bit::is_public::<VM>(object);
        #[cfg(not(feature = "thread_local_gc"))]
        let is_private_object = false;

        #[cfg(all(debug_assertions, feature = "thread_local_gc"))]
        {
            let is_published = Block::containing::<VM>(object).is_block_published();
            if !is_private_object {
                debug_assert!(is_published, "public block is corrupted");
            } else {
                #[cfg(feature = "debug_publish_object")]
                {
                    let owner = Block::containing::<VM>(object).owner();
                    let metadata =
                        crate::util::object_extra_header_metadata::get_extra_header_metadata::<
                            VM,
                            usize,
                        >(object)
                            & crate::util::object_extra_header_metadata::BOTTOM_HALF_MASK;
                    if metadata != usize::try_from(owner).unwrap() {
                        // private objects can live in reusable public blocks
                        debug_assert!(
                            metadata
                                == usize::try_from(owner).unwrap() || is_published,
                            "object: {:?}, metadata: {:?}, block owner: {:?}, forwarding status: {:?}, marked: {:?}",
                            object,
                            metadata,
                            Block::containing::<VM>(object).owner(),
                            object_forwarding::attempt_to_forward::<VM>(object),
                            self.is_marked(object)
                        );
                    }
                }
            }
        }
        // if object.to_address::<VM>().class_is_valid::<VM>() == false {
        //     println!(
        //         "worker: {:?}, object: {:?}, klass: {:?}, defrag: {}, public: {}",
        //         worker.ordinal,
        //         object,
        //         object.to_address::<VM>().class_pointer::<VM>(),
        //         Block::containing::<VM>(object).is_defrag_source(),
        //         crate::util::metadata::public_bit::is_public::<VM>(object)
        //     );
        //     // while self.log_buffer.is_empty() == false {
        //     //     let log = self.log_buffer.pop().unwrap();
        //     //     println!(
        //     //         "w: {:?}, old: {:?}|{:?}|{}, new: {:?}|{:?}|{}",
        //     //         log.0, log.1, log.2, log.3, log.4, log.5, log.6
        //     //     )
        //     // }
        //     panic!();
        // }
        let forwarding_status = object_forwarding::attempt_to_forward::<VM>(object);
        if object_forwarding::state_is_forwarded_or_being_forwarded(forwarding_status) {
            // We lost the forwarding race as some other thread has set the forwarding word; wait
            // until the object has been forwarded by the winner. Note that the object may not
            // necessarily get forwarded since Immix opportunistically moves objects.
            // if object.to_address::<VM>().class_is_valid::<VM>() == false {
            //     println!(
            //         "worker: {:?}, object: {:?}, klass: {:?}",
            //         worker.ordinal,
            //         object,
            //         object.to_address::<VM>().class_pointer::<VM>()
            //     );
            //     // while self.log_buffer.is_empty() == false {
            //     //     let log = self.log_buffer.pop().unwrap();
            //     //     println!(
            //     //         "w: {:?}, old: {:?}|{:?}|{}, new: {:?}|{:?}|{}",
            //     //         log.0, log.1, log.2, log.3, log.4, log.5, log.6
            //     //     )
            //     // }
            //     panic!();
            // }
            #[allow(clippy::let_and_return)]
            let new_object =
                object_forwarding::spin_and_get_forwarded_object::<VM>(object, forwarding_status);
            #[cfg(debug_assertions)]
            {
                if new_object == object {
                    debug_assert!(
                        self.is_marked(object) || self.defrag.space_exhausted() || self.is_pinned(object),
                        "Forwarded object is the same as original object {} even though it should have been copied",
                        object,
                    );
                } else {
                    // new_object != object
                    debug_assert!(
                        !Block::containing::<VM>(new_object).is_defrag_source(),
                        "Block {:?} containing forwarded object {} should not be a defragmentation source",
                        Block::containing::<VM>(new_object),
                        new_object,
                    );
                }
            }
            new_object
        } else if self.is_marked(object) {
            // Public object now can be left in-place

            // We won the forwarding race but the object is already marked so we clear the
            // forwarding status and return the unmoved object
            debug_assert!(
                nursery_collection || self.defrag.space_exhausted() || self.is_pinned(object) || is_private_object,
                "Forwarded object is the same as original object {} even though it should have been copied",
                object,
            );
            #[cfg(debug_assertions)]
            {
                #[cfg(all(feature = "debug_publish_object", feature = "thread_local_gc"))]
                {
                    let metadata =
                        crate::util::object_extra_header_metadata::get_extra_header_metadata::<
                            VM,
                            usize,
                        >(object)
                            & crate::util::object_extra_header_metadata::BOTTOM_HALF_MASK;
                    debug_assert!(
                        metadata
                            == usize::try_from(Block::containing::<VM>(object).owner()).unwrap()
                            || Block::containing::<VM>(object).is_block_published(),
                        "metadata: {:?}, block owner: {:?}",
                        metadata,
                        Block::containing::<VM>(object).owner()
                    );
                }
            }
            object_forwarding::clear_forwarding_bits::<VM>(object);
            object
        } else {
            // We won the forwarding race; actually forward and copy the object if it is not pinned
            // and we have sufficient space in our copy allocator
            let new_object = if self.is_pinned(object)
                || (!nursery_collection && self.defrag.space_exhausted())
                || is_private_object
            {
                // Now public object may be left in-place

                #[cfg(all(
                    feature = "debug_publish_object",
                    feature = "thread_local_gc",
                    debug_assertions
                ))]
                {
                    debug_assert!(
                        usize::try_from(Block::containing::<VM>(object).owner()).unwrap()
                            == crate::util::object_extra_header_metadata::get_extra_header_metadata::<
                                VM,
                                usize,
                            >(object)
                                & crate::util::object_extra_header_metadata::BOTTOM_HALF_MASK
                            || Block::containing::<VM>(object).is_block_published(),
                        "block owner: {} | extra header: {}",
                        Block::containing::<VM>(object).owner(),
                        crate::util::object_extra_header_metadata::get_extra_header_metadata::<
                            VM,
                            usize,
                        >(object)
                    );
                }

                self.attempt_mark(object, self.mark_state);
                object_forwarding::clear_forwarding_bits::<VM>(object);
                let block = Block::containing::<VM>(object);
                block.set_state(BlockState::Marked);
                if !is_private_object {
                    // public object is left in place, public line mark bit will be set during line marking
                    // The following assertion holds iff global reusable block is not defrag source
                    // if anything changes when determining defrag source, it needs to be revisited
                    #[cfg(debug_assertions)]
                    debug_assert!(
                        block.is_block_dirty() || block.owner() == Block::ANONYMOUS_OWNER,
                        "block: {:?} is not dirty but it is defrag source",
                        block
                    );
                    assert!(block.is_block_published());
                }

                #[cfg(feature = "vo_bit")]
                vo_bit::helper::on_object_marked::<VM>(object);

                object
            } else {
                // We are forwarding objects. When the copy allocator allocates the block, it should
                // mark the block. So we do not need to explicitly mark it here.
                #[cfg(not(feature = "thread_local_gc_copying"))]
                // Clippy complains if the "vo_bit" feature is not enabled.
                #[allow(clippy::let_and_return)]
                let new_object =
                    object_forwarding::forward_object::<VM>(object, semantics, copy_context);

                #[cfg(feature = "thread_local_gc_copying")]
                self.bytes_published.fetch_add(
                    VM::VMObjectModel::get_current_size(object),
                    Ordering::SeqCst,
                );
                // #[cfg(feature = "thread_local_gc_copying")]
                // self.bytes_copied.fetch_add(
                //     VM::VMObjectModel::get_current_size(object),
                //     Ordering::SeqCst,
                // );

                // When local gc is enabled, global gc only evacuates public object
                #[cfg(all(feature = "thread_local_gc_copying", feature = "debug_publish_object"))]
                let new_object = object_forwarding::forward_public_object::<VM>(
                    object,
                    semantics,
                    copy_context,
                    u32::MAX,
                );
                #[cfg(all(
                    feature = "thread_local_gc_copying",
                    not(feature = "debug_publish_object")
                ))]
                let new_object =
                    object_forwarding::forward_public_object::<VM>(object, semantics, copy_context);

                #[cfg(feature = "vo_bit")]
                vo_bit::helper::on_object_forwarded::<VM>(new_object);

                #[cfg(all(feature = "debug_publish_object", feature = "thread_local_gc"))]
                {
                    crate::util::object_extra_header_metadata::store_extra_header_metadata::<
                        VM,
                        usize,
                    >(new_object, usize::try_from(u32::MAX).unwrap());
                }
                new_object
            };
            debug_assert_eq!(
                Block::containing::<VM>(new_object).get_state(),
                BlockState::Marked
            );
            // self.log_buffer.push((
            //     worker.ordinal,
            //     object,
            //     object.to_address::<VM>().class_pointer::<VM>(),
            //     Block::containing::<VM>(object).is_defrag_source(),
            //     new_object,
            //     new_object.to_address::<VM>().class_pointer::<VM>(),
            //     Block::containing::<VM>(object).is_defrag_source(),
            // ));
            queue.enqueue(new_object);
            debug_assert!(new_object.is_live());
            self.unlog_object_if_needed(new_object);
            new_object
        }
    }

    #[cfg(all(feature = "thread_local_gc", not(feature = "thread_local_gc_copying")))]
    /// Trace and mark objects without evacuation.
    pub fn thread_local_trace_object_without_moving(
        &self,
        mutator: &Mutator<VM>,
        #[cfg(feature = "debug_publish_object")] source: ObjectReference,
        #[cfg(feature = "debug_publish_object")] _slot: VM::VMEdge,
        object: ObjectReference,
    ) -> ThreadlocalTracedObjectType {
        #[cfg(feature = "vo_bit")]
        vo_bit::helper::on_trace_object::<VM>(object);

        #[cfg(not(feature = "debug_publish_object"))]
        if crate::util::metadata::public_bit::is_public::<VM>(object) {
            return ThreadlocalTracedObjectType::Scanned(object);
        }
        #[cfg(feature = "debug_publish_object")]
        {
            if !source.is_null() && crate::util::metadata::public_bit::is_public::<VM>(source) {
                assert!(
                    crate::util::metadata::public_bit::is_public::<VM>(object),
                    "public src: {:?} --> private child; {:?}",
                    source,
                    object
                );
            }
            if crate::util::metadata::public_bit::is_public::<VM>(object) {
                assert!(
                    Block::containing::<VM>(object).is_block_published(),
                    "public block is corrupted"
                );
                debug_assert!(
                    Line::verify_line_mark_state_of_object::<VM>(
                        object,
                        self.line_mark_state.load(Ordering::Acquire),
                        Some(Line::public_line_mark_state(
                            self.line_mark_state.load(Ordering::Acquire)
                        ))
                    ),
                    "Public Object: {:?} is not published properly",
                    object
                );
                return ThreadlocalTracedObjectType::Scanned(object);
            }
            let m: usize =
                crate::util::object_extra_header_metadata::get_extra_header_metadata::<VM, usize>(
                    object,
                ) & crate::util::object_extra_header_metadata::BOTTOM_HALF_MASK;
            let block = Block::containing::<VM>(object);
            assert!(
                usize::try_from(mutator.mutator_id).unwrap() == m,
                "Object: {:?} 's owner is {:?}, but mutator is {}",
                object,
                m,
                mutator.mutator_id
            );
            assert!(
                mutator.mutator_id == block.owner(),
                "Block: {:?} 's owner is {}, but mutator is {}",
                block,
                block.owner(),
                mutator.mutator_id
            );
        }

        if self.thread_local_attempt_mark(object, self.mark_state) {
            let local_state = self.local_line_mark_state(mutator);
            // Mark block and lines
            if !super::BLOCK_ONLY {
                if !super::MARK_LINE_AT_SCAN_TIME {
                    self.thread_local_mark_lines(object, local_state);
                }
            } else {
                Block::containing::<VM>(object).set_state(BlockState::Marked);
            }

            self.unlog_object_if_needed(object);

            return ThreadlocalTracedObjectType::ToBeScanned(object);
        }
        ThreadlocalTracedObjectType::Scanned(object)
    }

    /// Trace object and do evacuation if required.
    #[cfg(feature = "thread_local_gc_copying")]
    #[allow(clippy::assertions_on_constants)]
    pub fn thread_local_trace_object_with_opportunistic_copy(
        &self,
        mutator: &mut Mutator<VM>,
        #[cfg(feature = "debug_publish_object")] _source: ObjectReference,
        #[cfg(feature = "debug_publish_object")] _slot: VM::VMEdge,
        object: ObjectReference,
        semantics: CopySemantics,
        // nursery_collection: bool,
    ) -> ThreadlocalTracedObjectType {
        #[cfg(feature = "debug_publish_object")]
        {
            if !_source.is_null() && crate::util::metadata::public_bit::is_public::<VM>(_source) {
                assert!(
                    crate::util::metadata::public_bit::is_public::<VM>(object),
                    "public src: {:?} --> private child; {:?}",
                    _source,
                    object
                );
            }
        }

        // public block is now defrag source, so simply leave those public
        // objects in place
        if crate::util::metadata::public_bit::is_public::<VM>(object) {
            #[cfg(feature = "debug_publish_object")]
            {
                assert!(
                    Block::containing::<VM>(object).is_block_published(),
                    "public block is corrupted"
                );
                debug_assert!(
                    Line::verify_line_mark_state_of_object::<VM>(
                        object,
                        self.line_mark_state.load(Ordering::Acquire)
                    ),
                    "Public Object: {:?} is not published properly",
                    object
                );
            }
            return ThreadlocalTracedObjectType::Scanned(object);
        }

        #[cfg(all(feature = "debug_publish_object", debug_assertions))]
        {
            let m = crate::util::object_extra_header_metadata::get_extra_header_metadata::<VM, usize>(
                object,
            ) & crate::util::object_extra_header_metadata::BOTTOM_HALF_MASK;
            let block = Block::containing::<VM>(object);
            debug_assert!(
                usize::try_from(mutator.mutator_id).unwrap() == m,
                "Object: {:?} 's owner is {:?}, but mutator is {}",
                object,
                m,
                mutator.mutator_id
            );
            // Now private objects can live in reusable public blocks
            debug_assert!(
                mutator.mutator_id == block.owner(),
                "Block: {:?} 's owner is {}, but mutator is {}",
                block,
                block.owner(),
                mutator.mutator_id
            );
        }

        debug_assert!(!super::BLOCK_ONLY);

        // Now object is private, it cannot live in a public block that does not
        // belong to its owner
        #[cfg(debug_assertions)]
        debug_assert!(
            Block::containing::<VM>(object).is_defrag_source(),
            "block: {:?}, owner: {:?}, mutator: {:?}, object: {:?} | block is not in defarg source",
            Block::containing::<VM>(object),
            Block::containing::<VM>(object).owner(),
            mutator.mutator_id,
            object
        );

        #[cfg(feature = "vo_bit")]
        vo_bit::helper::on_trace_object::<VM>(object);
        // Now object must be private, it needs to be evacuated
        if object_forwarding::thread_local_is_forwarded::<VM>(object) {
            // Note that the object may not necessarily get forwarded
            // since Immix opportunistically moves objects.
            #[allow(clippy::let_and_return)]
            let new_object = object_forwarding::thread_local_get_forwarded_object::<VM>(object);
            #[cfg(debug_assertions)]
            {
                if new_object == object {
                    debug_assert!(
                        self.is_marked(object) || self.defrag.space_exhausted() || self.is_pinned(object),
                        "Forwarded object is the same as original object {} even though it should have been copied",
                        object,
                    );
                } else {
                    // new_object != object
                    debug_assert!(
                        !Block::containing::<VM>(new_object).is_defrag_source(),
                        "Block {:?} containing forwarded object {} should not be a defragmentation source",
                        Block::containing::<VM>(new_object),
                        new_object,
                    );
                }
            }
            ThreadlocalTracedObjectType::Scanned(new_object)
        } else if self.is_marked(object) && !Block::containing::<VM>(object).is_block_published() {
            // private objects living in public reusable blocks may have mark bit set
            // The object is already marked so we clear the
            // forwarding status and return the unmoved object
            #[cfg(debug_assertions)]
            debug_assert!(
                false,
                "should not reach here in local gc, mutator: {}, block: {:?}, published: {}, owner: {}, object: {:?}",
                mutator.mutator_id,
                Block::containing::<VM>(object),
                Block::containing::<VM>(object).is_block_published(),
                Block::containing::<VM>(object).owner(),
                object
            );
            debug_assert!(
                self.defrag.space_exhausted() || self.is_pinned(object),
                "Forwarded object is the same as original object {} even though it should have been copied",
                object,
            );
            unreachable!("should not reach here in local gc")
            // object_forwarding::clear_forwarding_bits::<VM>(object);
            // ThreadlocalTracedObjectType::Scanned(object)
        } else {
            // actually forward and copy the object if it is not pinned
            // and we have sufficient space in our copy allocator
            let new_object = if self.is_pinned(object) {
                self.thread_local_attempt_mark(object, self.mark_state);
                Block::containing::<VM>(object).set_state(BlockState::Marked);

                #[cfg(feature = "vo_bit")]
                vo_bit::helper::on_object_marked::<VM>(object);
                unreachable!("should not reach here in local gc")
            } else {
                // We are forwarding objects.

                // Clippy complains if the "vo_bit" feature is not enabled.
                #[allow(clippy::let_and_return)]
                let new_object = object_forwarding::thread_local_forward_object::<VM>(
                    object, semantics, mutator,
                );

                #[cfg(feature = "debug_thread_local_gc_copying")]
                {
                    mutator.stats.bytes_copied += VM::VMObjectModel::get_size_when_copied(object);
                }

                #[cfg(feature = "vo_bit")]
                vo_bit::helper::on_object_forwarded::<VM>(new_object);

                #[cfg(feature = "debug_publish_object")]
                {
                    // copy metadata (object owner) over
                    let metadata =
                        crate::util::object_extra_header_metadata::get_extra_header_metadata::<
                            VM,
                            usize,
                        >(object);
                    debug_assert!(
                        usize::try_from(Block::containing::<VM>(object).owner()).unwrap()
                            == metadata
                                & crate::util::object_extra_header_metadata::BOTTOM_HALF_MASK
                            || Block::containing::<VM>(object).is_block_published(),
                        "owner: {:?}, metadata: {:?}",
                        Block::containing::<VM>(object).owner(),
                        metadata
                    );
                    debug_assert!(
                        metadata & crate::util::object_extra_header_metadata::BOTTOM_HALF_MASK
                            == usize::try_from(mutator.mutator_id).unwrap(),
                        "object: {:?} owner: {:?} | mutator: {:?}",
                        object,
                        metadata,
                        mutator.mutator_id
                    );
                    crate::util::object_extra_header_metadata::store_extra_header_metadata::<
                        VM,
                        usize,
                    >(new_object, metadata);
                }
                // Now local gc is done by the mutator, it is now using mutator's allocator, which
                // is not a copy allocator. So need to set the block state (not the case anymore,
                // during gc, mutator's allocator.copy is true)
                // Block::containing::<VM>(new_object).set_state(BlockState::Marked);
                debug_assert!(
                    Block::containing::<VM>(new_object).get_state() == BlockState::Marked
                );
                new_object
            };
            debug_assert_eq!(
                Block::containing::<VM>(new_object).get_state(),
                BlockState::Marked
            );

            self.unlog_object_if_needed(new_object);
            ThreadlocalTracedObjectType::ToBeScanned(new_object)
        }
    }

    fn unlog_object_if_needed(&self, object: ObjectReference) {
        if self.space_args.unlog_object_when_traced {
            // Make sure the side metadata for the line can fit into one byte. For smaller line size, we should
            // use `mark_as_unlogged` instead to mark the bit.
            const_assert!(
                Line::BYTES
                    >= (1
                        << (crate::util::constants::LOG_BITS_IN_BYTE
                            + crate::util::constants::LOG_MIN_OBJECT_SIZE))
            );
            const_assert_eq!(
                crate::vm::object_model::specs::VMGlobalLogBitSpec::LOG_NUM_BITS,
                0
            ); // We should put this to the addition, but type casting is not allowed in constant assertions.

            // Every immix line is 256 bytes, which is mapped to 4 bytes in the side metadata.
            // If we have one object in the line that is mature, we can assume all the objects in the line are mature objects.
            // So we can just mark the byte.
            VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC
                .mark_byte_as_unlogged::<VM>(object, Ordering::Relaxed);
        }
    }

    /// Mark all the lines that the given object spans.
    #[allow(clippy::assertions_on_constants)]
    pub fn mark_lines(&self, object: ObjectReference) {
        // This method is only called in global gc
        debug_assert!(!super::BLOCK_ONLY);

        Line::mark_lines_for_object::<VM>(object, self.line_mark_state.load(Ordering::Acquire));
    }

    #[cfg(feature = "thread_local_gc_copying")]
    pub fn mark_multiple_lines(&self, start: Address, end: Address) {
        let state = self.line_mark_state.load(Ordering::Acquire);
        let start_line = Line::from_unaligned_address(start);
        let mut end_line = Line::from_unaligned_address(end);
        if !Line::is_aligned(end) {
            end_line = end_line.next();
        }
        let iter = RegionIterator::<Line>::new(start_line, end_line);
        for line in iter {
            line.mark(state);
        }
    }

    #[cfg(feature = "thread_local_gc")]
    pub fn thread_local_mark_lines(&self, object: ObjectReference, local_state: u8) {
        debug_assert!(!super::BLOCK_ONLY);

        Line::thread_local_mark_lines_for_object::<VM>(object, local_state);
    }

    #[cfg(feature = "thread_local_gc")]
    fn thread_local_attempt_mark(&self, _object: ObjectReference, _mark_state: u8) -> bool {
        unsafe {
            let old_value = VM::VMObjectModel::LOCAL_MARK_BIT_SPEC.load::<VM, u8>(_object, None);
            if old_value == _mark_state {
                return false;
            }
            VM::VMObjectModel::LOCAL_MARK_BIT_SPEC.store::<VM, u8>(_object, _mark_state, None);
            true
        }
    }

    /// Atomically mark an object.
    fn attempt_mark(&self, object: ObjectReference, mark_state: u8) -> bool {
        loop {
            let old_value = VM::VMObjectModel::LOCAL_MARK_BIT_SPEC.load_atomic::<VM, u8>(
                object,
                None,
                Ordering::SeqCst,
            );
            if old_value == mark_state {
                return false;
            }

            if VM::VMObjectModel::LOCAL_MARK_BIT_SPEC
                .compare_exchange_metadata::<VM, u8>(
                    object,
                    old_value,
                    mark_state,
                    None,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
            {
                break;
            }
        }
        true
    }

    /// Check if an object is marked.
    fn is_marked_with(&self, object: ObjectReference, mark_state: u8) -> bool {
        let old_value = VM::VMObjectModel::LOCAL_MARK_BIT_SPEC.load_atomic::<VM, u8>(
            object,
            None,
            Ordering::SeqCst,
        );
        old_value == mark_state
    }

    pub(crate) fn is_marked(&self, object: ObjectReference) -> bool {
        self.is_marked_with(object, self.mark_state)
    }

    /// Check if an object is pinned.
    fn is_pinned(&self, _object: ObjectReference) -> bool {
        #[cfg(feature = "object_pinning")]
        return self.is_object_pinned(_object);

        #[cfg(not(feature = "object_pinning"))]
        false
    }

    /// Hole searching.
    ///
    /// Linearly scan lines in a block to search for the next
    /// hole, starting from the given line. If we find available lines,
    /// return a tuple of the start line and the end line (non-inclusive).
    ///
    /// Returns None if the search could not find any more holes.
    #[allow(clippy::assertions_on_constants)]
    pub fn get_next_available_lines(
        &self,
        search_start: Line,
        lines_required: u8,
    ) -> Option<(Line, Line)> {
        debug_assert!(!super::BLOCK_ONLY);

        #[cfg(feature = "thread_local_gc_copying")]
        {
            fn search_hole(
                search_start: Line,
                unavail_state: u8,
                current_state: u8,
            ) -> Option<(Line, Line, usize)> {
                let block = search_start.block();
                let mark_data = block.line_mark_table();
                let search_start_cursor = search_start.get_index_within_block();
                let mut start_cursor = search_start_cursor;
                let mut end_cursor;
                // Find start
                while start_cursor < mark_data.len() {
                    let mark = mark_data.get(start_cursor);
                    if mark != unavail_state && mark != current_state {
                        break;
                    }
                    start_cursor += 1;
                }
                if start_cursor == mark_data.len() {
                    return None;
                }
                let start = search_start.next_nth(start_cursor - search_start_cursor);
                #[cfg(debug_assertions)]
                debug_assert_eq!(start_cursor, start.get_index_within_block());
                end_cursor = start_cursor;
                // Find limit
                while end_cursor < mark_data.len() {
                    let mark = mark_data.get(end_cursor);
                    if mark == unavail_state || mark == current_state {
                        break;
                    }
                    end_cursor += 1;
                }
                let end = search_start.next_nth(end_cursor - search_start_cursor);

                #[cfg(not(feature = "thread_local_gc"))]
                debug_assert!(RegionIterator::<Line>::new(start, end)
                    .all(|line| !line.is_marked(unavail_state) && !line.is_marked(current_state)));
                #[cfg(feature = "thread_local_gc")]
                debug_assert!(RegionIterator::<Line>::new(start, end)
                    .all(|line| !line.is_marked(unavail_state)
                        && !line.is_marked(current_state)
                        && !line.is_line_published()));

                Some((start, end, end_cursor - start_cursor))
            }

            let unavail_state = self.line_unavail_state.load(Ordering::Acquire);
            let current_state = self.line_mark_state.load(Ordering::Acquire);

            let block = search_start.block();
            let mut search_start_line = search_start;

            loop {
                if let Some((start, end, size)) =
                    search_hole(search_start_line, unavail_state, current_state)
                {
                    if size >= lines_required as usize {
                        return Some((start, end));
                    } else {
                        // keep searching until it reaches the end of the block
                        if end == block.end_line() {
                            return None;
                        }
                        search_start_line = end;
                        // A hole is skipped, this block can be used as a dense reusable block
                        // block.reset_sparse();
                    }
                } else {
                    return None;
                }
            }
        }
        #[cfg(not(feature = "thread_local_gc_copying"))]
        {
            let unavail_state = self.line_unavail_state.load(Ordering::Acquire);
            let current_state = self.line_mark_state.load(Ordering::Acquire);
            let block = search_start.block();
            let mark_data = block.line_mark_table();
            let start_cursor = search_start.get_index_within_block();
            let mut cursor = start_cursor;
            // Find start
            while cursor < mark_data.len() {
                let mark = mark_data.get(cursor);
                if mark != unavail_state && mark != current_state {
                    break;
                }
                cursor += 1;
            }
            if cursor == mark_data.len() {
                return None;
            }
            let start = search_start.next_nth(cursor - start_cursor);
            // Find limit
            while cursor < mark_data.len() {
                let mark = mark_data.get(cursor);
                if mark == unavail_state || mark == current_state {
                    break;
                }
                cursor += 1;
            }
            let end = search_start.next_nth(cursor - start_cursor);
            debug_assert!(RegionIterator::<Line>::new(start, end)
                .all(|line| !line.is_marked(unavail_state) && !line.is_marked(current_state)));
            Some((start, end))
        }
    }

    pub fn is_last_gc_exhaustive(did_defrag_for_last_gc: bool) -> bool {
        if super::DEFRAG {
            did_defrag_for_last_gc
        } else {
            // If defrag is disabled, every GC is exhaustive.
            true
        }
    }

    pub(crate) fn get_pages_allocated(&self) -> usize {
        self.lines_consumed.load(Ordering::SeqCst) >> (LOG_BYTES_IN_PAGE - Line::LOG_BYTES as u8)
    }

    /// Post copy routine for Immix copy contexts
    pub fn post_copy(&self, object: ObjectReference, _bytes: usize) {
        // Mark the object
        VM::VMObjectModel::LOCAL_MARK_BIT_SPEC.store_atomic::<VM, u8>(
            object,
            self.mark_state,
            None,
            Ordering::SeqCst,
        );
        // Mark the line
        if !super::MARK_LINE_AT_SCAN_TIME {
            self.mark_lines(object);
        }
    }

    #[cfg(feature = "thread_local_gc")]
    pub fn thread_local_post_copy(
        &self,
        _mutator: &mut Mutator<VM>,
        object: ObjectReference,
        _bytes: usize,
    ) {
        // Mark the object
        unsafe {
            VM::VMObjectModel::LOCAL_MARK_BIT_SPEC.store::<VM, u8>(object, self.mark_state, None)
        };
        // Mark the line
        if !super::MARK_LINE_AT_SCAN_TIME {
            // let local_state = self.local_line_mark_state(mutator);
            let state = self.line_mark_state.load(Ordering::Relaxed);
            self.thread_local_mark_lines(object, state);
        }
    }

    #[cfg(feature = "thread_local_gc")]
    pub fn publish_object(
        &self,
        object: ObjectReference,
        #[cfg(feature = "debug_thread_local_gc_copying")] _tls: crate::util::VMMutatorThread,
    ) {
        debug_assert!(crate::util::metadata::public_bit::is_public::<VM>(object));
        // Mark block and lines
        if !super::BLOCK_ONLY {
            let state = self.line_mark_state.load(Ordering::Acquire);

            Line::publish_lines_of_object::<VM>(object, state);
        }
        let block = Block::containing::<VM>(object);
        // This funciton is always called by a mutator, so alway set the dirty bit
        if block.publish(true) {
            #[cfg(feature = "debug_thread_local_gc_copying")]
            {
                use crate::util::GLOBAL_GC_STATISTICS;

                if VM::VMActivePlan::is_mutator(_tls.0) {
                    let mutator = VM::VMActivePlan::mutator(_tls);
                    mutator.stats.blocks_published += 1;
                }
                let mut guard = GLOBAL_GC_STATISTICS.lock().unwrap();
                guard.blocks_published += 1;
            }
        }
        self.bytes_published.fetch_add(
            VM::VMObjectModel::get_current_size(object),
            Ordering::SeqCst,
        );
    }

    #[cfg(all(feature = "thread_local_gc", feature = "debug_publish_object"))]
    pub fn get_object_owner(&self, object: ObjectReference) -> u32 {
        #[cfg(feature = "debug_publish_object")]
        {
            let metadata: usize =
                crate::util::object_extra_header_metadata::get_extra_header_metadata::<VM, usize>(
                    object,
                ) & crate::util::object_extra_header_metadata::BOTTOM_HALF_MASK;
            return u32::try_from(metadata).unwrap();
        }
        #[cfg(not(feature = "debug_publish_object"))]
        Block::containing::<VM>(object).owner()
    }

    #[cfg(feature = "debug_publish_object")]
    pub fn is_object_published(&self, object: ObjectReference) -> bool {
        debug_assert!(!object.is_null());
        let is_published = crate::util::metadata::public_bit::is_public::<VM>(object);
        if object_forwarding::is_forwarded_or_being_forwarded::<VM>(object) {
            // object's public bit may have been cleared, so need to read the public bit on the forwarded object
            let new_object = object_forwarding::spin_and_get_forwarded_object::<VM>(
                object,
                object_forwarding::get_forwarding_status::<VM>(object),
            );

            let is_published = crate::util::metadata::public_bit::is_public::<VM>(new_object);

            is_published
        } else {
            // object has not been forwarded yet, the public bit read before is still valid
            is_published
        }
    }

    #[cfg(feature = "thread_local_gc_copying_stats")]
    pub fn print_immixspace_stats(&self) -> (usize, usize, usize, usize, usize, usize, usize) {
        let mut free_blocks = 0;
        let mut reusable_block = 0;
        let mut allocated_block = 0;
        let mut global_reusable_block = 0;
        let mut published_block = 0;
        let mut available_lines = 0;
        let mut sparse_block = 0;
        for chunk in self.chunk_map.all_chunks() {
            let blocks = chunk.iter_region::<Block>();
            if self.chunk_map.get(chunk) == ChunkState::Free {
                free_blocks += blocks.count();
            } else {
                for block in blocks {
                    match block.get_state() {
                        BlockState::Unallocated => free_blocks += 1,
                        BlockState::Reusable { unavailable_lines } => {
                            available_lines += Block::LINES - unavailable_lines as usize;
                            reusable_block += 1;
                            if block.is_block_published() {
                                global_reusable_block += 1;
                            }
                            if block.is_block_sparse() {
                                sparse_block += 1;
                            }
                        }
                        _ => {
                            allocated_block += 1;
                            if block.is_block_published() {
                                published_block += 1;
                            }
                        }
                    }
                }
            }
        }

        (
            free_blocks,
            reusable_block,
            global_reusable_block,
            allocated_block,
            published_block,
            available_lines,
            sparse_block,
        )
    }
}

#[cfg(feature = "thread_local_gc")]
impl<VM: VMBinding> crate::policy::gc_work::PolicyThreadlocalTraceObject<VM> for ImmixSpace<VM> {
    #[cfg(not(feature = "debug_publish_object"))]
    fn thread_local_trace_object<const KIND: TraceKind>(
        &self,
        mutator: &mut Mutator<VM>,
        object: ObjectReference,
        _copy: Option<CopySemantics>,
    ) -> ThreadlocalTracedObjectType {
        if KIND == TRACE_THREAD_LOCAL_FAST {
            #[cfg(feature = "thread_local_gc_copying")]
            panic!("local gc always do defrag");
            #[cfg(not(feature = "thread_local_gc_copying"))]
            self.thread_local_trace_object_without_moving(mutator, object)
        } else if KIND == TRACE_THREAD_LOCAL_DEFRAG {
            #[cfg(feature = "thread_local_gc_copying")]
            {
                self.thread_local_trace_object_with_opportunistic_copy(
                    mutator,
                    object,
                    _copy.unwrap(),
                )
            }
            #[cfg(not(feature = "thread_local_gc_copying"))]
            unreachable!()
        } else {
            unreachable!()
        }
    }

    #[cfg(feature = "debug_publish_object")]
    fn thread_local_trace_object<const KIND: TraceKind>(
        &self,
        mutator: &mut Mutator<VM>,
        _source: ObjectReference,
        _slot: VM::VMEdge,
        object: ObjectReference,
        _copy: Option<CopySemantics>,
    ) -> ThreadlocalTracedObjectType {
        if KIND == TRACE_THREAD_LOCAL_FAST {
            #[cfg(feature = "thread_local_gc_copying")]
            panic!("local gc always do defrag");
            #[cfg(not(feature = "thread_local_gc_copying"))]
            self.thread_local_trace_object_without_moving(mutator, _source, _slot, object)
        } else if KIND == TRACE_THREAD_LOCAL_DEFRAG {
            #[cfg(not(feature = "thread_local_gc_copying"))]
            unreachable!();
            #[cfg(feature = "thread_local_gc_copying")]
            {
                self.thread_local_trace_object_with_opportunistic_copy(
                    mutator,
                    _source,
                    _slot,
                    object,
                    _copy.unwrap(),
                    // // This should not be nursery collection. Nursery collection does not use PolicyTraceObject.
                    // false,
                )
                // let block = Block::containing::<VM>(object);
                // if block.is_block_published() {
                //     // local gc evacuates private objects living in public blocks
                //     self.thread_local_trace_object_with_opportunistic_copy(
                //         mutator,
                //         _source,
                //         _slot,
                //         object,
                //         _copy.unwrap(),
                //         // // This should not be nursery collection. Nursery collection does not use PolicyTraceObject.
                //         // false,
                //     )
                // } else {
                //     // this is a private block, there is no need to evacuate it
                //     debug_assert!(
                //         block.owner() == mutator.mutator_id,
                //         "block: {:?}, owner: {:?}, mutator: {:?}, object: {:?}, object published: {}",
                //         block,
                //         block.owner(),
                //         mutator.mutator_id,
                //         object,
                //         crate::util::metadata::public_bit::is_public::<VM>(object)
                //     );

                //     self.thread_local_trace_object_without_moving(mutator, _source, _slot, object)
                // }
            }
        } else {
            unreachable!()
        }
    }

    fn thread_local_post_scan_object(&self, _mutator: &Mutator<VM>, object: ObjectReference) {
        if super::MARK_LINE_AT_SCAN_TIME && !super::BLOCK_ONLY {
            debug_assert!(self.in_space(object));

            // let local_state: u8 = self.local_line_mark_state(mutator);
            let state = self.line_mark_state.load(Ordering::Relaxed);
            self.thread_local_mark_lines(object, state);
        }
    }

    fn thread_local_may_move_objects<const KIND: TraceKind>() -> bool {
        #[cfg(feature = "thread_local_gc_copying")]
        return true;
        #[cfg(feature = "thread_local_gc_ibm_style")]
        return false;
    }
}

/// A work packet to prepare each block for a major GC.
/// Performs the action on a range of chunks.
pub struct PrepareBlockState<VM: VMBinding> {
    pub space: &'static ImmixSpace<VM>,
    pub chunk: Chunk,
    pub defrag_threshold: Option<usize>,
}

impl<VM: VMBinding> PrepareBlockState<VM> {
    /// Clear object mark table
    fn reset_object_mark(&self) {
        // NOTE: We reset the mark bits because cyclic mark bit is currently not supported, yet.
        // See `ImmixSpace::prepare`.
        if let MetadataSpec::OnSide(side) = *VM::VMObjectModel::LOCAL_MARK_BIT_SPEC {
            side.bzero_metadata(self.chunk.start(), Chunk::BYTES);
        }
        if self.space.space_args.reset_log_bit_in_major_gc {
            if let MetadataSpec::OnSide(side) = *VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC {
                // We zero all the log bits in major GC, and for every object we trace, we will mark the log bit again.
                side.bzero_metadata(self.chunk.start(), Chunk::BYTES);
            } else {
                // If the log bit is not in side metadata, we cannot bulk zero. We can either
                // clear the bit for dead objects in major GC, or clear the log bit for new
                // objects. In either cases, we do not need to set log bit at tracing.
                unimplemented!("We cannot bulk zero unlogged bit.")
            }
        }
        // If the forwarding bits are on the side, we need to clear them, too.
        if let MetadataSpec::OnSide(side) = *VM::VMObjectModel::LOCAL_FORWARDING_BITS_SPEC {
            side.bzero_metadata(self.chunk.start(), Chunk::BYTES);
        }
    }

    #[cfg(feature = "thread_local_gc")]
    fn reset_public_line_mark(&self) {
        // In a non-moving setting, this is a no-op as public objects are not evacuated.
        #[cfg(feature = "thread_local_gc_copying")]
        Line::LINE_PUBLICATION_TABLE.bzero_metadata(self.chunk.start(), Chunk::BYTES);
    }
}

impl<VM: VMBinding> GCWork<VM> for PrepareBlockState<VM> {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        // Clear object mark table for this chunk
        self.reset_object_mark();

        #[cfg(feature = "thread_local_gc")]
        self.reset_public_line_mark();

        // Iterate over all blocks in this chunk
        for block in self.chunk.iter_region::<Block>() {
            let state = block.get_state();
            // Skip unallocated blocks.
            if state == BlockState::Unallocated {
                continue;
            }
            #[cfg(feature = "thread_local_gc")]
            let is_public = block.is_block_published();
            #[cfg(feature = "thread_local_gc")]
            // Check if this block needs to be defragmented.
            let is_defrag_source = if !super::DEFRAG {
                false
            } else if super::DEFRAG_EVERY_BLOCK {
                // Set every block as defrag source if so desired.
                true
            } else if !is_public {
                // do not defrag private blocks
                #[cfg(debug_assertions)]
                {
                    debug_assert_ne!(block.owner(), u32::MAX);
                    debug_assert!(block.is_block_dirty());
                }
                false
            } else if state.is_reusable() {
                // A corner case is that some local reusable block
                // might get published during mutator phase. However,
                // those blocks have dirty bit set
                #[cfg(debug_assertions)]
                {
                    let mut exists = false;
                    self.space.reusable_blocks.iterate_blocks(|b| {
                        if b == block {
                            exists = true;
                        }
                    });
                    self.space.sparse_reusable_blocks.iterate_blocks(|b| {
                        if b == block {
                            exists = true;
                        }
                    });
                    if block.owner() == u32::MAX {
                        debug_assert!(exists);
                    } else {
                        debug_assert!(!exists);
                    }
                }

                // Since every global gc is a defrag gc, defrag_threshold will never be None
                block.is_block_dirty() || block.get_holes() > self.defrag_threshold.unwrap()
            } else {
                // fully occupied public blocks, they may not be dirty
                // do not defrag those blocks
                debug_assert!(is_public);
                debug_assert!(block.get_state() == BlockState::Unmarked);
                false
            };

            #[cfg(not(feature = "thread_local_gc"))]
            // Check if this block needs to be defragmented.
            let is_defrag_source = if !super::DEFRAG {
                false
            } else if super::DEFRAG_EVERY_BLOCK {
                // Set every block as defrag source if so desired.
                true
            } else if let Some(defrag_threshold) = self.defrag_threshold {
                // This GC is a defrag GC.
                block.get_holes() > defrag_threshold
            } else {
                // Not a defrag GC.
                false
            };
            block.set_as_defrag_source(is_defrag_source);
            // Clear block mark data.
            block.set_state(BlockState::Unmarked);

            debug_assert!(!block.get_state().is_reusable());
            debug_assert_ne!(block.get_state(), BlockState::Marked);
        }
    }
}

/// Chunk sweeping work packet.
struct SweepChunk<VM: VMBinding> {
    space: &'static ImmixSpace<VM>,
    chunk: Chunk,
    /// A destructor invoked when all `SweepChunk` packets are finished.
    epilogue: Arc<FlushPageResource<VM>>,
}

impl<VM: VMBinding> GCWork<VM> for SweepChunk<VM> {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        // This work packet is only generated during global gc
        #[cfg(feature = "debug_thread_local_gc_copying")]
        use crate::util::GCStatistics;

        #[cfg(feature = "debug_thread_local_gc_copying")]
        let mut gc_stats = GCStatistics::default();

        let mut histogram = self.space.defrag.new_histogram();
        if self.space.chunk_map.get(self.chunk) == ChunkState::Allocated {
            let line_mark_state = if super::BLOCK_ONLY {
                None
            } else {
                Some(self.space.line_mark_state.load(Ordering::Acquire))
            };
            // number of allocated blocks.
            let mut allocated_blocks = 0;
            // Iterate over all allocated blocks in this chunk.
            for block in self
                .chunk
                .iter_region::<Block>()
                .filter(|block| block.get_state() != BlockState::Unallocated)
            {
                if !block.sweep(
                    self.space,
                    &mut histogram,
                    line_mark_state,
                    #[cfg(feature = "debug_thread_local_gc_copying")]
                    &mut gc_stats,
                ) {
                    // Block is live. Increment the allocated block count.
                    allocated_blocks += 1;
                } else {
                    // self.space.blocks_freed.fetch_add(1, Ordering::SeqCst);
                }
                #[cfg(all(feature = "thread_local_gc_copying", debug_assertions))]
                {
                    if block.is_block_published() {
                        // public block
                        debug_assert!(
                            !block.are_lines_private(),
                            "public block: {:?} contains private lines after global gc",
                            block
                        );
                    } else {
                        // private block
                        debug_assert!(
                            block.are_lines_private(),
                            "private block: {:?} contains public lines after global gc",
                            block
                        );
                    }
                }
            }

            // Set this chunk as free if there is not live blocks.
            if allocated_blocks == 0 {
                self.space.chunk_map.set(self.chunk, ChunkState::Free)
            }
        }
        #[cfg(feature = "debug_thread_local_gc_copying")]
        {
            use crate::util::GLOBAL_GC_STATISTICS;

            let mut guard = GLOBAL_GC_STATISTICS.lock().unwrap();
            guard.number_of_blocks_freed += gc_stats.number_of_blocks_freed;
            guard.number_of_live_blocks += gc_stats.number_of_live_blocks;
            guard.number_of_live_public_blocks += gc_stats.number_of_live_public_blocks;
            guard.number_of_global_reusable_blocks += gc_stats.number_of_global_reusable_blocks;
            guard.number_of_local_reusable_blocks += gc_stats.number_of_local_reusable_blocks;
            guard.number_of_free_lines_in_global_reusable_blocks +=
                gc_stats.number_of_free_lines_in_global_reusable_blocks;
            guard.number_of_free_lines_in_local_reusable_blocks +=
                gc_stats.number_of_free_lines_in_local_reusable_blocks;
        }
        self.space.defrag.add_completed_mark_histogram(histogram);
        self.epilogue.finish_one_work_packet();
    }
}

/// Count number of remaining work pacets, and flush page resource if all packets are finished.
struct FlushPageResource<VM: VMBinding> {
    space: &'static ImmixSpace<VM>,
    counter: AtomicUsize,
    #[cfg(feature = "thread_local_gc")]
    scheduler: Arc<GCWorkScheduler<VM>>,
}

impl<VM: VMBinding> FlushPageResource<VM> {
    /// Called after a related work packet is finished.
    fn finish_one_work_packet(&self) {
        if 1 == self.counter.fetch_sub(1, Ordering::SeqCst) {
            // We've finished releasing all the dead blocks to the BlockPageResource's thread-local queues.
            // Now flush the BlockPageResource.
            self.space.flush_page_resource();
            // When local gc is enabled, it keeps track of a local block list
            // That list needs to be updated after blocks get flushed
            #[cfg(feature = "thread_local_gc")]
            for mutator in VM::VMActivePlan::mutators() {
                self.scheduler.work_buckets[WorkBucketStage::Release].add(
                    crate::scheduler::gc_work::ReleaseMutator::<VM>::new(mutator),
                );
            }
            let mut set = std::collections::HashSet::new();
            self.space.reusable_blocks.iterate_blocks(|block| {
                let v = set.insert(block.start());
                if !v {
                    panic!("block: {:?} already exists", block);
                }
            });
        }
    }
}

use crate::policy::copy_context::PolicyCopyContext;
use crate::util::alloc::Allocator;
use crate::util::alloc::ImmixAllocator;

/// Normal immix copy context. It has one copying Immix allocator.
/// Most immix plans use this copy context.
pub struct ImmixCopyContext<VM: VMBinding> {
    allocator: ImmixAllocator<VM>,
}

impl<VM: VMBinding> PolicyCopyContext for ImmixCopyContext<VM> {
    type VM = VM;

    fn prepare(&mut self) {
        self.allocator.reset();
    }
    fn release(&mut self) {
        self.allocator.reset();
    }

    fn alloc_copy(
        &mut self,
        _original: ObjectReference,
        bytes: usize,
        align: usize,
        offset: usize,
    ) -> Address {
        #[cfg(not(feature = "thread_local_gc"))]
        return self.allocator.alloc(bytes, align, offset);
        #[cfg(feature = "thread_local_gc")]
        {
            if crate::util::metadata::public_bit::is_public::<VM>(_original) {
                let result = self.allocator.alloc_copy(bytes, align, offset);
                result
            } else {
                unreachable!("global gc trying to evacuate private objects");
            }
        }
    }
    fn post_copy(&mut self, obj: ObjectReference, bytes: usize) {
        self.get_space().post_copy(obj, bytes)
    }
}

impl<VM: VMBinding> ImmixCopyContext<VM> {
    pub(crate) fn new(
        tls: VMWorkerThread,
        context: Arc<AllocatorContext<VM>>,
        space: &'static ImmixSpace<VM>,
    ) -> Self {
        ImmixCopyContext {
            allocator: ImmixAllocator::new(
                tls.0,
                u32::MAX,
                Some(space),
                context,
                true,
                Some(ImmixAllocSemantics::Public), // only used in global gc to evacuate public objects
            ),
        }
    }

    fn get_space(&self) -> &ImmixSpace<VM> {
        self.allocator.immix_space()
    }
}

/// Hybrid Immix copy context. It includes two different immix allocators. One with `copy = true`
/// is used for defrag GCs, and the other is used for other purposes (such as promoting objects from
/// nursery to Immix mature space). This is used by generational immix.
pub struct ImmixHybridCopyContext<VM: VMBinding> {
    copy_allocator: ImmixAllocator<VM>,
    defrag_allocator: ImmixAllocator<VM>,
}

impl<VM: VMBinding> PolicyCopyContext for ImmixHybridCopyContext<VM> {
    type VM = VM;

    fn prepare(&mut self) {
        self.copy_allocator.reset();
        self.defrag_allocator.reset();
    }
    fn release(&mut self) {
        self.copy_allocator.reset();
        self.defrag_allocator.reset();
    }
    fn alloc_copy(
        &mut self,
        _original: ObjectReference,
        bytes: usize,
        align: usize,
        offset: usize,
    ) -> Address {
        if self.get_space().in_defrag() {
            self.defrag_allocator.alloc(bytes, align, offset)
        } else {
            self.copy_allocator.alloc(bytes, align, offset)
        }
    }
    fn post_copy(&mut self, obj: ObjectReference, bytes: usize) {
        self.get_space().post_copy(obj, bytes)
    }
}

impl<VM: VMBinding> ImmixHybridCopyContext<VM> {
    pub(crate) fn new(
        tls: VMWorkerThread,
        context: Arc<AllocatorContext<VM>>,
        space: &'static ImmixSpace<VM>,
    ) -> Self {
        ImmixHybridCopyContext {
            copy_allocator: ImmixAllocator::new(
                tls.0,
                u32::MAX,
                Some(space),
                context.clone(),
                false,
                None,
            ),
            defrag_allocator: ImmixAllocator::new(
                tls.0,
                u32::MAX,
                Some(space),
                context,
                true,
                None,
            ),
        }
    }

    fn get_space(&self) -> &ImmixSpace<VM> {
        // Both copy allocators should point to the same space.
        debug_assert_eq!(
            self.defrag_allocator.immix_space().common().descriptor,
            self.copy_allocator.immix_space().common().descriptor
        );
        // Just get the space from either allocator
        self.defrag_allocator.immix_space()
    }
}

#[cfg(feature = "vo_bit")]
#[derive(Clone, Copy)]
enum VOBitsClearingScope {
    /// Clear all VO bits in all blocks.
    FullGC,
    /// Clear unmarked blocks, only.
    BlockOnly,
    /// Clear unmarked lines, only.  (i.e. lines with line mark state **not** equal to `state`).
    Line { state: u8 },
}

/// A work packet to clear VO bit metadata after Prepare.
#[cfg(feature = "vo_bit")]
struct ClearVOBitsAfterPrepare {
    chunk: Chunk,
    scope: VOBitsClearingScope,
}

#[cfg(feature = "vo_bit")]
impl<VM: VMBinding> GCWork<VM> for ClearVOBitsAfterPrepare {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        match self.scope {
            VOBitsClearingScope::FullGC => {
                vo_bit::bzero_vo_bit(self.chunk.start(), Chunk::BYTES);
            }
            VOBitsClearingScope::BlockOnly => {
                self.clear_blocks(None);
            }
            VOBitsClearingScope::Line { state } => {
                self.clear_blocks(Some(state));
            }
        }
    }
}

#[cfg(feature = "vo_bit")]
impl ClearVOBitsAfterPrepare {
    fn clear_blocks(&mut self, line_mark_state: Option<u8>) {
        for block in self
            .chunk
            .iter_region::<Block>()
            .filter(|block| block.get_state() != BlockState::Unallocated)
        {
            block.clear_vo_bits_for_unmarked_regions(line_mark_state);
        }
    }
}

#[cfg(feature = "thread_local_gc_copying_stats")]
pub static TOTAL_IMMIX_ALLOCATION_BYTES: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);
