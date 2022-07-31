use super::gc_work::SSGCWorkContext;
use super::NON_LOCAL_OBJECTS;
use crate::plan::global::CommonPlan;
use crate::plan::global::GcStatus;
use crate::plan::semispace::mutator::ALLOCATOR_MAPPING;
use crate::plan::AllocationSemantics;
use crate::plan::Plan;
use crate::plan::PlanConstraints;
use crate::policy::copyspace::CopySpace;
use crate::policy::space::Space;
use crate::scheduler::*;
use crate::util::alloc::allocators::AllocatorSelector;
use crate::util::copy::*;
use crate::util::heap::layout::heap_layout::Mmapper;
use crate::util::heap::layout::heap_layout::VMMap;
use crate::util::heap::layout::vm_layout_constants::{HEAP_END, HEAP_START};
use crate::util::heap::HeapMeta;
use crate::util::heap::VMRequest;
use crate::util::metadata::side_metadata::{SideMetadataContext, SideMetadataSanity};
use crate::util::opaque_pointer::VMWorkerThread;
use crate::util::options::UnsafeOptionsWrapper;
use crate::BarrierSelector;
use crate::{plan::global::BasePlan, vm::ObjectModel, vm::VMBinding};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use mmtk_macros::PlanTraceObject;

use enum_map::EnumMap;

#[derive(PlanTraceObject)]
pub struct SemiSpace<VM: VMBinding> {
    pub hi: AtomicBool,
    #[trace(CopySemantics::DefaultCopy)]
    pub copyspace0: CopySpace<VM>,
    #[trace(CopySemantics::DefaultCopy)]
    pub copyspace1: CopySpace<VM>,
    #[fallback_trace]
    pub common: CommonPlan<VM>,
}

pub const SS_CONSTRAINTS: PlanConstraints = PlanConstraints {
    moves_objects: true,
    gc_header_bits: 2,
    gc_header_words: 0,
    num_specialized_scans: 1,
    // max_non_los_default_alloc_bytes:
    //     crate::plan::plan_constraints::MAX_NON_LOS_ALLOC_BYTES_COPYING_PLAN,
    barrier: BarrierSelector::ObjectOwnerBarrier,
    needs_log_bit: false,
    ..PlanConstraints::default()
};

impl<VM: VMBinding> Plan for SemiSpace<VM> {
    type VM = VM;

    fn constraints(&self) -> &'static PlanConstraints {
        &SS_CONSTRAINTS
    }

    fn create_copy_config(&'static self) -> CopyConfig<Self::VM> {
        use enum_map::enum_map;
        CopyConfig {
            copy_mapping: enum_map! {
                CopySemantics::DefaultCopy => CopySelector::CopySpace(0),
                _ => CopySelector::Unused,
            },
            space_mapping: vec![
                // // The tospace argument doesn't matter, we will rebind before a GC anyway.
                (CopySelector::CopySpace(0), &self.copyspace0),
            ],
            constraints: &SS_CONSTRAINTS,
        }
    }

    fn gc_init(&mut self, heap_size: usize, vm_map: &'static VMMap) {
        self.common.gc_init(heap_size, vm_map);

        self.copyspace0.init(vm_map);
        self.copyspace1.init(vm_map);
    }

    fn schedule_collection(&'static self, scheduler: &GCWorkScheduler<VM>) {
        self.base().set_collection_kind::<Self>(self);
        self.base().set_gc_status(GcStatus::GcPrepare);
        scheduler.schedule_common_work::<SSGCWorkContext<VM>>(self);
    }

    fn get_allocator_mapping(&self) -> &'static EnumMap<AllocationSemantics, AllocatorSelector> {
        &*ALLOCATOR_MAPPING
    }

    fn prepare(&mut self, tls: VMWorkerThread) {
        self.common.prepare(tls, true);

        self.hi
            .store(!self.hi.load(Ordering::SeqCst), Ordering::SeqCst); // flip the semi-spaces
                                                                       // prepare each of the collected regions
        let hi = self.hi.load(Ordering::SeqCst);
        self.copyspace0.prepare(hi);
        self.copyspace1.prepare(!hi);
        self.fromspace_mut()
            .set_copy_for_sft_trace(Some(CopySemantics::DefaultCopy));
        self.tospace_mut().set_copy_for_sft_trace(None);
    }

    fn prepare_worker(&self, worker: &mut GCWorker<VM>) {
        unsafe { worker.get_copy_context_mut().copy[0].assume_init_mut() }.rebind(self.tospace());
    }

    fn release(&mut self, tls: VMWorkerThread) {
        // {
        //     let mut mutators = self.base().mutators.lock().unwrap();
        //     self.base()
        //         .gc_counter
        //         .fetch_add(mutators.len(), Ordering::SeqCst);
        //     let mut result_file = std::fs::OpenOptions::new()
        //         .write(true)
        //         .append(true)
        //         .create(true)
        //         .open("/home/tianleq/run.txt")
        //         .unwrap();

        //     use std::io::Write;
        //     writeln!(&mut result_file, "----semispace release----").unwrap();
        //     use crate::vm::ActivePlan;
        //     const OWNER_MASK: usize = 0x00000000FFFFFFFF;
        //     let mut invalid = vec![];
        //     let mut err = false;
        //     for m in mutators.iter() {
        //         let mutator = VM::VMActivePlan::mutator(*m);
        //         if mutator.critical_section_active {
        //             err = true;
        //             break;
        //         }
        //     }
        //     if err {
        //         let size = mutators.len();
        //         let mut count = 0;
        //         println!("number of mutators: {}", mutators.len());
        //         for m in mutators.iter() {
        //             let mutator = VM::VMActivePlan::mutator(*m);
        //             println!(
        //                 "gc: {}, mutator: {} -- reqest: {}, active: {} ",
        //                 self.base().gc_counter.load(Ordering::SeqCst),
        //                 VM::VMActivePlan::mutator_id(*m),
        //                 mutator.request_id,
        //                 mutator.critical_section_active
        //             );
        //             count += 1;
        //         }
        //         assert!(false);
        //         // unsafe { std::ptr::null_mut::<i32>().write(42) };
        //     }
        //     for m in mutators.iter() {
        //         let mut published_object_within_the_req = 0;
        //         let owner = VM::VMActivePlan::mutator_id(*m);
        //         let mutator = VM::VMActivePlan::mutator(*m);

        //         if mutator.critical_section_active {
        //             println!("mutators: {}", mutators.len());
        //             assert!(
        //                 !mutator.critical_section_active,
        //                 "t: {} -- {} should have left critical section",
        //                 owner, mutator.request_id
        //             );
        //         }

        //         let request_id = mutator.request_id;
        //         let object_owner = (request_id as usize) << 32 | (owner & OWNER_MASK);
        //         let live_info = self.fromspace().live_object_info(object_owner);
        //         let mut bytes = 0;
        //         for e in NON_LOCAL_OBJECTS.lock().unwrap().iter() {
        //             if crate::util::critical_bit::is_alloced_in_critical_section(*e) {
        //                 let metadata = CopySpace::<VM>::get_header_object_owner(*e);
        //                 assert!(metadata != 0);
        //                 if metadata == object_owner {
        //                     if self.fromspace().in_space(*e) {
        //                         // these objects are added by previous gc and they are no longer reachable
        //                         invalid.push(*e);
        //                     } else {
        //                         published_object_within_the_req += 1;
        //                         bytes += VM::VMObjectModel::get_current_size(*e);
        //                     }
        //                 }
        //             }
        //         }

        //         writeln!(
        //             &mut result_file,
        //             "{} gc m-{} r-{} : alloc {} objects {} bytes, {} public object {} bytes, {} live object {} bytes",
        //             self.base().gc_counter.load(Ordering::SeqCst),
        //             owner,
        //             request_id,
        //             mutator.cirtical_section_object_counter,
        //             mutator.critical_section_memory_footprint,
        //             published_object_within_the_req,
        //             bytes,
        //             live_info.0,
        //             live_info.1
        //         ).unwrap();
        //     }
        //     for mutator in VM::VMActivePlan::mutators() {
        //         let mut published_object_within_the_req = 0;
        //         let owner = VM::VMActivePlan::mutator_id(mutator.mutator_tls);
        //         let request_id = mutator.request_id;
        //         let object_owner = (request_id as usize) << 32 | (owner & OWNER_MASK);
        //         let mut bytes = 0;
        //         for e in NON_LOCAL_OBJECTS.lock().unwrap().iter() {
        //             if crate::util::critical_bit::is_alloced_in_critical_section(*e) {
        //                 let metadata = CopySpace::<VM>::get_header_object_owner(*e);
        //                 assert!(metadata != 0);
        //                 if metadata == object_owner {
        //                     published_object_within_the_req += 1;
        //                     bytes += VM::VMObjectModel::get_current_size(*e);
        //                 }
        //             }
        //         }
        //         writeln!(
        //             &mut result_file,
        //             "m-{} r-{} : {} public object, {} bytes",
        //             owner, request_id, published_object_within_the_req, bytes
        //         )
        //         .unwrap();
        //     }

        //     {
        //         for e in invalid.iter() {
        //             NON_LOCAL_OBJECTS.lock().unwrap().remove(e);
        //         }
        //     }
        //     mutators.clear();
        // }
        self.common.release(tls, true);
        // release the collected region
        self.fromspace().release();
    }

    fn collection_required(&self, space_full: bool, space: &dyn Space<Self::VM>) -> bool {
        self.base().collection_required(self, space_full, space)
    }

    fn get_collection_reserved_pages(&self) -> usize {
        self.tospace().reserved_pages()
    }

    fn get_used_pages(&self) -> usize {
        self.tospace().reserved_pages() + self.common.get_used_pages()
    }

    fn base(&self) -> &BasePlan<VM> {
        &self.common.base
    }

    fn common(&self) -> &CommonPlan<VM> {
        &self.common
    }
}

impl<VM: VMBinding> SemiSpace<VM> {
    pub fn new(
        vm_map: &'static VMMap,
        mmapper: &'static Mmapper,
        options: Arc<UnsafeOptionsWrapper>,
    ) -> Self {
        let mut heap = HeapMeta::new(HEAP_START, HEAP_END);
        let specs = vec![];
        let global_metadata_specs = SideMetadataContext::new_global_specs(&specs);

        let res = SemiSpace {
            hi: AtomicBool::new(false),
            copyspace0: CopySpace::new(
                "copyspace0",
                false,
                true,
                VMRequest::discontiguous(),
                global_metadata_specs.clone(),
                vm_map,
                mmapper,
                &mut heap,
                &NON_LOCAL_OBJECTS,
            ),
            copyspace1: CopySpace::new(
                "copyspace1",
                true,
                true,
                VMRequest::discontiguous(),
                global_metadata_specs.clone(),
                vm_map,
                mmapper,
                &mut heap,
                &NON_LOCAL_OBJECTS,
            ),
            common: CommonPlan::new(
                vm_map,
                mmapper,
                options,
                heap,
                &SS_CONSTRAINTS,
                global_metadata_specs,
            ),
        };

        // Use SideMetadataSanity to check if each spec is valid. This is also needed for check
        // side metadata in extreme_assertions.
        {
            let mut side_metadata_sanity_checker = SideMetadataSanity::new();
            res.common
                .verify_side_metadata_sanity(&mut side_metadata_sanity_checker);
            res.copyspace0
                .verify_side_metadata_sanity(&mut side_metadata_sanity_checker);
            res.copyspace1
                .verify_side_metadata_sanity(&mut side_metadata_sanity_checker);
        }

        res
    }

    pub fn tospace(&self) -> &CopySpace<VM> {
        if self.hi.load(Ordering::SeqCst) {
            &self.copyspace1
        } else {
            &self.copyspace0
        }
    }

    pub fn tospace_mut(&mut self) -> &mut CopySpace<VM> {
        if self.hi.load(Ordering::SeqCst) {
            &mut self.copyspace1
        } else {
            &mut self.copyspace0
        }
    }

    pub fn fromspace(&self) -> &CopySpace<VM> {
        if self.hi.load(Ordering::SeqCst) {
            &self.copyspace0
        } else {
            &self.copyspace1
        }
    }

    pub fn fromspace_mut(&mut self) -> &mut CopySpace<VM> {
        if self.hi.load(Ordering::SeqCst) {
            &mut self.copyspace0
        } else {
            &mut self.copyspace1
        }
    }
}
