use ::policy::immortalspace::ImmortalSpace;
use ::policy::largeobjectspace::LargeObjectSpace;
use ::policy::codespace::CodeSpace;
use ::util::alloc::{BumpAllocator, LargeObjectAllocator, CodeAllocator};
use ::plan::mutator_context::MutatorContext;
use ::plan::Phase;
use ::util::{Address, ObjectReference};
use ::util::alloc::Allocator;
use ::plan::Allocator as AllocationType;
use ::util::heap::MonotonePageResource;
use ::plan::nogc::PLAN;

use libc::c_void;

#[repr(C)]
pub struct NoGCMutator {
    // ImmortalLocal
    nogc: BumpAllocator<MonotonePageResource<ImmortalSpace>>,
    los: LargeObjectAllocator,
    cos : CodeAllocator
}

impl MutatorContext for NoGCMutator {
    fn collection_phase(&mut self, tls: *mut c_void, phase: &Phase, primary: bool) {
        unimplemented!();
    }

    fn alloc(&mut self, size: usize, align: usize, offset: isize, allocator: AllocationType) -> Address {
        trace!("MutatorContext.alloc({}, {}, {}, {:?})", size, align, offset, allocator);
        match allocator {
            AllocationType::Los => self.los.alloc(size, align, offset),
            AllocationType::Code => self.cos.alloc(size, align, offset),
            _ => self.nogc.alloc(size, align, offset)
        }
    }

    fn alloc_slow(&mut self, size: usize, align: usize, offset: isize, allocator: AllocationType) -> Address {
        trace!("MutatorContext.alloc_slow({}, {}, {}, {:?})", size, align, offset, allocator);
        match allocator {
            AllocationType::Los => self.los.alloc(size, align, offset),
            AllocationType::Code => self.cos.alloc(size, align, offset),
            _ => self.nogc.alloc(size, align, offset)
        }
    }

    fn post_alloc(&mut self, refer: ObjectReference, type_refer: ObjectReference, bytes: usize, allocator: AllocationType) {
        match allocator {
            AllocationType::Los => {
                // FIXME: data race on immortalspace.mark_state !!!
                let unsync = unsafe { &*PLAN.unsync.get() };
                unsync.los.initialize_header(refer, true);
            }
            // FIXME: other allocation types
            _ => {}
        }
    }

    fn get_tls(&self) -> *mut c_void {
        self.nogc.tls
    }
}

impl NoGCMutator {
    pub fn new(tls: *mut c_void, space: &'static ImmortalSpace, los: &'static LargeObjectSpace, cos: &'static CodeSpace) -> Self {
        NoGCMutator {
            nogc: BumpAllocator::new(tls, Some(space)),
            los: LargeObjectAllocator::new(tls, Some(los)),
            cos: CodeAllocator::new(tls, Some(cos))
        }
    }
}