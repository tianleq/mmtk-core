use crate::scheduler::GCWorkScheduler;
#[cfg(feature = "thread_local_gc")]
use crate::util::VMMutatorThread;
use crate::vm::VMBinding;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Condvar, Mutex};

struct RequestSync {
    request_count: isize,
    last_request_count: isize,
    single_thread: bool,
    #[cfg(feature = "thread_local_gc")]
    global_gc_requested: bool,
}

/// GC requester.  This object allows other threads to request (trigger) GC,
/// and the GC coordinator thread waits for GC requests using this object.
pub struct GCRequester<VM: VMBinding> {
    /// Set by mutators to trigger GC.  It is atomic so that mutators can check if GC has already
    /// been requested efficiently in `poll` without acquiring any mutex.
    request_flag: AtomicBool,
    scheduler: Arc<GCWorkScheduler<VM>>,
}

impl<VM: VMBinding> GCRequester<VM> {
    pub fn new(scheduler: Arc<GCWorkScheduler<VM>>) -> Self {
        GCRequester {
            request_sync: Mutex::new(RequestSync {
                request_count: 0,
                last_request_count: -1,
                single_thread: false,
                #[cfg(feature = "thread_local_gc")]
                global_gc_requested: false,
            }),
            request_condvar: Condvar::new(),
            request_flag: AtomicBool::new(false),
            scheduler,
        }
    }

    /// Request a GC.  Called by mutators when polling (during allocation) and when handling user
    /// GC requests (e.g. `System.gc();` in Java).
    pub fn request(&self) {
        if self.request_flag.load(Ordering::Relaxed) {
            return;
        }

        if !self.request_flag.swap(true, Ordering::Relaxed) {
            #[cfg(feature = "thread_local_gc")]
            {
                // `GCWorkScheduler::request_schedule_collection` needs to hold a mutex to communicate
                // with GC workers, which is expensive for functions like `poll`.  We use the atomic
                guard.global_gc_requested = true;
            }

            // flag `request_flag` to elide the need to acquire the mutex in subsequent calls.
            guard.single_thread = false;
            self.scheduler.request_schedule_collection();
        }
    }

    #[cfg(feature = "thread_local_gc")]
    pub fn request_thread_local_gc(&self, _tls: VMMutatorThread) -> bool {
        // If global gc has been requested, then skip the local gc
        if self.request_flag.load(Ordering::Relaxed) {
            return false;
        }

        let guard = self.request_sync.lock().unwrap();

        !guard.global_gc_requested
    }

    #[cfg(feature = "thread_local_gc")]
    pub fn request_thread_local_gc(&self, _tls: VMMutatorThread) -> bool {
        // If global gc has been requested, then skip the local gc
        if self.request_flag.load(Ordering::Relaxed) {
            return false;
        }
        let guard = self.request_sync.lock().unwrap();

        !guard.global_gc_requested
    }

    /// Clear the "GC requested" flag so that mutators can trigger the next GC.
    /// Called by a GC worker when all mutators have come to a stop.
    pub fn clear_request(&self) {
        #[cfg(feature = "thread_local_gc")]
        let mut guard = self.request_sync.lock().unwrap();
        #[cfg(not(feature = "thread_local_gc"))]
        self.request_flag.store(false, Ordering::Relaxed);
        #[cfg(feature = "thread_local_gc")]
        {
            guard.global_gc_requested = false;
        }
    }
}
