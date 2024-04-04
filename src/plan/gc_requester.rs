#[cfg(feature = "thread_local_gc")]
use crate::util::VMMutatorThread;
use crate::vm::VMBinding;
use std::marker::PhantomData;
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
    request_sync: Mutex<RequestSync>,
    request_condvar: Condvar,
    request_flag: AtomicBool,
    phantom: PhantomData<VM>,
}

// Clippy says we need this...
impl<VM: VMBinding> Default for GCRequester<VM> {
    fn default() -> Self {
        Self::new()
    }
}

impl<VM: VMBinding> GCRequester<VM> {
    pub fn new() -> Self {
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
            phantom: PhantomData,
        }
    }

    pub fn request(&self) {
        if self.request_flag.load(Ordering::Relaxed) {
            return;
        }

        let mut guard = self.request_sync.lock().unwrap();
        if !self.request_flag.load(Ordering::Relaxed) {
            #[cfg(feature = "thread_local_gc")]
            {
                self.request_flag.store(true, Ordering::Relaxed);
                guard.global_gc_requested = true;
            }

            guard.request_count += 1;
            guard.single_thread = false;
            self.request_condvar.notify_all();
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

    pub fn clear_request(&self) {
        #[cfg(feature = "thread_local_gc")]
        let mut guard = self.request_sync.lock().unwrap();
        #[cfg(not(feature = "thread_local_gc"))]
        let guard = self.request_sync.lock().unwrap();
        self.request_flag.store(false, Ordering::Relaxed);
        #[cfg(feature = "thread_local_gc")]
        {
            guard.global_gc_requested = false;
        }

        drop(guard);
    }

    pub fn wait_for_request(&self) {
        let mut guard = self.request_sync.lock().unwrap();
        guard.last_request_count += 1;
        while guard.last_request_count == guard.request_count {
            guard = self.request_condvar.wait(guard).unwrap();
        }
    }
}
