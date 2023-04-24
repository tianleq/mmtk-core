use crate::util::{VMMutatorThread, VMThread};
use crate::vm::VMBinding;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Condvar, Mutex};

struct RequestSync {
    request_count: isize,
    last_request_count: isize,
}

/// GC requester.  This object allows other threads to request (trigger) GC,
/// and the GC coordinator thread waits for GC requests using this object.
pub struct GCRequester<VM: VMBinding> {
    request_sync: Mutex<RequestSync>,
    request_condvar: Condvar,
    request_flag: AtomicBool,
    tls: std::sync::Mutex<VMMutatorThread>,
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
            }),
            request_condvar: Condvar::new(),
            request_flag: AtomicBool::new(false),
            tls: std::sync::Mutex::new(VMMutatorThread(VMThread::UNINITIALIZED)),
            phantom: PhantomData,
        }
    }

    pub fn request(&self, thread_local_gc: bool, tls: VMMutatorThread) {
        if self.request_flag.load(Ordering::Relaxed) {
            return;
        }

        let mut guard = self.request_sync.lock().unwrap();
        if !self.request_flag.load(Ordering::Relaxed) {
            self.request_flag.store(true, Ordering::Relaxed);
            if thread_local_gc {
                let mut t = self.tls.lock().unwrap();
                *t = tls;
            }
            guard.request_count += 1;
            self.request_condvar.notify_all();
        }
    }

    pub fn clear_request(&self) {
        let guard = self.request_sync.lock().unwrap();
        self.request_flag.store(false, Ordering::Relaxed);
        {
            let mut t = self.tls.lock().unwrap();
            *t = VMMutatorThread(VMThread::UNINITIALIZED);
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

    pub fn get_tls(&self) -> VMMutatorThread {
        *self.tls.lock().unwrap()
    }
}
