use crate::util::{VMMutatorThread, VMThread};
use crate::vm::VMBinding;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Condvar, Mutex};

struct RequestSync {
    request_count: isize,
    last_request_count: isize,
    single_thread: bool,
}

pub struct GCRequest {
    pub single_thread: bool,
    pub thread_local: bool,
    pub tls: VMMutatorThread,
}

/// GC requester.  This object allows other threads to request (trigger) GC,
/// and the GC coordinator thread waits for GC requests using this object.
pub struct GCRequester<VM: VMBinding> {
    request_sync: Mutex<RequestSync>,
    thread_local_request_sync: Mutex<Vec<GCRequest>>,
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
            }),
            thread_local_request_sync: Mutex::new(vec![]),
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
            self.request_flag.store(true, Ordering::Relaxed);
            guard.request_count += 1;
            guard.single_thread = false;
            self.request_condvar.notify_all();
        }
    }

    pub fn request_thread_local_gc(&self, tls: VMMutatorThread) {
        let mut guard = self.thread_local_request_sync.lock().unwrap();
        let req = GCRequest {
            single_thread: true,
            thread_local: true,
            tls,
        };
        guard.push(req);
        self.request_condvar.notify_all();
    }

    pub fn request_single_thread_gc(&self) {
        if self.request_flag.load(Ordering::Relaxed) {
            return;
        }

        let mut guard = self.request_sync.lock().unwrap();
        if !self.request_flag.load(Ordering::Relaxed) {
            self.request_flag.store(true, Ordering::Relaxed);
            guard.request_count += 1;
            guard.single_thread = true;
            self.request_condvar.notify_all();
        }
    }

    pub fn clear_request(&self) {
        let guard = self.request_sync.lock().unwrap();
        self.request_flag.store(false, Ordering::Relaxed);

        drop(guard);
    }

    pub fn wait_for_request(&self) -> GCRequest {
        {
            // check thread-local request
            let mut guard = self.thread_local_request_sync.lock().unwrap();
            if let Some(req) = guard.pop() {
                return GCRequest {
                    single_thread: req.single_thread,
                    thread_local: req.thread_local,
                    tls: req.tls,
                };
            }
        }

        let mut guard = self.request_sync.lock().unwrap();
        guard.last_request_count += 1;
        while guard.last_request_count == guard.request_count {
            guard = self.request_condvar.wait(guard).unwrap();
        }
        return GCRequest {
            single_thread: guard.single_thread,
            thread_local: false,
            tls: VMMutatorThread(VMThread::UNINITIALIZED),
        };
    }
}
