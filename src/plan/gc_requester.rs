use crate::util::{VMMutatorThread, VMThread};
use crate::vm::VMBinding;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Condvar, Mutex};

struct RequestSync {
    request_count: isize,
    last_request_count: isize,
    single_thread: bool,
    #[cfg(feature = "thread_local_gc")]
    thread_local_requests: Vec<GCRequest>,
    #[cfg(feature = "thread_local_gc")]
    thread_local_request_count: usize,
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
                thread_local_request_count: 0,
                #[cfg(feature = "thread_local_gc")]
                thread_local_requests: Vec::new(),
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
            self.request_flag.store(true, Ordering::Relaxed);
            guard.request_count += 1;
            guard.single_thread = false;
            self.request_condvar.notify_all();
        }
    }

    #[cfg(feature = "thread_local_gc")]
    pub fn request_thread_local_gc(&self, tls: VMMutatorThread) {
        let mut guard = self.request_sync.lock().unwrap();
        let req: GCRequest = GCRequest {
            single_thread: true,
            thread_local: true,
            tls,
        };
        guard.thread_local_request_count += 1;
        guard.thread_local_requests.push(req);

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

    #[cfg(not(feature = "thread_local_gc"))]
    pub fn wait_for_request(&self) -> Vec<GCRequest> {
        let mut guard = self.request_sync.lock().unwrap();
        guard.last_request_count += 1;
        while guard.last_request_count == guard.request_count {
            guard = self.request_condvar.wait(guard).unwrap();
        }

        return vec![GCRequest {
            single_thread: guard.single_thread,
            thread_local: false,
            tls: VMMutatorThread(VMThread::UNINITIALIZED),
        }];
    }

    #[cfg(feature = "thread_local_gc")]
    pub fn wait_for_request(&self) -> Vec<GCRequest> {
        let mut guard = self.request_sync.lock().unwrap();
        guard.last_request_count += 1;
        while guard.last_request_count == guard.request_count
            && guard.thread_local_requests.is_empty()
        {
            guard = self.request_condvar.wait(guard).unwrap();
            println!(
                "GC Controller waked up. len: {}, count: {}",
                guard.thread_local_requests.len(),
                guard.thread_local_request_count
            );
        }
        // check if local gc is triggered
        let requests: Vec<GCRequest> = guard.thread_local_requests.drain(..).collect();
        if !requests.is_empty() {
            debug_assert!(
                guard.thread_local_request_count != 0
                    && guard.thread_local_request_count == requests.len(),
                "thread local gc count is corrupted"
            );
            // thread local gc should not increase this count
            // It is used by global gc only
            guard.last_request_count -= 1;
            guard.thread_local_request_count = 0;
            return requests;
        } else {
            debug_assert!(
                guard.last_request_count != guard.request_count,
                "global gc request count is corrupted"
            );
            return vec![GCRequest {
                single_thread: guard.single_thread,
                thread_local: false,
                tls: VMMutatorThread(VMThread::UNINITIALIZED),
            }];
        }
    }
}
