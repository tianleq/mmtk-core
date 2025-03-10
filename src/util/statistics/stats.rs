use crate::mmtk::MMTK;
use crate::util::options::Options;
use crate::util::statistics::counter::*;
use crate::util::statistics::Timer;
use crate::vm::VMBinding;

#[cfg(feature = "perf_counter")]
use pfm::Perfmon;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Mutex;

// TODO: Increasing this number would cause JikesRVM die at boot time. I don't really know why.
// E.g. using 1 << 14 will cause JikesRVM segfault at boot time.
pub const MAX_PHASES: usize = 1 << 12;
pub const MAX_COUNTERS: usize = 100;

/// GC stats shared among counters
pub struct SharedStats {
    phase: AtomicUsize,
    gathering_stats: AtomicBool,
}

impl SharedStats {
    fn increment_phase(&self) {
        self.phase.fetch_add(1, Ordering::SeqCst);
    }

    pub fn get_phase(&self) -> usize {
        self.phase.load(Ordering::SeqCst)
    }

    pub fn get_gathering_stats(&self) -> bool {
        self.gathering_stats.load(Ordering::SeqCst)
    }

    fn set_gathering_stats(&self, val: bool) {
        self.gathering_stats.store(val, Ordering::SeqCst);
    }
}

/// GC statistics
///
/// The struct holds basic GC statistics, like the GC count,
/// and an array of counters.
pub struct Stats {
    gc_count: AtomicUsize,
    total_time: Arc<Mutex<Timer>>,
    // crate `pfm` uses libpfm4 under the hood for parsing perf event names
    // Initialization of libpfm4 is required before we can use `PerfEvent` types
    #[cfg(feature = "perf_counter")]
    perfmon: Perfmon,

    pub shared: Arc<SharedStats>,
    counters: Mutex<Vec<Arc<Mutex<dyn Counter + Send>>>>,
    exceeded_phase_limit: AtomicBool,
    requests_time: Arc<Mutex<Timer>>,
    #[cfg(feature = "thread_local_gc")]
    thread_local_gc_count: AtomicUsize,
}

impl Stats {
    #[allow(unused)]
    pub fn new(options: &Options) -> Self {
        // Create a perfmon instance and initialize it
        // we use perfmon to parse perf event names
        #[cfg(feature = "perf_counter")]
        let perfmon = {
            let mut perfmon: Perfmon = Default::default();
            perfmon.initialize().expect("Perfmon failed to initialize");
            perfmon
        };
        let shared = Arc::new(SharedStats {
            phase: AtomicUsize::new(0),
            gathering_stats: AtomicBool::new(false),
        });
        let mut counters: Vec<Arc<Mutex<dyn Counter + Send>>> = vec![];
        // We always have a time counter enabled
        let t = Arc::new(Mutex::new(LongCounter::new(
            "time".to_string(),
            shared.clone(),
            true,
            true,
            false,
            MonotoneNanoTime {},
        )));
        let requests_time = Arc::new(Mutex::new(LongCounter::new(
            "requests_time".to_string(),
            shared.clone(),
            false,
            false,
            false,
            MonotoneNanoTime {},
        )));

        counters.push(t.clone());
        counters.push(requests_time.clone());

        // Read from the MMTK option for a list of perf events we want to
        // measure, and create corresponding counters
        #[cfg(feature = "perf_counter")]
        for e in &options.phase_perf_events.events {
            counters.push(Arc::new(Mutex::new(LongCounter::new(
                e.0.clone(),
                shared.clone(),
                true,
                false,
                PerfEventDiffable::new(&e.0, *options.perf_exclude_kernel),
            ))));
        }
        Stats {
            gc_count: AtomicUsize::new(0),
            total_time: t,
            #[cfg(feature = "perf_counter")]
            perfmon,

            shared,
            counters: Mutex::new(counters),
            exceeded_phase_limit: AtomicBool::new(false),
            requests_time,
            #[cfg(feature = "thread_local_gc")]
            thread_local_gc_count: AtomicUsize::new(0),
        }
    }

    pub fn new_event_counter(
        &self,
        name: &str,
        implicit_start: bool,
        implicit_stop: bool,
        merge_phases: bool,
    ) -> Arc<Mutex<EventCounter>> {
        let mut guard = self.counters.lock().unwrap();
        let counter = Arc::new(Mutex::new(EventCounter::new(
            name.to_string(),
            self.shared.clone(),
            implicit_start,
            implicit_stop,
            merge_phases,
        )));
        guard.push(counter.clone());
        counter
    }

    pub fn new_size_counter(
        &self,
        name: &str,
        implicit_start: bool,
        implicit_stop: bool,
        merge_phases: bool,
    ) -> Mutex<SizeCounter> {
        let u = self.new_event_counter(name, implicit_start, implicit_stop, merge_phases);
        let v = self.new_event_counter(
            &format!("{}.volume", name),
            implicit_start,
            implicit_stop,
            merge_phases,
        );
        Mutex::new(SizeCounter::new(u, v))
    }

    pub fn new_timer(
        &self,
        name: &str,
        implicit_start: bool,
        implicit_stop: bool,
        merge_phases: bool,
    ) -> Arc<Mutex<Timer>> {
        let mut guard = self.counters.lock().unwrap();
        let counter = Arc::new(Mutex::new(Timer::new(
            name.to_string(),
            self.shared.clone(),
            implicit_start,
            implicit_stop,
            merge_phases,
            MonotoneNanoTime {},
        )));
        guard.push(counter.clone());
        counter
    }

    pub fn start_gc(&self) {
        self.gc_count.fetch_add(1, Ordering::SeqCst);
        if !self.get_gathering_stats() {
            return;
        }
        if self.get_phase() < MAX_PHASES - 1 {
            let counters = self.counters.lock().unwrap();
            for counter in &(*counters) {
                counter.lock().unwrap().phase_change(self.get_phase());
            }
            self.shared.increment_phase();
        } else if !self.exceeded_phase_limit.load(Ordering::SeqCst) {
            eprintln!("Warning: number of GC phases exceeds MAX_PHASES");
            self.exceeded_phase_limit.store(true, Ordering::SeqCst);
        }
    }

    pub fn end_gc(&self) {
        if !self.get_gathering_stats() {
            return;
        }
        if self.get_phase() < MAX_PHASES - 1 {
            let counters = self.counters.lock().unwrap();
            for counter in &(*counters) {
                counter.lock().unwrap().phase_change(self.get_phase());
            }
            self.shared.increment_phase();
        } else if !self.exceeded_phase_limit.load(Ordering::SeqCst) {
            eprintln!("Warning: number of GC phases exceeds MAX_PHASES");
            self.exceeded_phase_limit.store(true, Ordering::SeqCst);
        }
    }

    #[cfg(feature = "thread_local_gc")]
    pub fn start_local_gc(&self) {
        if !self.get_gathering_stats() {
            return;
        }
        self.thread_local_gc_count.fetch_add(1, Ordering::SeqCst);
    }

    #[cfg(feature = "thread_local_gc")]
    pub fn end_local_gc(&self) {}

    pub fn print_stats<VM: VMBinding>(&self, mmtk: &'static MMTK<VM>) {
        println!(
            "============================ MMTk Statistics Totals ============================"
        );
        let scheduler_stat = mmtk.scheduler.statistics();
        self.print_column_names(&scheduler_stat);
        print!("{}\t", self.get_phase() / 2);

        print!("{}\t", self.thread_local_gc_count.load(Ordering::SeqCst));

        let counter = self.counters.lock().unwrap();
        for iter in &(*counter) {
            let c = iter.lock().unwrap();
            if c.merge_phases() {
                c.print_total(None);
            } else {
                c.print_total(Some(true));
                print!("\t");
                c.print_total(Some(false));
            }
            print!("\t");
        }
        for value in scheduler_stat.values() {
            print!("{}\t", value);
        }

        println!();
        print!("Total time: ");
        self.total_time.lock().unwrap().print_total(None);
        println!(" ms");
        #[cfg(feature = "thread_local_gc")]
        {
            use crate::REQUEST_SCOPE_ALLOCATION_COUNT;
            use crate::REQUEST_SCOPE_ALLOCATION_SIZE;
            use crate::REQUEST_SCOPE_PUBLICATION_COUNT;
            use crate::REQUEST_SCOPE_PUBLICATION_SIZE;
            use crate::REQUEST_SCOPE_SERVER_ALLOCATION_COUNT;
            use crate::REQUEST_SCOPE_SERVER_ALLOCATION_SIZE;

            let request_scope_allocation_count =
                REQUEST_SCOPE_ALLOCATION_COUNT.load(std::sync::atomic::Ordering::Acquire);
            let request_scope_allocation_size =
                REQUEST_SCOPE_ALLOCATION_SIZE.load(std::sync::atomic::Ordering::Acquire);
            let request_scope_publication_count =
                REQUEST_SCOPE_PUBLICATION_COUNT.load(std::sync::atomic::Ordering::Acquire);
            let request_scope_publication_size =
                REQUEST_SCOPE_PUBLICATION_SIZE.load(std::sync::atomic::Ordering::Acquire);

            let requests_time = self.requests_time.lock().unwrap().get_total(None);

            let all_scope_allocation_count =
                crate::ALLOCATION_COUNT.load(std::sync::atomic::Ordering::Acquire);
            let all_scope_allocation_size =
                crate::ALLOCATION_SIZE.load(std::sync::atomic::Ordering::Acquire);
            let all_scope_publication_count =
                crate::PUBLICATION_COUNT.load(std::sync::atomic::Ordering::Acquire);
            let all_scope_publication_size =
                crate::PUBLICATION_SIZE.load(std::sync::atomic::Ordering::Acquire);

            let total_time = self.total_time.lock().unwrap().get_total(None);
            let bytes_evacuated =
                crate::scheduler::thread_local_gc_work::BYTES_EVACUATED.load(Ordering::Acquire);
            let bytes_left =
                crate::scheduler::thread_local_gc_work::BYTES_LEFT.load(Ordering::Acquire);
            let public_bytes_traced =
                crate::scheduler::thread_local_gc_work::PUBLIC_BYTES_TRACED.load(Ordering::Acquire);
            println!("Bytes Evacuated: {}", bytes_evacuated);
            println!("Bytes Left: {}", bytes_left);
            println!("Public bytes traced: {}", public_bytes_traced);
            println!(
                "request scope bytes publication rate: {}, {}, {}, {}",
                request_scope_publication_size,
                request_scope_allocation_size,
                request_scope_publication_size as f64 / request_scope_allocation_size as f64,
                (request_scope_publication_size as f64 / requests_time as f64) * 1e9f64
            );
            println!(
                "request scope objects publication rate: {}, {}, {}, {}",
                request_scope_publication_count,
                request_scope_allocation_count,
                request_scope_publication_count as f64 / request_scope_allocation_count as f64,
                (request_scope_publication_count as f64 / requests_time as f64) * 1e9f64
            );

            println!(
                "request scope server side bytes/objects: {}/{}",
                REQUEST_SCOPE_SERVER_ALLOCATION_SIZE.load(std::sync::atomic::Ordering::Acquire),
                REQUEST_SCOPE_SERVER_ALLOCATION_COUNT.load(std::sync::atomic::Ordering::Acquire),
            );

            println!(
                "all scope bytes publication rate: {}, {}, {}, {}",
                all_scope_publication_size,
                all_scope_allocation_size,
                all_scope_publication_size as f64 / all_scope_allocation_size as f64,
                (request_scope_publication_size as f64 / total_time as f64) * 1e9f64
            );
            println!(
                "all scope objects publication rate: {}, {}, {}, {}",
                all_scope_publication_count,
                all_scope_allocation_count,
                all_scope_publication_count as f64 / all_scope_allocation_count as f64,
                (all_scope_publication_count as f64 / total_time as f64) * 1e9f64
            );
        }

        println!("------------------------------ End MMTk Statistics -----------------------------")
    }

    pub fn print_column_names(&self, scheduler_stat: &HashMap<String, String>) {
        print!("GC\t");
        print!("LOCAL_GC\t");
        let counter = self.counters.lock().unwrap();
        for iter in &(*counter) {
            let c = iter.lock().unwrap();
            if c.merge_phases() {
                print!("{}\t", c.name());
            } else {
                print!("{}.other\t{}.stw\t", c.name(), c.name());
            }
        }
        for name in scheduler_stat.keys() {
            print!("{}\t", name);
        }

        println!();
    }

    pub fn start_all(&self) {
        let counters = self.counters.lock().unwrap();
        if self.get_gathering_stats() {
            panic!("calling Stats.startAll() while stats running");
        }
        self.shared.set_gathering_stats(true);

        for c in &(*counters) {
            let mut ctr = c.lock().unwrap();
            if ctr.implicitly_start() {
                ctr.start();
            }
        }
    }

    pub fn stop_all<VM: VMBinding>(&self, mmtk: &'static MMTK<VM>) {
        self.stop_all_counters();
        self.print_stats(mmtk);
    }

    fn stop_all_counters(&self) {
        let counters = self.counters.lock().unwrap();
        for c in &(*counters) {
            let mut ctr = c.lock().unwrap();
            if ctr.implicit_stop() {
                ctr.stop();
            }
        }
        self.shared.set_gathering_stats(false);
    }

    fn get_phase(&self) -> usize {
        self.shared.get_phase()
    }

    pub fn get_gathering_stats(&self) -> bool {
        self.shared.get_gathering_stats()
    }

    pub fn request_starting(&self) {
        self.requests_time.lock().unwrap().start();
    }

    pub fn request_finished(&self) {
        self.requests_time.lock().unwrap().stop();
    }
}
