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
use std::time::Instant;

/// The default number of phases for statistics.
pub const DEFAULT_NUM_PHASES: usize = 1 << 12;
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
    requests_time: Arc<Mutex<Timer>>,
    #[cfg(feature = "thread_local_gc")]
    thread_local_gc_count: AtomicUsize,
    #[cfg(feature = "thread_local_gc")]
    forced_local_gc_counters: Mutex<Vec<usize>>,
    #[cfg(feature = "thread_local_gc")]
    /// Time (in nanoseconds) spent in the `Local` work bucket, one entry per GC, recorded as
    /// `(gc number, nanoseconds)`.  A GC in which the `Local` bucket never opened has no entry.
    forced_local_gc_times: Mutex<Vec<(usize, u64)>>,
    #[cfg(feature = "thread_local_gc")]
    /// The time at which the `Local` work bucket was opened for the current GC.
    forced_local_gc_start: Mutex<Option<Instant>>,
    // #[cfg(feature = "thread_local_gc")]
    // forced_local_gc_counters: Mutex<Vec<Arc<Mutex<dyn Counter + Send>>>>,
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

        // #[cfg(feature = "thread_local_gc")]
        // // Need to understand how this work
        // let forced_local_gc_counters: Vec<Arc<Mutex<dyn Counter + Send>>> = vec![];

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
            requests_time,

            #[cfg(feature = "thread_local_gc")]
            thread_local_gc_count: AtomicUsize::new(0),
            #[cfg(feature = "thread_local_gc")]
            forced_local_gc_counters: Mutex::new(Vec::new()),
            #[cfg(feature = "thread_local_gc")]
            forced_local_gc_times: Mutex::new(Vec::new()),
            #[cfg(feature = "thread_local_gc")]
            forced_local_gc_start: Mutex::new(None),
            // #[cfg(feature = "thread_local_gc")]
            // forced_local_gc_counters: Mutex::new(forced_local_gc_counters),
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
        let counters = self.counters.lock().unwrap();
        for counter in &(*counters) {
            counter.lock().unwrap().phase_change(self.get_phase());
        }
        self.shared.increment_phase();
    }

    pub fn end_gc(&self) {
        if !self.get_gathering_stats() {
            return;
        }
        let counters = self.counters.lock().unwrap();
        for counter in &(*counters) {
            counter.lock().unwrap().phase_change(self.get_phase());
        }
        self.shared.increment_phase();
    }

    /// Called when the `Local` work bucket is opened, i.e. once all the mutators are stopped.
    pub fn start_local_bucket(&self) {
        if !self.get_gathering_stats() {
            return;
        }
        *self.forced_local_gc_start.lock().unwrap() = Some(Instant::now());
    }

    /// Called when the `Local` work bucket has been drained, i.e. when the next stop-the-world
    /// bucket is opened.
    pub fn end_local_bucket(&self, local_gc_count: usize) {
        if !self.get_gathering_stats() {
            return;
        }
        // `take` the start time so that a spurious second call cannot count the same
        // interval twice.
        let start = self.forced_local_gc_start.lock().unwrap().take();
        if let Some(start) = start {
            let elapsed = start.elapsed();
            let ns = elapsed.as_secs() * 1_000_000_000 + u64::from(elapsed.subsec_nanos());
            // The current phase is the stop-the-world phase of GC `get_phase() / 2`, which is
            // the same numbering as the `GC` column printed by `print_stats`.
            let gc = self.get_phase() / 2;
            let mut times = self.forced_local_gc_times.lock().unwrap();
            // Should the bucket be opened more than once within one GC, keep the GC's total in
            // a single entry rather than reporting the same GC twice.
            match times.last_mut() {
                Some(last) if last.0 == gc => last.1 += ns,
                _ => times.push((gc, ns)),
            }
        }
        self.forced_local_gc_counters
            .lock()
            .unwrap()
            .push(local_gc_count);
    }

    /// Print the stop-the-world time of every GC, one line per GC.
    ///
    /// This reads the per-phase values already recorded by the `total_time` counter rather than
    /// timing the GC again, so these values are exactly the ones that add up to the `time.stw`
    /// column.
    fn print_gc_times(&self) {
        let timer = self.total_time.lock().unwrap();
        for gc in 0..(self.get_phase() / 2) {
            // Phase `2 * gc` is the mutator time preceding GC `gc`, and `2 * gc + 1` is its
            // stop-the-world time.  `total_time` does not merge phases, so `print_count` prints
            // that single phase, and `MonotoneNanoTime` prints it in milliseconds.
            print!("gc_time GC {}: ", gc);
            timer.print_count(2 * gc + 1);
            println!(" ms");
        }
    }

    /// Print the time spent in the `Local` work bucket for every GC, one line per GC.
    fn print_local_bucket_times(&self) {
        let times = self.forced_local_gc_times.lock().unwrap();
        for (gc, ns) in &(*times) {
            // Kept in nanoseconds internally and converted here, so no precision is lost.
            // Two decimal places matches how `MonotoneNanoTime` prints elapsed time.
            println!("local_bucket_time GC {}: {:.2} ms", gc, *ns as f64 / 1e6f64);
        }
    }

    /// Print the time spent in the `Local` work bucket for every GC, one line per GC.
    fn print_forced_local_gc_count(&self) {
        let counters = self.forced_local_gc_counters.lock().unwrap();
        for count in counters.iter() {
            // Kept in nanoseconds internally and converted here, so no precision is lost.
            // Two decimal places matches how `MonotoneNanoTime` prints elapsed time.
            println!("forced local Gc count {}", count);
        }
    }

    // #[cfg(feature = "thread_local_gc")]
    // pub fn start_all_forced_local_gc(&self) {
    //     if !self.get_gathering_stats() {
    //         return;
    //     }
    //     let forced_local_gc_counters = self.forced_local_gc_counters.lock().unwrap();
    // }

    // #[cfg(feature = "thread_local_gc")]
    // pub fn stop_all_forced_local_gc(&self) {
    //     if !self.get_gathering_stats() {
    //         return;
    //     }
    //     let forced_local_gc_counters = self.forced_local_gc_counters.lock().unwrap();
    // }

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
        // self.print_gc_times();
        // self.print_local_bucket_times();
        // println!(
        //     "Forced local GC: {}",
        //     mmtk.state.forced_local_gc_counter.load(Ordering::Relaxed)
        // );
        // println!(
        //     "Forced local Marking: {}",
        //     mmtk.state
        //         .forced_local_marking_counter
        //         .load(Ordering::Relaxed)
        // );

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
