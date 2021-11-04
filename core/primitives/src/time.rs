use std::default::Default;
use std::sync::{Arc, Mutex};

use chrono;
use once_cell::sync::Lazy;

pub use chrono::Utc;
pub use std::time::{Duration, Instant};

use chrono::DateTime;
use std::collections::{HashMap, VecDeque};
use std::ops::Deref;
use std::thread::ThreadId;

pub struct MockClockPerThread {
    utc: VecDeque<DateTime<Utc>>,
    durations: VecDeque<Duration>,
    utc_call_count: u64,
    instant_call_count: u64,
    instant: Instant,
}

pub struct Clock {
    clocks_per_thread: HashMap<ThreadId, MockClockPerThread>,
}

impl MockClockPerThread {
    pub fn reset(&mut self) {
        self.utc.clear();
        self.durations.clear();
        self.utc_call_count = 0;
        self.instant_call_count = 0;
        self.instant = Instant::now();
    }
}

impl Default for MockClockPerThread {
    fn default() -> Self {
        Self {
            utc: VecDeque::with_capacity(16),
            durations: VecDeque::with_capacity(16),
            utc_call_count: 0,
            instant_call_count: 0,
            instant: Instant::now(),
        }
    }
}

impl Default for Clock {
    fn default() -> Self {
        Self { clocks_per_thread: HashMap::default() }
    }
}

static SINGLETON: Lazy<Arc<Mutex<Clock>>> = Lazy::new(|| Arc::from(Mutex::new(Clock::new())));

impl Clock {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get() -> &'static Arc<Mutex<Clock>> {
        SINGLETON.deref()
    }
    pub fn reset(&mut self) {
        self.current_mut().reset();
    }
    pub fn add_utc(&mut self, mock_date: DateTime<chrono::Utc>) {
        self.current_mut().utc.push_back(mock_date);
    }

    pub fn pop_utc(&mut self) -> Option<DateTime<chrono::Utc>> {
        let instance = self.current_mut();
        instance.utc_call_count += 1;
        instance.utc.pop_front()
    }

    pub fn pop_instant(&mut self) -> Option<Instant> {
        let instance = self.current_mut();
        instance.instant_call_count += 1;
        let x = instance.durations.pop_front();
        match x {
            Some(t) => instance.instant.checked_add(t),
            None => None,
        }
    }

    pub fn current_mut(&mut self) -> &mut MockClockPerThread {
        let handle = std::thread::current();
        let id = handle.id();
        self.clocks_per_thread.entry(id).or_default()
    }

    pub fn current(&self) -> Option<&MockClockPerThread> {
        let handle = std::thread::current();
        let id = handle.id();
        self.clocks_per_thread.get(&id)
    }

    pub fn add_instant(&mut self, mock_instant: Duration) {
        self.current_mut().durations.push_back(mock_instant);
    }

    pub fn instant_call_count(&mut self) -> u64 {
        let instance = self.current_mut();
        instance.instant_call_count
    }

    pub fn utc_call_count(&self) -> u64 {
        self.current().unwrap().utc_call_count
    }

    pub fn count_instant(&self) -> usize {
        self.current().unwrap().durations.len()
    }

    pub fn utc() -> DateTime<chrono::Utc> {
        let time_singleton = Clock::get();
        let x = time_singleton.lock().unwrap().pop_utc();
        match x {
            Some(t) => t,
            None => chrono::Utc::now(),
        }
    }

    pub fn instant() -> Instant {
        let time_singleton = Clock::get();
        let x = time_singleton.lock().unwrap().pop_instant();
        match x {
            Some(t) => t,
            None => Instant::now(),
        }
    }
}
