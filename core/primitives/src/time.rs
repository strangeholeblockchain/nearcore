use std::default::Default;
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;
use chrono;
use once_cell::sync::Lazy;

pub use chrono::Utc;
pub use std::time::{Duration, Instant};

use std::collections::{HashMap, VecDeque};
use std::thread::ThreadId;

pub struct MockTimeSingletonPerThread {
    utc: VecDeque<chrono::DateTime<Utc>>,
    durations: VecDeque<Duration>,
    utc_call_count: u64,
    instant_call_count: u64,
    instant: Instant,
}

pub struct MockTimeSingleton {
    threads: HashMap<ThreadId, MockTimeSingletonPerThread>,
}

impl MockTimeSingletonPerThread {
    pub fn reset(&mut self) {
        self.utc.clear();
        self.durations.clear();
        self.utc_call_count = 0;
        self.instant_call_count = 0;
        self.instant = Instant::now();
    }
}

impl Default for MockTimeSingletonPerThread {
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

impl Default for MockTimeSingleton {
    fn default() -> Self {
        Self { threads: HashMap::default() }
    }
}

static SINGLETON: Lazy<ArcSwap<Mutex<MockTimeSingleton>>> =
    Lazy::new(|| ArcSwap::from_pointee(Mutex::new(MockTimeSingleton::new())));

impl MockTimeSingleton {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get() -> Arc<Mutex<MockTimeSingleton>> {
        SINGLETON.load_full()
    }

    pub fn set(value: MockTimeSingleton) {
        SINGLETON.store(Arc::new(Mutex::new(value)))
    }

    pub fn add_utc(&mut self, mock_date: chrono::DateTime<chrono::Utc>) {
        self.current_mut().unwrap().utc.push_back(mock_date);
    }

    pub fn pop_utc(&mut self) -> Option<chrono::DateTime<chrono::Utc>> {
        let instance = self.current_mut().unwrap();
        instance.utc_call_count += 1;
        instance.utc.pop_front()
    }

    pub fn pop_instant(&mut self) -> Option<Instant> {
        let instance = self.current_mut()?;
        instance.instant_call_count += 1;
        let x = instance.durations.pop_front();
        match x {
            Some(t) => instance.instant.checked_add(t),
            None => None,
        }
    }

    pub fn current_mut(&mut self) -> Option<&mut MockTimeSingletonPerThread> {
        let handle = std::thread::current();
        let id = handle.id();
        if !self.threads.contains_key(&id) {
            self.threads.insert(id, MockTimeSingletonPerThread::default());
        }
        self.threads.get_mut(&id)
    }

    pub fn current(&self) -> Option<&MockTimeSingletonPerThread> {
        let handle = std::thread::current();
        let id = handle.id();
        self.threads.get(&id)
    }

    pub fn reset(&mut self) {
        self.current_mut().unwrap().reset();
    }

    pub fn add_instant(&mut self, mock_instant: Duration) {
        self.current_mut().unwrap().durations.push_back(mock_instant);
    }

    pub fn get_instant_call_count(&mut self) -> u64 {
        let instance = self.current_mut();
        match instance {
            Some(t) => t.instant_call_count,
            None => 0,
        }
    }

    pub fn get_utc_call_count(&self) -> u64 {
        self.current().unwrap().utc_call_count
    }

    pub fn count_instant(&self) -> usize {
        self.current().unwrap().durations.len()
    }
}

pub trait MockTime {
    type Value;

    fn now_or_mock() -> Self::Value;

    fn system_time() -> Self::Value;
}

impl MockTime for Utc {
    type Value = chrono::DateTime<chrono::Utc>;

    fn now_or_mock() -> chrono::DateTime<chrono::Utc> {
        let time_singleton = MockTimeSingleton::get();
        let x = time_singleton.lock().unwrap().pop_utc();
        match x {
            Some(t) => t,
            None => chrono::Utc::now(),
        }
    }

    fn system_time() -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now()
    }
}

impl MockTime for Instant {
    type Value = Instant;

    fn now_or_mock() -> Instant {
        let time_singleton = MockTimeSingleton::get();
        let x = time_singleton.lock().unwrap().pop_instant();
        match x {
            Some(t) => t,
            None => Instant::now(),
        }
    }

    fn system_time() -> Instant {
        Instant::now()
    }
}
