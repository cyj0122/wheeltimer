//! # wheeltimer
//!
//! An implementation of the time wheel algorithm in Rust, inspired by Netty's `HashedWheelTimer`.
//! Provides functionality for creating and cancelling delayed tasks in an asynchronous environment.
//!
//! ## Features
//! - Asynchronous task scheduling with configurable tick duration and wheel size
//! - Support for cancellation of pending timeouts
//! - Thread-safe design using `Arc`, `RwLock`, and `Mutex`


use std::cmp::max;
use std::collections::VecDeque;
use std::fmt::{Display, Formatter};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicU8, AtomicUsize};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{Mutex, Notify, RwLock, RwLockWriteGuard};

const WORKER_STATE_INIT: u8 = 0;
const WORKER_STATE_STARTED: u8 = 1;
const WORKER_STATE_SHUTDOWN: u8 = 2;

/// Enumeration of possible errors returned by timer operations
#[derive(Debug)]
pub enum Error {
    /// Ticks per wheel must be greater than zero
    TicksPerWheelError(u32),

    /// Number of pending timeouts exceeds allowed maximum
    MaxPendingTimeoutCountError(u64, u64),

    /// Generic unknown error
    UnknownError,
}


impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::TicksPerWheelError(ticks_per_wheel) => {
                write!(f, "ticksPerWheel must be greater than 0: {}", ticks_per_wheel)
            }
            Error::MaxPendingTimeoutCountError(pending_timeouts, max_pending_timeouts) => {
                write!(f, "Number of pending timeouts ({}) is greater than or equal to maximum allowed pending timeouts ({})", pending_timeouts, max_pending_timeouts)
            }
            Error::UnknownError => {
                write!(f, "unknown error")
            }
        }
    }
}

/// A reference-counted handle to a time wheel-based timer.
///
/// # Examples
/// ```
/// use wheeltimer::WheelTimer;
///
/// #[tokio::main]
/// async fn main() {
///     let timer = WheelTimer::new(tokio::time::Duration::from_millis(100), 1024, 100000).unwrap();
///     timer.start().await;
///     let timeout = timer.new_timeout(tokio::time::Duration::from_secs(2), |timeout| {
///         async move {
///             // async task logic here
///             println!("a delay task is executed");
///         }
///     }).await;
///
///     tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
///     timer.stop().await;
/// }
///
/// ```
#[derive(Clone)]
pub struct WheelTimer {
    inner: Arc<WheelTimerInner>,
}

struct WheelTimerInner {
    worker_state: AtomicU8,
    tick_duration: u64,
    wheel: Vec<WheelBucket>,
    mask: u32,
    timeout_queue: Mutex<VecDeque<Timeout>>,
    cancelled_queue: Mutex<VecDeque<Timeout>>,
    pending_timeouts: AtomicU64,
    max_pending_timeouts: u64,
    start_time: RwLock<u64>,
}

impl WheelTimer {
    pub fn new(tick_duration: Duration, ticks_per_wheel: u32, max_pending_timeouts: u64) -> Result<Self, Error> {
        if ticks_per_wheel == 0 {
            return Err(Error::TicksPerWheelError(ticks_per_wheel));
        }
        let mut temp = 1;
        while temp < ticks_per_wheel {
            temp <<= 1;
        }
        let ticks_per_wheel = temp;
        let wheel: Vec<WheelBucket> = (0..temp).map(|_| {
            WheelBucket::new()
        }).collect();
        let mask = ticks_per_wheel - 1;
        Ok(Self {
            inner: Arc::new(WheelTimerInner {
                worker_state: AtomicU8::new(WORKER_STATE_INIT),
                tick_duration: tick_duration.as_nanos() as u64,
                wheel,
                mask,
                timeout_queue: Mutex::new(VecDeque::new()),
                cancelled_queue: Mutex::new(VecDeque::new()),
                pending_timeouts: AtomicU64::new(0),
                max_pending_timeouts,
                start_time: RwLock::new(0),
            })
        })
    }

    pub async fn start(&self) {
        if let Ok(_) = self.inner.worker_state.compare_exchange(WORKER_STATE_INIT, WORKER_STATE_STARTED, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst) {
            let self_clone = self.clone();
            let count_down_latch = CountDownLatch::new(1);
            let cdl_clone = count_down_latch.clone();
            tokio::spawn(async move {
                let mut worker = Worker::new(self_clone);
                worker.run(&cdl_clone).await;
            });
            count_down_latch.wait().await;
        }
    }

    pub async fn stop(&self) {
        let _ = self.inner.worker_state.compare_exchange(WORKER_STATE_STARTED, WORKER_STATE_SHUTDOWN, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst);
    }

    pub async fn new_timeout<F, Fut>(&self, delay: Duration, task: F) -> Result<Timeout, Error>
    where
        F: Fn(Timeout) -> Fut + Send + Sync + 'static,
        Fut: Future<Output=()> + Send + 'static,
    {
        let current_pending_timeouts = self.inner.pending_timeouts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        if self.inner.max_pending_timeouts > 0 && current_pending_timeouts > self.inner.max_pending_timeouts {
            self.inner.pending_timeouts.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            return Err(Error::MaxPendingTimeoutCountError(current_pending_timeouts, self.inner.max_pending_timeouts));
        }

        self.start().await;

        let deadline = {
            let start_time = *self.inner.start_time.read().await;
            system_time_nanos() + delay.as_nanos() as u64 - start_time
        };

        let timeout = Timeout::new(self.clone(), deadline, Box::new(move |timeout| Box::pin(task(timeout))));
        self.inner.timeout_queue.lock().await.push_back(timeout.clone());

        Ok(timeout)
    }

    pub fn pending_timeouts(&self) -> u64 {
        self.inner.pending_timeouts.load(std::sync::atomic::Ordering::SeqCst)
    }
}

fn system_time_nanos() -> u64 {
    let since_the_epoch = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
    since_the_epoch.as_secs() * 1_000_000_000 + since_the_epoch.subsec_nanos() as u64
}

struct Worker {
    timer: WheelTimer,
    tick: u64,
    tick_duration: u64,
    wheel_length: u64,
    unprocessed_timeouts: Vec<Timeout>,
}

impl Worker {
    fn new(timer: WheelTimer) -> Self {
        Worker {
            tick_duration: timer.inner.tick_duration,
            wheel_length: timer.inner.wheel.len() as u64,
            timer,
            tick: 0,
            unprocessed_timeouts: Vec::new(),
        }
    }

    async fn run(&mut self, count_down_latch: &CountDownLatch) {
        {
            let mut start_time = self.timer.inner.start_time.write().await;
            *start_time = system_time_nanos();

            count_down_latch.count_down();
        }
        let mut interval = tokio::time::interval(Duration::from_nanos(self.tick_duration));
        loop {
            if self.timer.inner.worker_state.load(std::sync::atomic::Ordering::SeqCst) == WORKER_STATE_STARTED {
                interval.tick().await;
                if self.timer.inner.worker_state.load(std::sync::atomic::Ordering::SeqCst) != WORKER_STATE_STARTED {
                    break;
                }
            } else {
                break;
            }
            let deadline = self.tick_duration * (self.tick + 1);
            let idx = self.tick & self.timer.inner.mask as u64;
            self.process_cancelled_tasks().await;
            let bucket = self.timer.inner.wheel.get(idx as usize).unwrap();
            self.transfer_timeouts_to_buckets().await;
            bucket.expire_timeouts(deadline).await;
            self.tick += 1;
            continue;
        }

        for bucket in self.timer.inner.wheel.iter() {
            bucket.clear_timeouts(&mut self.unprocessed_timeouts).await;
        }

        loop {
            let mut timeout_queue = self.timer.inner.timeout_queue.lock().await;
            if let Some(timeout) = timeout_queue.pop_front() {
                if !timeout.is_cancelled() {
                    self.unprocessed_timeouts.push(timeout);
                }
            } else {
                break;
            }
        }
        self.process_cancelled_tasks().await;
    }

    async fn process_cancelled_tasks(&self) {
        let mut cancelled_queue = self.timer.inner.cancelled_queue.lock().await;
        while let Some(timeout) = cancelled_queue.pop_front() {
            timeout.remove().await;
        }
    }

    async fn transfer_timeouts_to_buckets(&self) {
        let mut timeout_queue = self.timer.inner.timeout_queue.lock().await;
        for _i in 0..100000 {
            if let Some(timeout) = timeout_queue.pop_front() {
                if timeout.is_cancelled() {
                    continue;
                }

                let calculated = {
                    let mut timeout_inner = timeout.inner.write().await;
                    let calculated = timeout_inner.deadline / self.tick_duration;

                    timeout_inner.remaining_rounds = if calculated > self.tick {
                        (calculated - self.tick) / self.wheel_length
                    } else {
                        0
                    };
                    calculated
                };
                let ticks = max(calculated, self.tick);

                let stop_index = ticks & self.timer.inner.mask as u64;
                self.timer.inner.wheel[stop_index as usize].add_timeout(timeout).await;
            } else {
                break;
            }
        }
    }

    #[allow(dead_code)]
    fn unprocessed_timeouts(&self) -> &Vec<Timeout> {
        &self.unprocessed_timeouts
    }
}

#[derive(Clone)]
struct WheelBucket {
    inner: Arc<RwLock<WheelBucketInner>>,
}

struct WheelBucketInner {
    head: Option<Timeout>,
    tail: Option<Timeout>,
}

impl WheelBucket {
    fn new() -> Self {
        WheelBucket {
            inner: Arc::new(RwLock::new(WheelBucketInner {
                head: None,
                tail: None,
            })),
        }
    }

    // 只有 worker 线程调用
    async fn add_timeout(&self, timeout: Timeout) {
        let mut bucket = self.inner.write().await;
        if bucket.head.is_none() {
            {
                let mut timeout_inner = timeout.inner.write().await;
                assert!(timeout_inner.bucket.is_none());
                timeout_inner.bucket = Some(self.clone());
            }
            bucket.head = Some(timeout);
            bucket.tail = bucket.head.clone();
        } else {
            let tail = bucket.tail.take().unwrap();
            {
                let mut timeout_inner = timeout.inner.write().await;
                assert!(timeout_inner.bucket.is_none());
                timeout_inner.bucket = Some(self.clone());
                timeout_inner.prev = Some(tail.clone());
            }
            let mut tail = tail.inner.write().await;
            tail.next = Some(timeout.clone());
            bucket.tail = Some(timeout);
        }
    }


    // worker 线程调用
    async fn expire_timeouts(&self, deadline: u64) {
        let mut mut_timeout = {
            let bucket = self.inner.read().await;
            bucket.head.clone()
        };
        while let Some(timeout) = mut_timeout {
            let mut timeout_inner = timeout.inner.write().await;
            if timeout_inner.remaining_rounds <= 0 {
                {
                    let mut bucket = self.inner.write().await;
                    timeout_inner.next = Self::remove(&mut bucket, &timeout, &mut timeout_inner).await;
                }
                if timeout_inner.deadline <= deadline {
                    timeout.expire().await;
                }
            } else if timeout.is_cancelled() {
                let mut bucket = self.inner.write().await;
                timeout_inner.next = Self::remove(&mut bucket, &timeout, &mut timeout_inner).await;
            } else {
                timeout_inner.remaining_rounds -= 1;
            }
            mut_timeout = timeout_inner.next.clone();
        }
    }

    // 多线程调用
    async fn remove(bucket: &mut RwLockWriteGuard<'_, WheelBucketInner>, timeout: &Timeout, timeout_inner: &mut RwLockWriteGuard<'_, WheelTimeoutInner>) -> Option<Timeout> {
        let next = timeout_inner.next.clone();
        if timeout_inner.prev.is_some() {
            let mut prev = timeout_inner.prev.as_ref().unwrap().inner.write().await;
            prev.next = next.clone();
        }
        if timeout_inner.next.is_some() {
            let mut next = timeout_inner.next.as_ref().unwrap().inner.write().await;
            next.prev = timeout_inner.prev.clone();
        }

        if let Some(head) = &bucket.head {
            if head == timeout {
                bucket.head = None;
                bucket.tail = None;
            } else {
                bucket.head = next.clone();
            }
        } else if let Some(tail) = &bucket.tail {
            if tail == timeout {
                bucket.tail = timeout_inner.prev.clone();
            }
        }
        timeout_inner.prev = None;
        timeout_inner.next = None;
        timeout_inner.bucket = None;

        timeout.timer.inner.pending_timeouts.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);

        next
    }

    // worker 线程调用
    async fn clear_timeouts(&self, timeouts: &mut Vec<Timeout>) {
        loop {
            if let Some(timeout) = self.poll_timeout().await {
                if timeout.is_expired() || timeout.is_cancelled() {
                    continue;
                }
                timeouts.push(timeout);
            } else {
                break;
            }
        }
    }

    async fn poll_timeout(&self) -> Option<Timeout> {
        let mut bucket = self.inner.write().await;
        let head_opt = bucket.head.take();
        if head_opt.is_none() {
            return None;
        }
        let head = head_opt.clone().unwrap();
        let mut head_inner = head.inner.write().await;
        if head_inner.next.is_none() {
            bucket.tail = None;
        } else {
            bucket.head = head_inner.next.clone();
            {
                let next = head_inner.next.take().unwrap();
                let mut next = next.inner.write().await;
                next.prev = None;
            }
        }
        head_inner.prev = None;
        head_inner.bucket = None;

        head_opt
    }
}

const ST_INIT: u8 = 0;
const ST_CANCELLED: u8 = 1;
const ST_EXPIRED: u8 = 2;

#[derive(Clone)]
pub struct Timeout {
    inner: Arc<RwLock<WheelTimeoutInner>>,
    state: Arc<AtomicU8>,
    timer: WheelTimer,
}

type BoxedAsyncFn = Box<dyn Fn(Timeout) -> Pin<Box<dyn Future<Output=()> + Send>> + Send + Sync>;

struct WheelTimeoutInner {
    task: BoxedAsyncFn,
    deadline: u64,
    remaining_rounds: u64,
    next: Option<Timeout>,
    prev: Option<Timeout>,
    bucket: Option<WheelBucket>,
}

impl PartialEq for Timeout {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

impl Timeout {
    fn new(timer: WheelTimer, deadline: u64, task: BoxedAsyncFn) -> Self {
        Timeout {
            inner: Arc::new(RwLock::new(WheelTimeoutInner {
                task,
                deadline,
                remaining_rounds: 0,
                next: None,
                prev: None,
                bucket: None,
            })),
            state: Arc::new(AtomicU8::new(ST_INIT)),
            timer,
        }
    }

    async fn expire(&self) {
        match self.state.compare_exchange(ST_INIT, ST_EXPIRED, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst) {
            Ok(_) => {
                let self_clone = self.clone();
                tokio::spawn(async move {
                    let inner = self_clone.inner.read().await;
                    let f = &inner.task;
                    f(self_clone.clone()).await
                });
            }
            Err(_) => {}
        }
    }

    // worker线程调用
    async fn remove(&self) {
        let timeout_inner = self.inner.read().await;
        if timeout_inner.bucket.is_some() {
            let bucket = timeout_inner.bucket.clone().unwrap();
            drop(timeout_inner);
            let mut bucket = bucket.inner.write().await;
            let mut timeout_inner = self.inner.write().await;
            WheelBucket::remove(&mut bucket, self, &mut timeout_inner).await;
        } else {
            self.timer.inner.pending_timeouts.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
    fn timer(&self) -> WheelTimer {
        self.timer.clone()
    }

    pub fn is_expired(&self) -> bool {
        self.state.load(std::sync::atomic::Ordering::SeqCst) == ST_EXPIRED
    }

    pub fn is_cancelled(&self) -> bool {
        self.state.load(std::sync::atomic::Ordering::SeqCst) == ST_CANCELLED
    }

    pub async fn cancel(&self) -> bool {
        match self.state.compare_exchange(ST_INIT, ST_CANCELLED, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst) {
            Ok(_) => {
                let timer = self.timer();
                if let Ok(mut cancelled) = timer.inner.cancelled_queue.try_lock() {
                    cancelled.push_back(self.clone());
                }
                true
            }
            Err(_) => {
                false
            }
        }
    }
}

#[derive(Clone)]
pub struct CountDownLatch {
    notify: Arc<Notify>,
    count: Arc<AtomicUsize>,
}

impl CountDownLatch {
    pub fn new(count: usize) -> Self {
        CountDownLatch {
            notify: Arc::new(Notify::new()),
            count: Arc::new(AtomicUsize::new(count)),
        }
    }

    pub fn count_down(&self) {
        let prev = self.count.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        if prev == 1 {
            self.notify.notify_waiters();
        }
    }

    pub async fn wait(&self) {
        if self.count.load(std::sync::atomic::Ordering::SeqCst) == 0 {
            return; // already done
        }

        let notified = self.notify.notified();
        if self.count.load(std::sync::atomic::Ordering::SeqCst) == 0 {
            return; // check again to avoid race
        }
        notified.await;
    }

    pub fn remaining(&self) -> usize {
        self.count.load(std::sync::atomic::Ordering::SeqCst)
    }
}