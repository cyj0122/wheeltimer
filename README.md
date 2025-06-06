# wheeltimer

A high-performance asynchronous time wheel-based timer implementation in Rust, inspired by Netty's `HashedWheelTimer`.

This crate provides a scalable and thread-safe way to schedule delayed tasks using the **time wheel algorithm**, ideal for use cases such as connection timeouts, heartbeat checks, or any system requiring large numbers of scheduled events.

## ✨ Features

- ⏱️ Asynchronous task scheduling with configurable tick duration
- 🧮 Efficient time wheel structure (power-of-two sized buckets)
- 🧵 Thread-safe design using `Arc`, `RwLock`, and `Mutex`
- 🚫 Support for cancelling pending timeouts
- 📈 Configurable maximum number of pending timeouts
- 📦 Easy-to-use API with support for async closures

## 📦 Usage

Add this to your Cargo.toml

```toml
[dependencies]
wheeltimer = "0.2.0"
```


### 🔧 Example

```rust
use wheeltimer::WheelTimer;
use std::time::Duration;

#[tokio::main]
async fn main() {
    // Create a new timer with 10ms tick duration and 1024 buckets
    let timer = WheelTimer::new(Duration::from_millis(10), 1024, 100000).unwrap();

    // Start the timer
    timer.start().await;

    // Schedule a timeout after 2 seconds
    let timeout = timer.new_timeout(Duration::from_secs(2), 
        async move {
            println!("A delayed task is executed!");
        }
    ).await.unwrap();

    // Wait for the task to execute
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Stop the timer
    timer.stop().await;
}
```


## 🛠️ API Overview

- `WheelTimer::new(tick_duration, ticks_per_wheel, max_pending_timeouts)` – Creates a new timer.
- `.start()` – Starts the internal worker thread.
- `.new_timeout(delay, task)` – Schedules a task to run after a delay.
- `.stop()` – Stops the timer and cancels all pending timeouts.
- `.cancel()` on `Timeout` – Cancels an individual timeout before it fires.

## 📁 Documentation

You can view the full documentation online:

[📚 https://docs.rs/wheeltimer](https://docs.rs/wheeltimer)

Or build it locally:

```bash
cargo doc --open
```


## 🧪 Testing

Run tests:

```bash
cargo test
```


## 🤝 Contributing

Contributions are welcome! Feel free to open issues or submit pull requests on [GitHub](https://github.com/yourname/wheeltimer).

## 📄 License

Licensed under either of:
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))