use std::sync::Arc;
use std::time::{Duration, SystemTime};
use wheeltimer::CountDownLatch;
use rand::Rng;

// #[tokio::test]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn simple_test() {
    let mut rng = rand::thread_rng();
    let total = 100000;

    let latch = Arc::new(CountDownLatch::new(total));

    let timer = wheeltimer::WheelTimer::new(Duration::from_millis(100), 1024, 100000);
    assert!(timer.is_ok());
    let timer = timer.unwrap();
    timer.start().await;

    for i in 0..total {
        let delay = rng.gen_range(1..10);
        let latch_clone = latch.clone();
        let ms = system_time_ms();
        println!("add delay timeout:{}s", delay);

        let timeout = timer.new_timeout(Duration::from_secs(delay), async move {
            println!("timeout, expect: {}s, delay:{}ms", delay, system_time_ms() - ms);
            latch_clone.count_down();
        }).await.unwrap();

        if i % 3 == 0 && timeout.cancel().await {
            println!("timeout cancel:{}, expect: {}s, delay:{}ms", timeout.is_cancelled(), delay, system_time_ms() - ms);
            latch.count_down();
        }
    }

    latch.wait().await;
    timer.stop().await;
}

fn system_time_ms() -> u128 {
    let since_the_epoch = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
    since_the_epoch.as_millis()
}