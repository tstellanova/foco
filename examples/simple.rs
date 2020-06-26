use core::sync::atomic::{AtomicPtr, Ordering};
use core::time;
use foco::{Broker, TopicMeta, ANY_PUBLISHER};
use lazy_static::lazy_static;
use std::thread;

#[derive(Default, Copy, Clone, Debug)]
struct TemperaturePoint {
    x: u32,
    y: u32,
    temperature: f32,
}

#[derive(Default, Copy, Clone)]
struct HotTopic {}

impl TopicMeta for HotTopic {
    type MsgType = TemperaturePoint;
    const TOPIC: &'static str = "hot_topic";
}

fn main()  {
    lazy_static! {
        /// this is how we share a broker between multiple threads
        static ref BROKER: AtomicPtr<Broker<HotTopic>> = AtomicPtr::default();
    };
    let mut broker: Broker<HotTopic> = Broker::new();
    BROKER.store(&mut broker, Ordering::Relaxed);

    println!("setup publication for `{}`", HotTopic::TOPIC);

    let pub_thread = thread::spawn(move || {
        let advert = unsafe {
            BROKER
                .load(Ordering::SeqCst)
                .as_mut()
                .unwrap()
                .advertise()
                .unwrap()
        };

        for pub_count in 0..1000 {
            let msg = TemperaturePoint {
                x: pub_count as u32,
                y: pub_count as u32,
                temperature: 21.0,
            };

            unsafe {
                BROKER
                    .load(Ordering::SeqCst)
                    .as_mut()
                    .unwrap()
                    .publish(&advert, &msg);
            }

            thread::sleep(time::Duration::from_millis(1));
        }
    });

    let sub_thread = thread::spawn(move || {
        let mut subscription = unsafe {
            BROKER
                .load(Ordering::SeqCst)
                .as_mut()
                .unwrap()
                .subscribe(ANY_PUBLISHER)
                .unwrap()
        };

        let mut read_count: u32 = 0;
        while read_count < 1000 {
            let msg_res = unsafe {
                BROKER
                    .load(Ordering::SeqCst)
                    .as_ref()
                    .unwrap()
                    .poll(&mut subscription)
            };
            if let Ok(msg) = msg_res {
                read_count += 1;
                println!("msg: {:?}", msg);
            }
        }
    });

    pub_thread.join().expect("pub_thread panicked");
    sub_thread.join().expect("sub_thread panicked");


}
