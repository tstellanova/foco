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
struct HotTopicType {}

impl TopicMeta for HotTopicType {
    type MsgType = TemperaturePoint;
    const TOPIC: &'static str = "hotwhere";
}

fn main() -> ! {
    lazy_static! {
        /// this is how we share a broker between multiple threads
        static ref BROKER: AtomicPtr<Broker<HotTopicType>> = AtomicPtr::default();
    };
    let mut broker: Broker<HotTopicType> = Broker::new();

    BROKER.store(&mut broker, Ordering::Relaxed);

    let pub_thread = thread::spawn(move || {
        let advert = unsafe {
            BROKER
                .load(Ordering::SeqCst)
                .as_mut()
                .unwrap()
                .advertise()
                .unwrap()
        };

        let mut pub_count: usize = 0;
        loop {
            let msg = TemperaturePoint {
                x: pub_count as u32,
                y: pub_count as u32,
                temperature: 21.0,
            };
            pub_count += 1;
            //safe because only this thread ever uses a mutable SpmsRing,
            //and Q_PTR never changes
            unsafe {
                BROKER
                    .load(Ordering::SeqCst)
                    .as_mut()
                    .unwrap()
                    .publish(&advert, &msg);
            }

            thread::sleep(time::Duration::from_micros(1));
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

        loop {
            let msg_res = unsafe {
                BROKER
                    .load(Ordering::SeqCst)
                    .as_ref()
                    .unwrap()
                    .poll(&mut subscription)
            };
            if let Ok(msg) = msg_res {
                println!("msg: {:?}", msg);
            }
        }
    });

    pub_thread.join().expect("pub_thread panicked");
    sub_thread.join().expect("sub_thread panicked");

    loop {}
}
