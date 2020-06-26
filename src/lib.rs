/*
Copyright (c) 2020 Todd Stellanova
LICENSE: BSD3 (see LICENSE file)
*/

#![cfg_attr(not(test), no_std)]

use array_macro::array;
use core::marker::PhantomData;
use core::sync::atomic::{AtomicU32, Ordering};

/// The foco system should
/// - allow publishers to broadcast messages on particular topics
/// - allow subscribers to subscribe to topics
/// - allow multiple publishers on a single topic (with unique publisher instance ids )
/// - allow subscribers to filter on any publisher, or specific set of publishers
/// - allow a QoS agent to prioritize some publisher instances over others on a topic
///
/// The foco system comprises:
/// - One message queue per topic, per publisher (SPMC queue)
/// - One rust type per topic
/// - Multiple readers poll the queue for messages (subscribe)

/// We know the list of topics+types at compile time
/// We could define a message type as a compound type of the topic and "primitive" type
/// We could tune the queue size at compile time, per type
/// We know the max number of publishers at compile time (esp if nobody ever unpublishes)
/// But what if this is a library? We would not know all the publishers
/// Allow the library to be built with message/topic definition files
///
use spms_ring::{ReadToken, SpmsRing };

pub type PublisherId = u32;
pub const ANY_PUBLISHER: PublisherId = 0;
pub const MAX_PUBLISHERS_PER_TOPIC: u32 = 4;

/// Defines meta information for a topic
pub trait TopicMeta {
    /// The concrete message type published on a topic
    type MsgType;
    /// A string topic label
    const TOPIC: &'static str;
}

/// Allows a publisher to publish messages of a given type (on a given topic)
#[derive(Copy, Clone, Hash, Eq, PartialEq, PartialOrd)]
pub struct Advertisement<M>
where
    M: TopicMeta + Copy,
{
    pub advertiser_id: PublisherId,
    /// This just marks the msg type of the advertisement
    meta: PhantomData<*const M>,
}

impl<M> Advertisement<M>
where
    M: TopicMeta + Copy,
{
    fn new(instance: PublisherId) -> Self {
        Self {
            advertiser_id: instance,
            meta: PhantomData,
        }
    }
}

pub struct Subscription<M>
where
    M: TopicMeta + Copy,
{
    pub(crate) advert: Advertisement<M>,
    pub(crate) read_token: ReadToken,
}

type DefaultQueueSize = generic_array::typenum::U20;

/// One message Broker per topic-constrained type
/// Each Broker provides a map from topic to message queue
pub struct Broker<M>
where
    M: TopicMeta + Default + Copy,
{
    advertiser_count: AtomicU32,
    topic_queues:
        [SpmsRing<<M as TopicMeta>::MsgType, DefaultQueueSize>; MAX_PUBLISHERS_PER_TOPIC as usize],
}

impl<M> Broker<M>
where
    M: TopicMeta + Default + Copy,
    <M as TopicMeta>::MsgType: Default + Copy,
{
    pub fn new() -> Self {
        Self {
            advertiser_count: AtomicU32::new(0),
            topic_queues: array![SpmsRing::default(); MAX_PUBLISHERS_PER_TOPIC as usize],
        }
    }

    /// Subscribe to a topic (and specific instance if desired)
    pub fn subscribe(&mut self, instance: PublisherId) -> Option<Subscription<M>> {
        if instance != ANY_PUBLISHER && instance >= self.advertiser_count.load(Ordering::SeqCst) {
            //requested a specific advertiser that doesn't exist
            return None;
        }

        let advert = Advertisement::new(instance);

        Some(Subscription {
            advert,
            read_token: Default::default(),
        })
    }

    /// Advertise on a topic.  This provides a publisher a route to publication
    pub fn advertise(&mut self) -> Option<Advertisement<M>> {
        //optimistically try to advertise this publisher
        let publisher_id = self.advertiser_count.fetch_add(1, Ordering::SeqCst);
        if publisher_id < MAX_PUBLISHERS_PER_TOPIC {
            let advert = Advertisement::new(publisher_id);
            Some(advert)
        } else {
            self.advertiser_count.fetch_sub(1, Ordering::SeqCst);
            None
        }
    }

    /// Publish a new message on the topic
    pub fn publish(&mut self, advert: &Advertisement<M>, msg: &M::MsgType) {
        self.queue_for_advert(advert).publish(msg)
    }

    /// Has this publisher already advertised?
    /// This merely tells the caller whether we have n >= instance
    /// number of registered (advertised) publishers.
    pub fn pub_registered(&self, instance: PublisherId) -> bool {
        instance < self.advertiser_count.load(Ordering::Relaxed)
    }

    /// How many publishers have advertised on this topic?
    pub fn group_count(&self) -> u32 {
        self.advertiser_count.load(Ordering::Relaxed)
    }

    /// Read the next message from the topic
    pub fn poll(&mut self, sub: &mut Subscription<M>) -> nb::Result<M::MsgType, ()> {
        self.queue_for_advert(&sub.advert)
            .read_next(&mut sub.read_token)
    }

    /// Obtain the queue for a particular advertisement (a specific publisher)
    fn queue_for_advert(
        &mut self,
        advert: &Advertisement<M>,
    ) -> &mut SpmsRing<M::MsgType, DefaultQueueSize> {
        //advert.instance
        let qi = advert.advertiser_id as usize;
        &mut self.topic_queues[qi]
    }
}

#[cfg(test)]
mod tests {
    use super::{Broker, TopicMeta, ANY_PUBLISHER};

    #[derive(Default, Copy, Clone)]
    struct Point {
        x: u32,
        y: u32,
    }

    #[derive(Default, Copy, Clone)]
    struct RoverLocation {}
    impl TopicMeta for RoverLocation {
        type MsgType = Point;
        const TOPIC: &'static str = "rover";
    }

    #[derive(Default, Copy, Clone)]
    struct HomeLocation {}
    impl TopicMeta for HomeLocation {
        type MsgType = Point;
        const TOPIC: &'static str = "home";
    }

    #[test]
    fn setup_pubsub() {
        let mut broker: Broker<HomeLocation> = Broker::new();
        let adv = broker.advertise().unwrap();
        assert!(broker.pub_registered(0));
        let mut sb = broker.subscribe(ANY_PUBLISHER).unwrap();

        for i in 0..5 {
            let msg = Point { x: i, y: i };
            broker.publish(&adv, &msg);
            let next_msg_r = broker.poll(&mut sb);
            assert!(next_msg_r.is_ok());

            let next_msg = next_msg_r.unwrap();
            //println!("next_msg: {:?}", next_msg);
            assert_eq!(next_msg.x, i);
            assert_eq!(next_msg.y, i);
        }
    }

    #[test]
    fn two_queues_same_inner_type_diff_topics() {
        let mut broker1: Broker<HomeLocation> = Broker::new();
        let adv1 = broker1.advertise().unwrap();
        let mut broker2: Broker<RoverLocation> = Broker::new();
        let mut sb2 = broker2.subscribe(ANY_PUBLISHER).unwrap();

        for i in 0..5 {
            let msg = Point { x: i, y: i };
            broker1.publish(&adv1, &msg);
            // we publish on queue1, no data on queue2 yet
            let next_msg_r = broker2.poll(&mut sb2);
            assert!(next_msg_r.is_err());
        }
    }

    #[test]
    fn two_queues_same_topic() {
        let mut broker: Broker<HomeLocation> = Broker::new();
        let adv1 = broker.advertise().unwrap();
        let adv2 = broker.advertise().unwrap();
        assert!(broker.pub_registered(0));
        assert!(broker.pub_registered(1));

        //TODO s/b Some/None Advertiser?
        let mut sb2 = broker.subscribe(adv2.advertiser_id).unwrap();

        for i in 0..5 {
            let msg = Point { x: i, y: i };
            broker.publish(&adv1, &msg);
            // we publish on queue1, no data on queue2 yet
            let next_msg_r = broker.poll(&mut sb2);
            assert!(next_msg_r.is_err());
            broker.publish(&adv2, &msg);
            let next_msg_r = broker.poll(&mut sb2);
            assert!(next_msg_r.is_ok());
        }
    }
}
