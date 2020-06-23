/*
Copyright (c) 2020 Todd Stellanova
LICENSE: BSD3 (see LICENSE file)
*/

#![cfg_attr(not(test), no_std)]

use array_macro::array;
use core::marker::PhantomData;


/// The foco system should
/// - allow publishers to broadcast messages on particular topics
/// - allow subscribers to subscribe to topics
/// - allow multiple publishers on a single topic (with unique publisher instance ids )
/// - allow subscribers to filter on any publisher, or specific set of publishers
/// - allow a QoS agent to prioritize some publisher instances over others on a topic
///
/// The gossip system comprises:
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
use spmc_ring::{ReadToken, SpmcQueue};

pub type PublisherId = u32;
const ANY_PUBLISHER: PublisherId = 0;
type DefaultQueueSize = generic_array::typenum::U20;

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

const MAX_QUEUES_PER_TOPIC: usize = 8;

/// One Registrar per type:
/// Each Registrar provides a map from topic to queue instance
pub struct Registrar<M>
where
    M: TopicMeta + Default + Copy,
{
    advertiser_count: usize,
    topic_queues: [SpmcQueue<<M as TopicMeta>::MsgType, DefaultQueueSize>; MAX_QUEUES_PER_TOPIC],
}

impl<M> Registrar<M>
where
    M: TopicMeta + Default + Copy,
    <M as TopicMeta>::MsgType: Default + Copy,
{
    fn new() -> Self {
        Self {
            advertiser_count: 0,
            topic_queues: array![SpmcQueue::default(); MAX_QUEUES_PER_TOPIC],
        }
    }

    /// Subscribe to a topic (and specific instance if desired)
    ///
    pub fn subscribe(&mut self, instance: PublisherId) -> Option<Subscription<M>> {
        if instance != ANY_PUBLISHER && instance >= self.advertiser_count as u32 {
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
    pub fn advertise(&mut self) -> Advertisement<M> {
        let advert = Advertisement::new(self.advertiser_count as u32);
        //TODO check for advertiser overflow
        self.advertiser_count += 1;
        advert
    }

    /// Publish a new message on the topic
    pub fn publish(&mut self, advert: &Advertisement<M>, msg: &M::MsgType) {
        self.queue_for_advert(advert).publish(msg)
    }

    /// Read the next message from the topic
    pub fn poll(&mut self, sub: &mut Subscription<M>) -> nb::Result<M::MsgType, ()> {
        self.queue_for_advert(&sub.advert)
            .read_next(&mut sub.read_token)
    }

    fn queue_for_advert(
        &mut self,
        advert: &Advertisement<M>,
    ) -> &mut SpmcQueue<M::MsgType, DefaultQueueSize> {
        //advert.instance
        let qi = advert.advertiser_id as usize;
        &mut self.topic_queues[qi]
    }
}

#[cfg(test)]
mod tests {
    use super::{TopicMeta, Registrar, ANY_PUBLISHER};

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
        let mut reggo: Registrar<HomeLocation> = Registrar::new();
        let adv = reggo.advertise();
        let mut sb = reggo.subscribe(ANY_PUBLISHER).unwrap();

        for i in 0..5 {
            let msg = Point { x: i, y: i };
            reggo.publish(&adv, &msg);
            let next_msg_r = reggo.poll(&mut sb);
            assert!(next_msg_r.is_ok());

            let next_msg = next_msg_r.unwrap();
            //println!("next_msg: {:?}", next_msg);
            assert_eq!(next_msg.x, i);
            assert_eq!(next_msg.y, i);
        }
    }

    #[test]
    fn two_queues_same_inner_type_diff_topics() {
        let mut reg1: Registrar<HomeLocation> = Registrar::new();
        let adv1 = reg1.advertise();
        //let mut sb1 = reg1.subscribe(ANY_PUBLISHER).unwrap();

        let mut reg2: Registrar<RoverLocation> = Registrar::new();
        //let adv2 = reg2.advertise();
        let mut sb2 = reg2.subscribe(ANY_PUBLISHER).unwrap();

        for i in 0..5 {
            let msg = Point { x: i, y: i };
            reg1.publish(&adv1, &msg);
            // we publish on queue1, no data on queue2 yet
            let next_msg_r = reg2.poll(&mut sb2);
            assert!(next_msg_r.is_err());
        }
    }

    #[test]
    fn two_queues_same_topic() {
        let mut reg: Registrar<HomeLocation> = Registrar::new();
        let adv1 = reg.advertise();
        let mut sb1 = reg.subscribe(ANY_PUBLISHER).unwrap();

        let adv2 = reg.advertise();
        let mut sb2 = reg.subscribe(adv2.advertiser_id).unwrap();

        for i in 0..5 {
            let msg = Point { x: i, y: i };
            reg.publish(&adv1, &msg);
            // we publish on queue1, no data on queue2 yet
            let next_msg_r = reg.poll(&mut sb2);
            assert!(next_msg_r.is_err());
            reg.publish(&adv2, &msg);
            let next_msg_r = reg.poll(&mut sb2);
            assert!(next_msg_r.is_ok());
        }
    }
}
