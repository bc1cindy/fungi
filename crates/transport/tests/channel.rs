//! Consumer workflows exercised against the in-memory transport.

use std::collections::BTreeSet;
use std::time::Duration;

use fungi_transport::{Channel, ChannelBuilder, RecvError, RecvHalf, SendHalf, into_stream};
use fungi_transport_testkit::mem::{MemAddr, MemChannel, MemConfig, duplex, network};
use futures_util::StreamExt;

#[derive(Debug)]
struct AddressOnlyBuilder;

impl ChannelBuilder for AddressOnlyBuilder {
    type Input = MemAddr;
    type Channel = MemChannel;

    fn build(
        &mut self,
        _address: &Self::Input,
    ) -> impl std::future::Future<Output = Result<Self::Channel, fungi_transport::BuildError>> + Send
    {
        let (channel, peer) = duplex(MemConfig::default());
        drop(peer);
        std::future::ready(Ok(channel))
    }
}

#[tokio::test]
async fn construction_does_not_require_an_online_peer() {
    let mut channel = AddressOnlyBuilder.build(&MemAddr).await.unwrap();
    assert!(channel.send(b"message".to_vec()).await.is_err());
}

#[tokio::test(start_paused = true)]
async fn sequential_ping_pong() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (mut left, mut right) = duplex(MemConfig::default());

        for value in 0..3 {
            left.send([value].to_vec()).await.unwrap();
            assert_eq!(right.recv().await.unwrap(), [value]);

            right.send([value, value].to_vec()).await.unwrap();
            assert_eq!(left.recv().await.unwrap(), [value, value]);
        }
    })
    .await
    .expect("in-memory workflow must complete before the deadline");
}

#[tokio::test(start_paused = true)]
async fn concurrent_full_duplex_under_backpressure() {
    tokio::time::timeout(Duration::from_secs(5), async {
        async fn exchange(channel: &mut MemChannel, tag: u8) -> Vec<Vec<u8>> {
            let (mut sender, mut receiver) = channel.split();
            let sending = async move {
                for value in 0..32 {
                    sender.send([tag, value].to_vec()).await.unwrap();
                }
            };
            let receiving = async move {
                let mut messages = Vec::with_capacity(32);
                for _ in 0..32 {
                    messages.push(receiver.recv().await.unwrap());
                }
                messages
            };
            let ((), messages) = futures_util::future::join(sending, receiving).await;
            messages
        }

        let config = MemConfig {
            capacity: Some(1),
            ..MemConfig::default()
        };
        let (mut left, mut right) = duplex(config);
        let exchanges =
            futures_util::future::join(exchange(&mut left, b'l'), exchange(&mut right, b'r'));
        let (from_right, from_left) = tokio::time::timeout(Duration::from_secs(5), exchanges)
            .await
            .expect("both peers must keep draining while they send");

        assert!(from_right.iter().all(|message| message[0] == b'r'));
        assert!(from_left.iter().all(|message| message[0] == b'l'));
    })
    .await
    .expect("in-memory workflow must complete before the deadline");
}

#[tokio::test(start_paused = true)]
async fn multiplex_channels_as_streams() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut pairs = (0..4)
            .map(|_| duplex(MemConfig::default()))
            .collect::<Vec<_>>();
        for (index, (sender, _receiver)) in pairs.iter_mut().enumerate() {
            sender.send([index as u8].to_vec()).await.unwrap();
        }

        let streams = pairs
            .into_iter()
            .map(|(_sender, receiver)| Box::pin(into_stream(receiver)))
            .collect::<Vec<_>>();
        let mut merged = futures_util::stream::select_all(streams);
        let mut received = BTreeSet::new();
        while received.len() < 4 {
            match merged.next().await {
                Some(Ok(message)) => {
                    assert!(
                        received.insert(message[0]),
                        "a channel yielded a duplicate tag"
                    );
                }
                Some(Err(RecvError::Closed)) => {}
                Some(Err(error)) => panic!("unexpected receive failure: {error}"),
                None => panic!("streams ended before every message arrived"),
            }
        }

        assert_eq!(received, BTreeSet::from([0, 1, 2, 3]));
    })
    .await
    .expect("in-memory workflow must complete before the deadline");
}

#[tokio::test(start_paused = true)]
async fn reconnect_after_peer_loss() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (mut connector, mut listener) = network(MemConfig::default());
        let (client, server) = tokio::join!(connector.build(&MemAddr), listener.build(&()));
        let (mut client, server) = (client.unwrap(), server.unwrap());
        drop(server);
        assert!(client.send(b"message".to_vec()).await.is_err());

        let (client, server) = tokio::join!(connector.build(&MemAddr), listener.build(&()));
        let (mut client, mut server) = (client.unwrap(), server.unwrap());
        client.send(b"again".to_vec()).await.unwrap();
        assert_eq!(server.recv().await.unwrap(), b"again");
    })
    .await
    .expect("in-memory workflow must complete before the deadline");
}

#[tokio::test(start_paused = true)]
async fn cancelled_receive_does_not_lose_the_message() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (mut sender, mut receiver) = duplex(MemConfig {
            latency: Some(Duration::from_millis(10)),
            ..MemConfig::default()
        });
        let mut interval = tokio::time::interval(Duration::from_millis(1));
        let mut ticks = 0;
        let message = loop {
            tokio::select! {
                _ = interval.tick() => {
                    ticks += 1;
                    if ticks == 3 {
                        sender.send(b"late".to_vec()).await.unwrap();
                    }
                }
                message = receiver.recv() => break message.unwrap(),
            }
        };

        assert_eq!(message, b"late");
    })
    .await
    .expect("in-memory workflow must complete before the deadline");
}
