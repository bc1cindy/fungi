//! Reusable conformance checks for byte channel implementations.
//!
//! Each function accepts fresh channels supplied by an implementation's own
//! test suite. No check assumes FIFO delivery because [`Channel`] does not
//! promise ordering.

use std::time::Duration;

use fungi_transport::{Channel, ChannelBuilder, RecvError, RecvHalf, SendError, SendHalf};

/// Verify intact delivery in both directions without assuming ordering.
pub async fn roundtrip_both_directions<
    C: Channel<Vec<u8>, SendError = SendError, RecvError = RecvError>,
>(
    mut left: C,
    mut right: C,
) {
    left.send(b"first".to_vec()).await.unwrap();
    let first = right.recv().await.unwrap();

    left.send(b"second".to_vec()).await.unwrap();
    let second = right.recv().await.unwrap();

    assert_eq!(first, b"first");
    assert_eq!(second, b"second");

    right.send(b"reply".to_vec()).await.unwrap();
    assert_eq!(left.recv().await.unwrap(), b"reply");
}

/// Verify that dropping one endpoint closes the other endpoint's receiver.
///
/// Use only for backends that detect peer closure.
pub async fn closed_after_peer_drop<
    C: Channel<Vec<u8>, SendError = SendError, RecvError = RecvError>,
>(
    peer: C,
    mut channel: C,
) {
    drop(peer);
    assert!(matches!(channel.recv().await, Err(RecvError::Closed)));
}

/// Verify that repeatedly abandoning a receive does not consume a message.
pub async fn recv_is_cancel_safe<
    C: Channel<Vec<u8>, SendError = SendError, RecvError = RecvError>,
>(
    mut sender: C,
    mut receiver: C,
) {
    for _ in 0..10 {
        let attempt = tokio::time::timeout(Duration::from_millis(5), receiver.recv()).await;
        assert!(attempt.is_err());
    }

    sender.send(b"message".to_vec()).await.unwrap();
    assert_eq!(receiver.recv().await.unwrap(), b"message");
}

/// Verify that a payload beyond the declared limit is rejected exactly.
pub async fn too_large<C: Channel<Vec<u8>, SendError = SendError, RecvError = RecvError>>(
    mut channel: C,
    max: usize,
) {
    let message = vec![0; max + 1];
    assert!(matches!(
        channel.send(message).await,
        Err(SendError::TooLarge { max: actual }) if actual == max
    ));
}

/// Verify that an oversized rejection leaves the channel usable.
pub async fn too_large_is_recoverable<
    C: Channel<Vec<u8>, SendError = SendError, RecvError = RecvError>,
>(
    mut sender: C,
    mut receiver: C,
    max: usize,
) {
    let message = vec![0; max + 1];
    assert!(matches!(
        sender.send(message).await,
        Err(SendError::TooLarge { max: actual }) if actual == max
    ));

    let recovery_message = vec![0x42; max.min(5)];
    sender.send(recovery_message.clone()).await.unwrap();
    assert_eq!(receiver.recv().await.unwrap(), recovery_message);
}

/// Verify connection establishment, peer loss, and reconnection.
///
/// Use only for connection-backed channels that detect peer loss.
pub async fn build_use_drop_rebuild<C, L>(mut connector: C, mut listener: L, address: &C::Input)
where
    C: ChannelBuilder,
    C::Channel: Channel<Vec<u8>, SendError = SendError, RecvError = RecvError>,
    L: ChannelBuilder<Input = (), Channel = C::Channel>,
{
    let (client, server) =
        futures_util::future::join(connector.build(address), listener.build(&())).await;
    let (mut client, mut server) = (client.expect("connect"), server.expect("accept"));

    client.send(b"message".to_vec()).await.unwrap();
    assert_eq!(server.recv().await.unwrap(), b"message");

    drop(server);
    let detected = tokio::time::timeout(Duration::from_secs(5), client.recv())
        .await
        .expect("a dropped peer must not leave recv pending forever");
    assert!(detected.is_err());

    let (client, server) =
        futures_util::future::join(connector.build(address), listener.build(&())).await;
    let (mut client, mut server) = (client.expect("reconnect"), server.expect("reaccept"));
    client.send(b"again".to_vec()).await.unwrap();
    assert_eq!(server.recv().await.unwrap(), b"again");
}

/// Verify progress when both peers send bursts through bounded links.
pub async fn mutual_bursts_converge<
    C: Channel<Vec<u8>, SendError = SendError, RecvError = RecvError>,
>(
    mut left: C,
    mut right: C,
    burst: usize,
) {
    async fn drive<C: Channel<Vec<u8>, SendError = SendError, RecvError = RecvError>>(
        channel: &mut C,
        tag: u8,
        burst: usize,
    ) {
        let (mut sender, mut receiver) = channel.split();
        let sending = async move {
            for index in 0..burst {
                sender.send([tag, index as u8].to_vec()).await.unwrap();
            }
        };
        let receiving = async move {
            for _ in 0..burst {
                receiver.recv().await.unwrap();
            }
        };
        futures_util::future::join(sending, receiving).await;
    }

    let exchange = futures_util::future::join(
        drive(&mut left, b'l', burst),
        drive(&mut right, b'r', burst),
    );
    tokio::time::timeout(Duration::from_secs(5), exchange)
        .await
        .expect("mutual bursts must make progress");
}
