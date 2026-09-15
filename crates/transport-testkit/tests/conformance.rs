//! Exercise conformance helpers independently of a public backend.

use fungi_transport::{
    BuildError, Channel, ChannelBuilder, RecvError, RecvHalf, SendError, SendHalf,
};
use fungi_transport_testkit::testkit;
use tokio::sync::mpsc;

#[derive(Debug)]
struct Sender {
    queue: mpsc::Sender<Vec<u8>>,
    max: usize,
}

#[derive(Debug)]
struct Receiver(mpsc::Receiver<Vec<u8>>);

#[derive(Debug)]
struct Fixture {
    sender: Sender,
    receiver: Receiver,
}

fn pair(max: usize) -> (Fixture, Fixture) {
    let (left, incoming_right) = mpsc::channel(1);
    let (right, incoming_left) = mpsc::channel(1);
    (
        Fixture {
            sender: Sender { queue: left, max },
            receiver: Receiver(incoming_left),
        },
        Fixture {
            sender: Sender { queue: right, max },
            receiver: Receiver(incoming_right),
        },
    )
}

impl SendHalf for &mut Sender {
    type SendError = SendError;

    async fn send(&mut self, message: Vec<u8>) -> Result<(), SendError> {
        if message.len() > self.max {
            return Err(SendError::TooLarge { max: self.max });
        }
        self.queue
            .send(message)
            .await
            .map_err(|_| SendError::Closed)
    }
}

impl RecvHalf for &mut Receiver {
    type RecvError = RecvError;

    async fn recv(&mut self) -> Result<Vec<u8>, RecvError> {
        self.0.recv().await.ok_or(RecvError::Closed)
    }
}

impl SendHalf for Fixture {
    type SendError = SendError;

    async fn send(&mut self, message: Vec<u8>) -> Result<(), SendError> {
        (&mut self.sender).send(message).await
    }
}

impl RecvHalf for Fixture {
    type RecvError = RecvError;

    async fn recv(&mut self) -> Result<Vec<u8>, RecvError> {
        (&mut self.receiver).recv().await
    }
}

impl Channel for Fixture {
    type SendHalf<'a> = &'a mut Sender;
    type RecvHalf<'a> = &'a mut Receiver;

    fn split(&mut self) -> (Self::SendHalf<'_>, Self::RecvHalf<'_>) {
        (&mut self.sender, &mut self.receiver)
    }
}

struct Connector(mpsc::Sender<Fixture>);
struct Listener(mpsc::Receiver<Fixture>);

impl ChannelBuilder for Connector {
    type Input = ();
    type Channel = Fixture;

    async fn build(&mut self, _: &()) -> Result<Fixture, BuildError> {
        let (client, server) = pair(1024);
        self.0
            .send(server)
            .await
            .map_err(|_| BuildError::Unreachable)?;
        Ok(client)
    }
}

impl ChannelBuilder for Listener {
    type Input = ();
    type Channel = Fixture;

    async fn build(&mut self, _: &()) -> Result<Fixture, BuildError> {
        self.0.recv().await.ok_or(BuildError::Unreachable)
    }
}

#[tokio::test(start_paused = true)]
async fn helpers_check_delivery_limits_cancellation_and_reconnection() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let (left, right) = pair(1024);
        testkit::roundtrip_both_directions(left, right).await;

        let (left, right) = pair(1024);
        testkit::closed_after_peer_drop(left, right).await;

        let (left, right) = pair(1024);
        testkit::recv_is_cancel_safe(left, right).await;

        let (left, _) = pair(8);
        testkit::too_large(left, 8).await;

        let (left, right) = pair(8);
        testkit::too_large_is_recoverable(left, right, 8).await;

        let (left, right) = pair(1024);
        testkit::mutual_bursts_converge(left, right, 16).await;

        let (sender, receiver) = mpsc::channel(1);
        testkit::build_use_drop_rebuild(Connector(sender), Listener(receiver), &()).await;
    })
    .await
    .expect("conformance helpers must complete");
}
