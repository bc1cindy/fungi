//! In-memory channels for deterministic transport and consumer tests.
//!
//! A duplex channel uses two crossed bounded queues. Capacity defaults to one
//! so tests encounter backpressure without additional configuration.

use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::mpsc;

use fungi_transport::{
    BuildError, Channel, ChannelBuilder, RecvError, RecvHalf, SendError, SendHalf,
};

/// Delivery behavior simulated by an in-memory sender.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Delivery {
    /// Wait until the message enters the peer's queue.
    #[default]
    Confirmed,
    /// Return after a non-blocking delivery attempt; loss is silent.
    BestEffort,
}

/// Configuration shared by both ends of an in-memory channel.
#[derive(Debug, Clone, Default)]
pub struct MemConfig {
    /// Queue capacity per direction. Missing or zero capacity becomes one.
    pub capacity: Option<usize>,
    /// Maximum message length in bytes.
    pub max_message_len: Option<usize>,
    /// Delay applied before each delivery attempt.
    pub latency: Option<Duration>,
    /// Internal deadline for confirmed delivery into the peer's queue.
    pub send_timeout: Option<Duration>,
    /// Delivery behavior for this channel.
    pub delivery: Delivery,
}

/// One endpoint of an in-memory duplex channel.
#[derive(Debug)]
pub struct MemChannel {
    sender: mpsc::Sender<Vec<u8>>,
    receiver: mpsc::Receiver<Vec<u8>>,
    config: MemConfig,
    drop_next: AtomicUsize,
    fail_next: AtomicUsize,
}

impl MemChannel {
    /// Silently discard the next `count` best-effort delivery attempts.
    ///
    /// Confirmed delivery ignores this setting.
    pub fn drop_next(&self, count: usize) {
        self.drop_next.store(count, Ordering::Relaxed);
    }

    /// Fail the next `count` delivery attempts with a transport error.
    pub fn fail_next(&self, count: usize) {
        self.fail_next.store(count, Ordering::Relaxed);
    }
}

/// Create two connected in-memory channel endpoints.
pub fn duplex(config: MemConfig) -> (MemChannel, MemChannel) {
    let capacity = config.capacity.unwrap_or(1).max(1);
    let (left_sender, right_receiver) = mpsc::channel(capacity);
    let (right_sender, left_receiver) = mpsc::channel(capacity);
    let endpoint = |sender, receiver| MemChannel {
        sender,
        receiver,
        config: config.clone(),
        drop_next: AtomicUsize::new(0),
        fail_next: AtomicUsize::new(0),
    };
    (
        endpoint(left_sender, left_receiver),
        endpoint(right_sender, right_receiver),
    )
}

fn take_one(counter: &AtomicUsize) -> bool {
    counter
        .try_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
            count.checked_sub(1)
        })
        .is_ok()
}

fn prepare(config: &MemConfig, message: Vec<u8>) -> Result<Vec<u8>, usize> {
    match config.max_message_len {
        Some(max) if message.len() > max => Err(max),
        _ => Ok(message),
    }
}

async fn send_message(
    sender: &mpsc::Sender<Vec<u8>>,
    config: &MemConfig,
    drop_next: &AtomicUsize,
    fail_next: &AtomicUsize,
    message: Result<Vec<u8>, usize>,
) -> Result<(), SendError> {
    let message = message.map_err(|max| SendError::TooLarge { max })?;

    if take_one(fail_next) {
        return Err(SendError::Transport("injected failure".into()));
    }
    if let Some(latency) = config.latency {
        tokio::time::sleep(latency).await;
    }
    match config.delivery {
        Delivery::Confirmed => match config.send_timeout {
            Some(timeout) => tokio::time::timeout(timeout, sender.send(message))
                .await
                .map_err(|_| SendError::Transport("send timed out".into()))?
                .map_err(|_| SendError::Closed),
            None => sender.send(message).await.map_err(|_| SendError::Closed),
        },
        Delivery::BestEffort => {
            if sender.is_closed() {
                return Err(SendError::Closed);
            }
            if take_one(drop_next) {
                return Ok(());
            }
            try_send_best_effort(sender, message)
        }
    }
}

fn try_send_best_effort(sender: &mpsc::Sender<Vec<u8>>, message: Vec<u8>) -> Result<(), SendError> {
    match sender.try_send(message) {
        Ok(()) | Err(mpsc::error::TrySendError::Full(_)) => Ok(()),
        Err(mpsc::error::TrySendError::Closed(_)) => Err(SendError::Closed),
    }
}

/// Borrowed sending direction of a [`MemChannel`].
#[derive(Debug)]
pub struct MemSendHalf<'a> {
    sender: &'a mpsc::Sender<Vec<u8>>,
    config: &'a MemConfig,
    drop_next: &'a AtomicUsize,
    fail_next: &'a AtomicUsize,
}

/// Borrowed receiving direction of a [`MemChannel`].
#[derive(Debug)]
pub struct MemRecvHalf<'a> {
    receiver: &'a mut mpsc::Receiver<Vec<u8>>,
}

impl SendHalf for MemSendHalf<'_> {
    type SendError = SendError;
    fn send(&mut self, message: Vec<u8>) -> impl Future<Output = Result<(), SendError>> + Send {
        let message = prepare(self.config, message);
        send_message(
            self.sender,
            self.config,
            self.drop_next,
            self.fail_next,
            message,
        )
    }
}

impl RecvHalf for MemRecvHalf<'_> {
    type RecvError = RecvError;
    async fn recv(&mut self) -> Result<Vec<u8>, RecvError> {
        self.receiver.recv().await.ok_or(RecvError::Closed)
    }
}

impl SendHalf for MemChannel {
    type SendError = SendError;
    fn send(&mut self, message: Vec<u8>) -> impl Future<Output = Result<(), SendError>> + Send {
        let message = prepare(&self.config, message);
        send_message(
            &self.sender,
            &self.config,
            &self.drop_next,
            &self.fail_next,
            message,
        )
    }
}

impl RecvHalf for MemChannel {
    type RecvError = RecvError;
    async fn recv(&mut self) -> Result<Vec<u8>, RecvError> {
        self.receiver.recv().await.ok_or(RecvError::Closed)
    }
}

impl Channel for MemChannel {
    type SendHalf<'a> = MemSendHalf<'a>;
    type RecvHalf<'a> = MemRecvHalf<'a>;

    fn split(&mut self) -> (Self::SendHalf<'_>, Self::RecvHalf<'_>) {
        (
            MemSendHalf {
                sender: &self.sender,
                config: &self.config,
                drop_next: &self.drop_next,
                fail_next: &self.fail_next,
            },
            MemRecvHalf {
                receiver: &mut self.receiver,
            },
        )
    }
}

/// Address of the sole endpoint exposed by an in-memory network.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct MemAddr;

/// Channel builder side of an in-memory network.
#[derive(Debug, Clone)]
pub struct MemChannelBuilder {
    config: MemConfig,
    incoming: mpsc::Sender<MemChannel>,
}

/// Listener side of an in-memory network.
#[derive(Debug)]
pub struct MemListener {
    incoming: mpsc::Receiver<MemChannel>,
}

/// Create a paired channel builder and listener.
pub fn network(config: MemConfig) -> (MemChannelBuilder, MemListener) {
    let (sender, receiver) = mpsc::channel(8);
    (
        MemChannelBuilder {
            config,
            incoming: sender,
        },
        MemListener { incoming: receiver },
    )
}

impl ChannelBuilder for MemChannelBuilder {
    type Input = MemAddr;
    type Channel = MemChannel;

    fn build(
        &mut self,
        _address: &Self::Input,
    ) -> impl Future<Output = Result<Self::Channel, BuildError>> + Send {
        let (local, remote) = duplex(self.config.clone());
        let incoming = self.incoming.clone();
        async move {
            incoming
                .send(remote)
                .await
                .map_err(|_| BuildError::Unreachable)?;
            Ok(local)
        }
    }
}

impl ChannelBuilder for MemListener {
    type Input = ();
    type Channel = MemChannel;

    async fn build(&mut self, _input: &()) -> Result<Self::Channel, BuildError> {
        self.incoming.recv().await.ok_or(BuildError::Unreachable)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit;

    #[test]
    fn best_effort_attempt_reports_a_closed_queue() {
        let (sender, receiver) = mpsc::channel(1);
        drop(receiver);
        assert!(matches!(
            try_send_best_effort(&sender, vec![1]),
            Err(SendError::Closed)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn passes_roundtrip_conformance() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (left, right) = duplex(MemConfig::default());
            testkit::roundtrip_both_directions(left, right).await;
        })
        .await
        .expect("in-memory workflow must complete before the deadline");
    }

    #[tokio::test(start_paused = true)]
    async fn reports_peer_closure() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (left, right) = duplex(MemConfig::default());
            testkit::closed_after_peer_drop(left, right).await;
        })
        .await
        .expect("in-memory workflow must complete before the deadline");
    }

    #[tokio::test(start_paused = true)]
    async fn receive_is_cancel_safe() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (left, right) = duplex(MemConfig::default());
            testkit::recv_is_cancel_safe(left, right).await;
        })
        .await
        .expect("in-memory workflow must complete before the deadline");
    }

    #[tokio::test(start_paused = true)]
    async fn rejects_oversized_messages_and_recovers() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let config = MemConfig {
                capacity: Some(2),
                max_message_len: Some(8),
                ..MemConfig::default()
            };
            let (left, right) = duplex(config);
            testkit::too_large_is_recoverable(left, right, 8).await;

            let (left, _right) = duplex(MemConfig {
                max_message_len: Some(8),
                ..MemConfig::default()
            });
            testkit::too_large(left, 8).await;
        })
        .await
        .expect("in-memory workflow must complete before the deadline");
    }

    #[tokio::test(start_paused = true)]
    async fn oversized_recovery_fits_small_limits() {
        tokio::time::timeout(Duration::from_secs(5), async {
            for max in [0, 1, 5] {
                let (left, right) = duplex(MemConfig {
                    max_message_len: Some(max),
                    ..MemConfig::default()
                });
                testkit::too_large_is_recoverable(left, right, max).await;
            }
        })
        .await
        .expect("recovery must complete before the deadline");
    }

    #[tokio::test(start_paused = true)]
    async fn confirmed_delivery_ignores_injected_loss() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (mut left, mut right) = duplex(MemConfig::default());
            left.drop_next(2);
            left.send(vec![1]).await.unwrap();
            assert_eq!(right.recv().await.unwrap(), vec![1]);
            let (mut sender, _) = left.split();
            sender.send(vec![2]).await.unwrap();
            assert_eq!(right.recv().await.unwrap(), vec![2]);
        })
        .await
        .expect("confirmed delivery must complete before the deadline");
    }

    #[tokio::test(start_paused = true)]
    async fn best_effort_reports_closure_even_with_injected_loss() {
        tokio::time::timeout(Duration::from_secs(5), async {
            for loss in [0, 1] {
                for split in [false, true] {
                    let (mut left, right) = duplex(MemConfig {
                        delivery: Delivery::BestEffort,
                        ..MemConfig::default()
                    });
                    left.drop_next(loss);
                    drop(right);
                    let result = if split {
                        let (mut sender, _) = left.split();
                        sender.send(vec![1]).await
                    } else {
                        left.send(vec![1]).await
                    };
                    assert!(matches!(result, Err(SendError::Closed)));
                }
            }
        })
        .await
        .expect("closed sends must complete before the deadline");
    }

    #[tokio::test(start_paused = true)]
    async fn accepts_a_message_at_the_exact_size_limit() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (mut left, mut right) = duplex(MemConfig {
                max_message_len: Some(8),
                ..MemConfig::default()
            });
            let message = [0_u8; 8];

            left.send(message.to_vec()).await.unwrap();
            let received = tokio::time::timeout(Duration::from_millis(100), right.recv())
                .await
                .expect("message should be delivered before the deadline")
                .unwrap();
            assert_eq!(received, message);
        })
        .await
        .expect("in-memory workflow must complete before the deadline");
    }

    #[tokio::test(start_paused = true)]
    async fn reconnects_after_peer_loss() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (connector, listener) = network(MemConfig::default());
            testkit::build_use_drop_rebuild(connector, listener, &MemAddr).await;
        })
        .await
        .expect("in-memory workflow must complete before the deadline");
    }

    #[tokio::test(start_paused = true)]
    async fn simultaneous_bursts_make_progress() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let config = MemConfig {
                capacity: Some(1),
                ..MemConfig::default()
            };
            let (left, right) = duplex(config);
            testkit::mutual_bursts_converge(left, right, 16).await;
        })
        .await
        .expect("in-memory workflow must complete before the deadline");
    }

    #[tokio::test(start_paused = true)]
    async fn zero_capacity_produces_a_usable_channel() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (mut left, mut right) = duplex(MemConfig {
                capacity: Some(0),
                ..MemConfig::default()
            });
            left.send(b"message".to_vec()).await.unwrap();
            assert_eq!(right.recv().await.unwrap(), b"message");
        })
        .await
        .expect("in-memory workflow must complete before the deadline");
    }

    #[tokio::test(start_paused = true)]
    async fn mock_is_fifo_per_direction() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (mut left, mut right) = duplex(MemConfig {
                capacity: Some(2),
                ..MemConfig::default()
            });
            left.send(b"first".to_vec()).await.unwrap();
            left.send(b"second".to_vec()).await.unwrap();
            assert_eq!(right.recv().await.unwrap(), b"first");
            assert_eq!(right.recv().await.unwrap(), b"second");
        })
        .await
        .expect("in-memory workflow must complete before the deadline");
    }

    #[tokio::test(start_paused = true)]
    async fn empty_message_roundtrips() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (mut left, mut right) = duplex(MemConfig::default());
            left.send(b"".to_vec()).await.unwrap();
            assert_eq!(right.recv().await.unwrap(), b"");
        })
        .await
        .expect("in-memory workflow must complete before the deadline");
    }

    #[tokio::test(start_paused = true)]
    async fn confirmed_send_times_out_when_the_queue_is_full() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (mut left, _right) = duplex(MemConfig {
                capacity: Some(1),
                send_timeout: Some(Duration::from_millis(5)),
                ..MemConfig::default()
            });
            left.send(b"first".to_vec()).await.unwrap();
            assert!(matches!(
                left.send(b"second".to_vec()).await,
                Err(SendError::Transport(_))
            ));
        })
        .await
        .expect("in-memory workflow must complete before the deadline");
    }

    #[tokio::test(start_paused = true)]
    async fn best_effort_silently_loses_when_the_queue_is_full() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (mut left, mut right) = duplex(MemConfig {
                capacity: Some(1),
                delivery: Delivery::BestEffort,
                ..MemConfig::default()
            });
            left.send(b"kept".to_vec()).await.unwrap();
            left.send(b"lost".to_vec()).await.unwrap();
            assert_eq!(right.recv().await.unwrap(), b"kept");
        })
        .await
        .expect("in-memory workflow must complete before the deadline");
    }

    #[tokio::test(start_paused = true)]
    async fn injected_loss_and_failure_are_observable() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (mut left, mut right) = duplex(MemConfig {
                capacity: Some(2),
                delivery: Delivery::BestEffort,
                ..MemConfig::default()
            });
            left.drop_next(1);
            left.send(b"lost".to_vec()).await.unwrap();
            left.send(b"kept".to_vec()).await.unwrap();
            assert_eq!(right.recv().await.unwrap(), b"kept");

            left.fail_next(1);
            assert!(matches!(
                left.send(b"failed".to_vec()).await,
                Err(SendError::Transport(_))
            ));
        })
        .await
        .expect("in-memory workflow must complete before the deadline");
    }

    #[tokio::test(start_paused = true)]
    async fn latency_uses_runtime_time() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (mut left, mut right) = duplex(MemConfig {
                latency: Some(Duration::from_secs(1)),
                ..MemConfig::default()
            });
            let started = tokio::time::Instant::now();
            left.send(b"message".to_vec()).await.unwrap();
            assert!(started.elapsed() >= Duration::from_secs(1));
            assert_eq!(right.recv().await.unwrap(), b"message");
        })
        .await
        .expect("in-memory workflow must complete before the deadline");
    }

    #[tokio::test(start_paused = true)]
    async fn dead_listener_and_connector_are_reported() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (mut connector, listener) = network(MemConfig::default());
            drop(listener);
            assert!(matches!(
                connector.build(&MemAddr).await,
                Err(BuildError::Unreachable)
            ));

            let (connector, mut listener) = network(MemConfig::default());
            drop(connector);
            assert!(matches!(
                listener.build(&()).await,
                Err(BuildError::Unreachable)
            ));
        })
        .await
        .expect("in-memory workflow must complete before the deadline");
    }
}
