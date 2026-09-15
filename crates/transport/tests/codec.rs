//! Typed consumer workflows over byte transports.

use std::error::Error;
use std::io;
use std::time::Duration;

use fungi_transport::{
    Channel, Codec, CodecChannel, CodecError, RecvError, RecvHalf, SendError, SendHalf, into_stream,
};
use fungi_transport_testkit::mem::{MemConfig, duplex};
use futures_util::StreamExt;

#[derive(Debug, PartialEq, Eq)]
struct Message(u8);

#[derive(Debug)]
struct ByteCodec;

impl Codec<Message> for ByteCodec {
    type EncodeError = io::Error;
    type DecodeError = io::Error;

    fn encode(&self, message: Message) -> Result<Vec<u8>, io::Error> {
        if message.0 == 255 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "reserved value",
            ));
        }
        Ok(vec![message.0])
    }

    fn decode(&self, bytes: Vec<u8>) -> Result<Message, io::Error> {
        match bytes.as_slice() {
            [value] => Ok(Message(*value)),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "expected one byte",
            )),
        }
    }
}

#[tokio::test(start_paused = true)]
async fn typed_roundtrip_and_encode_recovery() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (left, right) = duplex(MemConfig::default());
        let mut left = CodecChannel::new(left, ByteCodec);
        let mut right = CodecChannel::new(right, ByteCodec);
        let error = left.send(Message(255)).await.unwrap_err();
        assert!(matches!(error, CodecError::Codec(_)));
        assert_eq!(error.to_string(), "codec: reserved value");
        assert_eq!(error.source().unwrap().to_string(), "reserved value");
        left.send(Message(7)).await.unwrap();
        assert_eq!(right.recv().await.unwrap(), Message(7));
        right.send(Message(8)).await.unwrap();
        assert_eq!(left.recv().await.unwrap(), Message(8));
        drop(right);
        let error = left.send(Message(9)).await.unwrap_err();
        assert!(matches!(error, CodecError::Transport(SendError::Closed)));
        assert_eq!(error.to_string(), "transport: channel closed");
        assert_eq!(error.source().unwrap().to_string(), "channel closed");
        assert!(matches!(
            left.recv().await,
            Err(CodecError::Transport(RecvError::Closed))
        ));
    })
    .await
    .expect("typed roundtrip must complete");
}

#[tokio::test(start_paused = true)]
async fn invalid_payload_ends_a_typed_stream() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (mut sender, receiver) = duplex(MemConfig::default());
        sender.send(vec![1, 2]).await.unwrap();
        let stream = into_stream(CodecChannel::new(receiver, ByteCodec));
        futures_util::pin_mut!(stream);
        let error = stream.next().await.unwrap().unwrap_err();
        assert!(matches!(error, CodecError::Codec(_)));
        assert_eq!(error.source().unwrap().to_string(), "expected one byte");
        assert!(stream.next().await.is_none());
    })
    .await
    .expect("typed stream must complete");
}

#[tokio::test(start_paused = true)]
async fn borrowed_typed_halves_preserve_errors_and_cancel_safety() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (mut peer, receiver) = duplex(MemConfig::default());
        let mut receiver = CodecChannel::new(receiver, ByteCodec);
        let (mut sender, mut receiver) = receiver.split();
        assert!(
            tokio::time::timeout(Duration::from_millis(1), receiver.recv())
                .await
                .is_err()
        );
        peer.send(vec![42]).await.unwrap();
        assert_eq!(receiver.recv().await.unwrap(), Message(42));
        assert!(matches!(
            sender.send(Message(255)).await,
            Err(CodecError::Codec(_))
        ));
        sender.send(Message(3)).await.unwrap();
        assert_eq!(peer.recv().await.unwrap(), vec![3]);
        peer.send(vec![]).await.unwrap();
        assert!(matches!(receiver.recv().await, Err(CodecError::Codec(_))));
    })
    .await
    .expect("typed halves must complete");
}

#[tokio::test(start_paused = true)]
async fn typed_halves_make_independent_progress_under_backpressure() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (left, right) = duplex(MemConfig::default());
        let mut left = CodecChannel::new(left, ByteCodec);
        let mut right = CodecChannel::new(right, ByteCodec);
        let (mut ls, mut lr) = left.split();
        let (mut rs, mut rr) = right.split();
        let send = async {
            for value in 0..8 {
                ls.send(Message(value)).await.unwrap();
                rs.send(Message(value + 8)).await.unwrap();
            }
        };
        let recv = async {
            for value in 0..8 {
                assert_eq!(rr.recv().await.unwrap(), Message(value));
                assert_eq!(lr.recv().await.unwrap(), Message(value + 8));
            }
        };
        futures_util::future::join(send, recv).await;
        drop(right);
        let (mut sender, mut receiver) = left.split();
        assert!(matches!(
            sender.send(Message(1)).await,
            Err(CodecError::Transport(SendError::Closed))
        ));
        assert!(matches!(
            receiver.recv().await,
            Err(CodecError::Transport(RecvError::Closed))
        ));
    })
    .await
    .expect("typed duplex must complete");
}
