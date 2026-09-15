//! Minimal RPC builder process used to exercise subprocess transport.

use fungi_transport::{
    BuildError, Channel, ChannelBuilder, RecvError, RecvHalf, SendError, SendHalf,
};
use fungi_transport_capnp::serve_builder;
use tokio::sync::mpsc;

#[derive(Debug)]
struct Echo {
    sender: mpsc::Sender<Vec<u8>>,
    receiver: mpsc::Receiver<Vec<u8>>,
}
#[derive(Debug)]
struct Sender<'a>(&'a mpsc::Sender<Vec<u8>>);
#[derive(Debug)]
struct Receiver<'a>(&'a mut mpsc::Receiver<Vec<u8>>);
impl SendHalf for Sender<'_> {
    type SendError = SendError;
    async fn send(&mut self, message: Vec<u8>) -> Result<(), SendError> {
        mpsc::Sender::send(self.0, message)
            .await
            .map_err(|_| SendError::Closed)
    }
}
impl RecvHalf for Receiver<'_> {
    type RecvError = RecvError;
    async fn recv(&mut self) -> Result<Vec<u8>, RecvError> {
        mpsc::Receiver::recv(self.0).await.ok_or(RecvError::Closed)
    }
}
impl SendHalf for Echo {
    type SendError = SendError;
    async fn send(&mut self, message: Vec<u8>) -> Result<(), SendError> {
        self.sender
            .send(message)
            .await
            .map_err(|_| SendError::Closed)
    }
}
impl RecvHalf for Echo {
    type RecvError = RecvError;
    async fn recv(&mut self) -> Result<Vec<u8>, RecvError> {
        self.receiver.recv().await.ok_or(RecvError::Closed)
    }
}
impl Channel for Echo {
    type SendHalf<'a> = Sender<'a>;
    type RecvHalf<'a> = Receiver<'a>;
    fn split(&mut self) -> (Self::SendHalf<'_>, Self::RecvHalf<'_>) {
        (Sender(&self.sender), Receiver(&mut self.receiver))
    }
}
#[derive(Debug)]
struct Builder;
impl ChannelBuilder for Builder {
    type Input = Vec<u8>;
    type Channel = Echo;
    async fn build(&mut self, _: &Vec<u8>) -> Result<Echo, BuildError> {
        let (sender, receiver) = mpsc::channel(1);
        Ok(Echo { sender, receiver })
    }
}
#[tokio::main(flavor = "current_thread")]
async fn main() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(serve_builder(
            Builder,
            Ok,
            tokio::io::join(tokio::io::stdin(), tokio::io::stdout()),
        ))
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn echo_supports_owned_and_borrowed_operations_and_reports_closure() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let mut echo = Builder.build(&Vec::new()).await.unwrap();
            echo.send(vec![42]).await.unwrap();
            assert_eq!(echo.recv().await.unwrap(), vec![42]);
            let (mut sender, mut receiver) = echo.split();
            sender.send(Vec::new()).await.unwrap();
            assert!(receiver.recv().await.unwrap().is_empty());
            echo.receiver.close();
            assert!(matches!(echo.send(vec![1]).await, Err(SendError::Closed)));
            assert!(matches!(echo.recv().await, Err(RecvError::Closed)));
            let (mut sender, mut receiver) = echo.split();
            assert!(matches!(sender.send(vec![1]).await, Err(SendError::Closed)));
            assert!(matches!(receiver.recv().await, Err(RecvError::Closed)));
        })
        .await
        .unwrap();
    }
}
