//! Cap'n Proto RPC channels and builders for the Fungi transport contract.
//!
//! RPC capabilities stay on a dedicated current-thread executor. Client handles
//! and their operation futures are `Send`. The connection remains alive until
//! its last channel or builder is dropped. The RPC link provides plumbing, not
//! peer authentication or delivery confirmation.

use std::io;
use std::rc::Rc;
use std::sync::{Arc, Weak};

use capnp::capability::Promise;
use capnp_rpc::{RpcSystem, rpc_twoparty_capnp::Side, twoparty};
use fungi_transport::{
    BuildError, Channel, ChannelBuilder, RecvError, RecvHalf, SendError, SendHalf,
};
use futures_util::{StreamExt, stream::FuturesUnordered};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

mod channel_capnp {
    #![allow(dead_code, missing_docs, missing_debug_implementations, clippy::all)]
    include!(concat!(env!("OUT_DIR"), "/channel_capnp.rs"));
}
use channel_capnp::{build_result, builder, channel, recv_result, send_result};

type Reply<T> = oneshot::Sender<T>;
type PendingReceive = Option<oneshot::Receiver<Result<Vec<u8>, RecvError>>>;

enum ChannelCommand {
    Send(Vec<u8>, Reply<Result<(), SendError>>),
    Recv(Reply<Result<Vec<u8>, RecvError>>),
}
struct BuildCommand {
    input: Vec<u8>,
    reply: Reply<Result<CapnpChannel, BuildError>>,
}
#[derive(Debug)]
struct Link {
    _lifetime: mpsc::Sender<()>,
    max_message_len: usize,
}
enum Bootstrap {
    Channel(mpsc::Receiver<ChannelCommand>),
    Builder(mpsc::Receiver<BuildCommand>, Weak<Link>),
}

/// One remote byte channel. Receiving state survives canceled operation futures.
#[derive(Debug)]
pub struct CapnpChannel {
    commands: mpsc::Sender<ChannelCommand>,
    pending: PendingReceive,
    link: Arc<Link>,
}

impl CapnpChannel {
    /// Connect to a channel bootstrap over an owned RPC stream.
    pub fn connect<Io>(io: Io, max_message_len: usize) -> io::Result<Self>
    where
        Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        let (link, lifetime) = new_link(max_message_len);
        let (commands, receiver) = mpsc::channel(2);
        start_client(io, Bootstrap::Channel(receiver), lifetime)?;
        Ok(Self {
            commands,
            pending: None,
            link,
        })
    }
}

/// Borrowed sending direction of a remote channel.
#[derive(Debug)]
pub struct CapnpSendHalf<'a> {
    commands: &'a mpsc::Sender<ChannelCommand>,
    max_message_len: usize,
}
/// Borrowed receiving direction of a remote channel.
#[derive(Debug)]
pub struct CapnpRecvHalf<'a> {
    commands: &'a mpsc::Sender<ChannelCommand>,
    pending: &'a mut PendingReceive,
}

async fn send_message(
    commands: &mpsc::Sender<ChannelCommand>,
    max: usize,
    message: Vec<u8>,
) -> Result<(), SendError> {
    if message.len() > max {
        return Err(SendError::TooLarge { max });
    }
    let (reply, result) = oneshot::channel();
    commands
        .send(ChannelCommand::Send(message, reply))
        .await
        .map_err(|_| SendError::Closed)?;
    result.await.map_err(|_| SendError::Closed)?
}

async fn receive_message(
    commands: &mpsc::Sender<ChannelCommand>,
    pending: &mut PendingReceive,
) -> Result<Vec<u8>, RecvError> {
    if pending.is_none() {
        let (reply, result) = oneshot::channel();
        commands
            .send(ChannelCommand::Recv(reply))
            .await
            .map_err(|_| RecvError::Closed)?;
        // No suspension between queuing the request and retaining its response.
        *pending = Some(result);
    }
    let result = pending
        .as_mut()
        .unwrap()
        .await
        .map_err(|_| RecvError::Closed);
    *pending = None;
    result?
}

impl SendHalf for CapnpChannel {
    type SendError = SendError;
    async fn send(&mut self, message: Vec<u8>) -> Result<(), SendError> {
        send_message(&self.commands, self.link.max_message_len, message).await
    }
}
impl RecvHalf for CapnpChannel {
    type RecvError = RecvError;
    async fn recv(&mut self) -> Result<Vec<u8>, RecvError> {
        receive_message(&self.commands, &mut self.pending).await
    }
}
impl SendHalf for CapnpSendHalf<'_> {
    type SendError = SendError;
    async fn send(&mut self, message: Vec<u8>) -> Result<(), SendError> {
        send_message(self.commands, self.max_message_len, message).await
    }
}
impl RecvHalf for CapnpRecvHalf<'_> {
    type RecvError = RecvError;
    async fn recv(&mut self) -> Result<Vec<u8>, RecvError> {
        receive_message(self.commands, self.pending).await
    }
}
impl Channel for CapnpChannel {
    type SendHalf<'a> = CapnpSendHalf<'a>;
    type RecvHalf<'a> = CapnpRecvHalf<'a>;
    fn split(&mut self) -> (Self::SendHalf<'_>, Self::RecvHalf<'_>) {
        (
            CapnpSendHalf {
                commands: &self.commands,
                max_message_len: self.link.max_message_len,
            },
            CapnpRecvHalf {
                commands: &self.commands,
                pending: &mut self.pending,
            },
        )
    }
}

/// Remote builder accepting opaque byte tokens interpreted by the backend.
#[derive(Debug)]
pub struct CapnpBuilder {
    commands: mpsc::Sender<BuildCommand>,
    _link: Arc<Link>,
}
impl CapnpBuilder {
    /// Connect to a builder bootstrap over an owned RPC stream.
    pub fn connect<Io>(io: Io, max_message_len: usize) -> io::Result<Self>
    where
        Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        let (link, lifetime) = new_link(max_message_len);
        let (commands, receiver) = mpsc::channel(1);
        let bootstrap = Bootstrap::Builder(receiver, Arc::downgrade(&link));
        start_client(io, bootstrap, lifetime)?;
        Ok(Self {
            commands,
            _link: link,
        })
    }

    /// Spawn a builder server process speaking RPC on stdin/stdout.
    ///
    /// Stderr is inherited. Dropping the last derived handle kills and reaps the
    /// process. Spawn errors are reported before a builder handle is returned.
    pub fn spawn(mut command: tokio::process::Command, max_message_len: usize) -> io::Result<Self> {
        let (link, lifetime) = new_link(max_message_len);
        let (commands, receiver) = mpsc::channel(1);
        let boot = Bootstrap::Builder(receiver, Arc::downgrade(&link));
        let (ready, started) = std::sync::mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name("capnp-client".into())
            .spawn(move || {
                let mut setup = || -> io::Result<_> {
                    let runtime = runtime()?;
                    let entered = runtime.enter();
                    let child = command
                        .stdin(std::process::Stdio::piped())
                        .stdout(std::process::Stdio::piped())
                        .kill_on_drop(true)
                        .spawn();
                    drop(entered);
                    Ok((runtime, child?))
                };
                match setup() {
                    Ok((runtime, mut child)) => {
                        let reader = child.stdout.take().unwrap();
                        let writer = child.stdin.take().unwrap();
                        let _ = ready.send(Ok(()));
                        let local = tokio::task::LocalSet::new();
                        local.block_on(&runtime, async move {
                            run_client(reader, writer, boot, lifetime).await;
                            if tokio::time::timeout(
                                std::time::Duration::from_millis(100),
                                child.wait(),
                            )
                            .await
                            .is_err()
                            {
                                let _ = child.start_kill();
                                let _ = child.wait().await;
                            }
                        });
                    }
                    Err(error) => {
                        let _ = ready.send(Err(error));
                    }
                }
            })?;
        started.recv().map_err(io::Error::other)??;
        Ok(Self {
            commands,
            _link: link,
        })
    }

    /// Treat this remote builder as an inbound builder with unit input.
    pub fn into_acceptor(self) -> CapnpAcceptor {
        CapnpAcceptor(self)
    }
}
impl ChannelBuilder for CapnpBuilder {
    type Input = Vec<u8>;
    type Channel = CapnpChannel;
    async fn build(&mut self, input: &Vec<u8>) -> Result<CapnpChannel, BuildError> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(BuildCommand {
                input: input.clone(),
                reply,
            })
            .await
            .map_err(|_| BuildError::Unreachable)?;
        result.await.map_err(|_| BuildError::Unreachable)?
    }
}
/// Inbound remote builder. Sends an empty token for each acceptance request.
#[derive(Debug)]
pub struct CapnpAcceptor(CapnpBuilder);
impl ChannelBuilder for CapnpAcceptor {
    type Input = ();
    type Channel = CapnpChannel;
    async fn build(&mut self, _: &()) -> Result<CapnpChannel, BuildError> {
        self.0.build(&Vec::new()).await
    }
}

fn new_link(max_message_len: usize) -> (Arc<Link>, mpsc::Receiver<()>) {
    let (sender, receiver) = mpsc::channel(1);
    (
        Arc::new(Link {
            _lifetime: sender,
            max_message_len,
        }),
        receiver,
    )
}
fn runtime() -> io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
}
fn start_client<Io>(io: Io, boot: Bootstrap, lifetime: mpsc::Receiver<()>) -> io::Result<()>
where
    Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let runtime = runtime()?;
    std::thread::Builder::new()
        .name("capnp-client".into())
        .spawn(move || {
            let local = tokio::task::LocalSet::new();
            let (reader, writer) = tokio::io::split(io);
            local.block_on(&runtime, run_client(reader, writer, boot, lifetime));
        })?;
    Ok(())
}
async fn run_client<R, W>(reader: R, writer: W, boot: Bootstrap, mut lifetime: mpsc::Receiver<()>)
where
    R: AsyncRead + Unpin + 'static,
    W: AsyncWrite + Unpin + 'static,
{
    let network = twoparty::VatNetwork::new(
        reader.compat(),
        writer.compat_write(),
        Side::Client,
        Default::default(),
    );
    let mut rpc = RpcSystem::new(Box::new(network), None);
    match boot {
        Bootstrap::Channel(receiver) => {
            let remote = rpc.bootstrap::<channel::Client>(Side::Server);
            tokio::task::spawn_local(channel_actor(remote, receiver));
        }
        Bootstrap::Builder(receiver, link) => {
            let remote = rpc.bootstrap::<builder::Client>(Side::Server);
            tokio::task::spawn_local(builder_actor(remote, receiver, link));
        }
    }
    tokio::select! { _ = rpc => {}, _ = lifetime.recv() => {} }
}
async fn channel_actor(remote: channel::Client, mut commands: mpsc::Receiver<ChannelCommand>) {
    let mut operations = FuturesUnordered::new();
    loop {
        tokio::select! {
            command = commands.recv() => {
                let Some(command) = command else { break };
                operations.push(dispatch_channel(remote.clone(), command));
            }
            _ = operations.next(), if !operations.is_empty() => {}
        }
    }
}
async fn dispatch_channel(remote: channel::Client, command: ChannelCommand) {
    match command {
        ChannelCommand::Send(message, reply) => {
            let mut request = remote.send_request();
            request.get().set_message(&message);
            let result = async {
                let response = request.send().promise.await.map_err(rpc_send_error)?;
                let result = response
                    .get()
                    .and_then(|r| r.get_result())
                    .map_err(rpc_send_error)?;
                match result
                    .which()
                    .map_err(capnp::Error::from)
                    .map_err(rpc_send_error)?
                {
                    send_result::Accepted(()) => Ok(()),
                    send_result::TooLarge(max) => Err(SendError::TooLarge {
                        max: usize::try_from(max).unwrap_or(usize::MAX),
                    }),
                    send_result::Closed(()) => Err(SendError::Closed),
                    send_result::Failed(error) => Err(SendError::Transport(
                        error
                            .and_then(|t| t.to_str().map_err(Into::into))
                            .map_err(rpc_send_error)?
                            .to_owned()
                            .into(),
                    )),
                }
            }
            .await;
            let _ = reply.send(result);
        }
        ChannelCommand::Recv(reply) => {
            let result = async {
                let response = remote
                    .recv_request()
                    .send()
                    .promise
                    .await
                    .map_err(rpc_recv_error)?;
                let result = response
                    .get()
                    .and_then(|r| r.get_result())
                    .map_err(rpc_recv_error)?;
                match result
                    .which()
                    .map_err(capnp::Error::from)
                    .map_err(rpc_recv_error)?
                {
                    recv_result::Message(message) => Ok(message.map_err(rpc_recv_error)?.to_vec()),
                    recv_result::Closed(()) => Err(RecvError::Closed),
                    recv_result::Failed(error) => Err(RecvError::Transport(
                        error
                            .and_then(|t| t.to_str().map_err(Into::into))
                            .map_err(rpc_recv_error)?
                            .to_owned()
                            .into(),
                    )),
                }
            }
            .await;
            let _ = reply.send(result);
        }
    }
}
async fn builder_actor(
    remote: builder::Client,
    mut commands: mpsc::Receiver<BuildCommand>,
    link: Weak<Link>,
) {
    let mut operations = FuturesUnordered::new();
    loop {
        tokio::select! {
            command = commands.recv() => {
                let Some(command) = command else { break };
                operations.push(dispatch_build(remote.clone(), command, link.clone()));
            }
            _ = operations.next(), if !operations.is_empty() => {}
        }
    }
}
async fn dispatch_build(remote: builder::Client, command: BuildCommand, link: Weak<Link>) {
    let mut request = remote.build_request();
    request.get().set_input(&command.input);
    let result = async {
        let response = request.send().promise.await.map_err(rpc_build_error)?;
        let result = response
            .get()
            .and_then(|r| r.get_result())
            .map_err(rpc_build_error)?;
        match result
            .which()
            .map_err(capnp::Error::from)
            .map_err(rpc_build_error)?
        {
            build_result::Channel(remote) => {
                let remote = remote.map_err(rpc_build_error)?;
                let link = link.upgrade().ok_or(BuildError::Unreachable)?;
                let (commands, receiver) = mpsc::channel(2);
                tokio::task::spawn_local(channel_actor(remote, receiver));
                Ok(CapnpChannel {
                    commands,
                    pending: None,
                    link,
                })
            }
            build_result::Unreachable(()) => Err(BuildError::Unreachable),
            build_result::Failed(error) => Err(BuildError::Transport(
                error
                    .and_then(|t| t.to_str().map_err(Into::into))
                    .map_err(rpc_build_error)?
                    .to_owned()
                    .into(),
            )),
        }
    }
    .await;
    // An abandoned build drops its channel, closing the per-capability actor.
    let _ = command.reply.send(result);
}
fn rpc_send_error(error: capnp::Error) -> SendError {
    if error.kind == capnp::ErrorKind::Disconnected {
        SendError::Closed
    } else {
        SendError::Transport(error.into())
    }
}
fn rpc_recv_error(error: capnp::Error) -> RecvError {
    if error.kind == capnp::ErrorKind::Disconnected {
        RecvError::Closed
    } else {
        RecvError::Transport(error.into())
    }
}
fn rpc_build_error(error: capnp::Error) -> BuildError {
    if error.kind == capnp::ErrorKind::Disconnected {
        BuildError::Unreachable
    } else {
        BuildError::Transport(error.into())
    }
}

type QueuedSend = (Vec<u8>, Reply<Result<(), SendError>>);
type Received = mpsc::Receiver<Result<Vec<u8>, RecvError>>;
struct ChannelServer {
    sends: mpsc::Sender<QueuedSend>,
    receives: Rc<Mutex<Received>>,
}
fn serve_channel<C>(mut backend: C) -> channel::Client
where
    C: Channel<Vec<u8>, SendError = SendError, RecvError = RecvError> + 'static,
{
    let (sends, mut send_commands) = mpsc::channel::<QueuedSend>(1);
    let (received, receives) = mpsc::channel(1);
    tokio::task::spawn_local(async move {
        let (mut sender, mut receiver) = backend.split();
        let sending = async move {
            while let Some((message, reply)) = send_commands.recv().await {
                let result = sender.send(message).await;
                let terminal = !matches!(result, Ok(()) | Err(SendError::TooLarge { .. }));
                let _ = reply.send(result);
                if terminal {
                    break;
                }
            }
        };
        let receiving = async move {
            loop {
                let result = receiver.recv().await;
                let terminal = result.is_err();
                if received.send(result).await.is_err() || terminal {
                    break;
                }
            }
        };
        // Finishing either direction disposes the whole channel. An interrupted
        // send is never resumed on this backend, preserving its framing contract.
        tokio::select! { _ = sending => {}, _ = receiving => {} }
    });
    capnp_rpc::new_client(ChannelServer {
        sends,
        receives: Rc::new(Mutex::new(receives)),
    })
}
impl channel::Server for ChannelServer {
    fn send(
        &mut self,
        params: channel::SendParams,
        mut results: channel::SendResults,
    ) -> Promise<(), capnp::Error> {
        let message = capnp_rpc::pry!(capnp_rpc::pry!(params.get()).get_message()).to_vec();
        let sends = self.sends.clone();
        Promise::from_future(async move {
            let (reply, result) = oneshot::channel();
            let result = if sends.send((message, reply)).await.is_err() {
                Err(SendError::Closed)
            } else {
                result.await.unwrap_or(Err(SendError::Closed))
            };
            let mut output = results.get().init_result();
            match result {
                Ok(()) => output.set_accepted(()),
                Err(SendError::TooLarge { max }) => output.set_too_large(max as u64),
                Err(SendError::Closed) => output.set_closed(()),
                Err(error) => output.set_failed(error.to_string().as_str()),
            }
            Ok(())
        })
    }
    fn recv(
        &mut self,
        _: channel::RecvParams,
        mut results: channel::RecvResults,
    ) -> Promise<(), capnp::Error> {
        let receives = self.receives.clone();
        Promise::from_future(async move {
            let result = receives
                .lock()
                .await
                .recv()
                .await
                .unwrap_or(Err(RecvError::Closed));
            let mut output = results.get().init_result();
            match result {
                Ok(message) => output.set_message(&message),
                Err(RecvError::Closed) => output.set_closed(()),
                Err(error) => output.set_failed(error.to_string().as_str()),
            }
            Ok(())
        })
    }
}
struct BuilderServer<B, D> {
    backend: Rc<Mutex<B>>,
    decode: Rc<D>,
}
impl<B, D> builder::Server for BuilderServer<B, D>
where
    B: ChannelBuilder + 'static,
    B::Channel: Channel<Vec<u8>, SendError = SendError, RecvError = RecvError> + 'static,
    D: Fn(Vec<u8>) -> Result<B::Input, BuildError> + 'static,
{
    fn build(
        &mut self,
        params: builder::BuildParams,
        mut results: builder::BuildResults,
    ) -> Promise<(), capnp::Error> {
        let input = capnp_rpc::pry!(capnp_rpc::pry!(params.get()).get_input()).to_vec();
        let input = (self.decode)(input);
        let backend = self.backend.clone();
        Promise::from_future(async move {
            let result = match input {
                Ok(input) => backend.lock().await.build(&input).await,
                Err(error) => Err(error),
            };
            let mut output = results.get().init_result();
            match result {
                Ok(channel) => output.set_channel(serve_channel(channel)),
                Err(BuildError::Unreachable) => output.set_unreachable(()),
                Err(error) => output.set_failed(error.to_string().as_str()),
            }
            Ok(())
        })
    }
}

async fn run_server<Io>(io: Io, bootstrap: capnp::capability::Client)
where
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
{
    let (reader, writer) = tokio::io::split(io);
    let network = twoparty::VatNetwork::new(
        reader.compat(),
        writer.compat_write(),
        Side::Server,
        Default::default(),
    );
    let _ = RpcSystem::new(Box::new(network), Some(bootstrap)).await;
}
/// Serve an owned backend channel on the caller's `LocalSet`.
pub async fn serve<C, Io>(backend: C, io: Io)
where
    C: Channel<Vec<u8>, SendError = SendError, RecvError = RecvError> + 'static,
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
{
    run_server(io, serve_channel(backend).client).await;
}
/// Serve a builder on the caller's `LocalSet`, decoding opaque input tokens.
///
/// The decoder runs synchronously before the backend is borrowed. Backend
/// channel errors cross the RPC boundary as structured results; opaque errors
/// retain their diagnostic text, not their original Rust types.
pub async fn serve_builder<B, D, Io>(backend: B, decode: D, io: Io)
where
    B: ChannelBuilder + 'static,
    B::Channel: Channel<Vec<u8>, SendError = SendError, RecvError = RecvError> + 'static,
    D: Fn(Vec<u8>) -> Result<B::Input, BuildError> + 'static,
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
{
    let bootstrap: builder::Client = capnp_rpc::new_client(BuilderServer {
        backend: Rc::new(Mutex::new(backend)),
        decode: Rc::new(decode),
    });
    run_server(io, bootstrap.client).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use fungi_transport_testkit::mem::{MemConfig, duplex};
    use std::time::Duration;

    #[tokio::test]
    async fn stopped_actors_report_closed_channels_and_unreachable_builders() {
        let (commands, receiver) = mpsc::channel(1);
        drop(receiver);
        assert!(matches!(
            send_message(&commands, 1024, vec![1]).await,
            Err(SendError::Closed)
        ));
        let mut pending = None;
        assert!(matches!(
            receive_message(&commands, &mut pending).await,
            Err(RecvError::Closed)
        ));
        assert!(pending.is_none());

        let (commands, receiver) = mpsc::channel(1);
        drop(receiver);
        let (link, _lifetime) = new_link(1024);
        let mut builder = CapnpBuilder {
            commands,
            _link: link,
        };
        assert!(matches!(
            builder.build(&vec![1]).await,
            Err(BuildError::Unreachable)
        ));
    }

    #[tokio::test]
    async fn canceled_receive_preserves_a_response_already_delivered_to_the_client() {
        tokio::time::timeout(
            Duration::from_secs(5),
            tokio::task::LocalSet::new().run_until(async {
                let (backend, mut peer) = duplex(MemConfig::default());
                let (client, io) = tokio::io::duplex(64);
                let server = tokio::task::spawn_local(serve(backend, io));
                let mut channel = CapnpChannel::connect(client, 1024).unwrap();
                assert!(
                    tokio::time::timeout(Duration::from_millis(5), channel.recv())
                        .await
                        .is_err()
                );
                peer.send(b"retained".to_vec()).await.unwrap();
                tokio::time::timeout(Duration::from_secs(5), async {
                    while channel.pending.as_ref().unwrap().is_empty() {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                })
                .await
                .unwrap();
                let (_, mut receiver) = channel.split();
                assert_eq!(receiver.recv().await.unwrap(), b"retained");
                drop(peer);
                assert!(
                    tokio::time::timeout(Duration::from_secs(5), receiver.recv())
                        .await
                        .unwrap()
                        .is_err()
                );
                drop(channel);
                tokio::time::timeout(Duration::from_secs(5), server)
                    .await
                    .unwrap()
                    .unwrap();
            }),
        )
        .await
        .unwrap();
    }

    #[test]
    fn rpc_disconnects_and_protocol_failures_have_distinct_diagnostics() {
        assert!(matches!(
            rpc_send_error(capnp::Error::disconnected("gone".into())),
            SendError::Closed
        ));
        assert!(matches!(
            rpc_recv_error(capnp::Error::disconnected("gone".into())),
            RecvError::Closed
        ));
        assert!(matches!(
            rpc_build_error(capnp::Error::disconnected("gone".into())),
            BuildError::Unreachable
        ));
        let send = rpc_send_error(capnp::Error::failed("bad response".into()));
        let recv = rpc_recv_error(capnp::Error::failed("bad response".into()));
        let build = rpc_build_error(capnp::Error::failed("bad response".into()));
        for error in [&send as &dyn std::error::Error, &recv, &build] {
            assert!(error.to_string().contains("bad response"));
            assert!(error.source().is_some());
        }
    }
}
