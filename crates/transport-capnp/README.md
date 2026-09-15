# Cap'n Proto transport bridge

This crate exposes Fungi byte channels and channel builders across a Cap'n Proto
RPC stream, including subprocess stdin/stdout. It belongs to the root Cargo
workspace and uses its lockfile, metadata, lints, and Nix checks.

`CapnpChannel` implements `Channel<Vec<u8>>`, `SendHalf`, and `RecvHalf`.
`split()` borrows both directions from one owner. Use the existing `CodecChannel`
adapter to exchange typed messages; the RPC protocol carries opaque bytes.

`CapnpBuilder` accepts backend-specific byte tokens. `into_acceptor()` exposes a
unit-input inbound builder and sends an empty token. `serve_builder` receives a
synchronous decoder that maps those tokens to the backend's `ChannelBuilder::Input`.
It does not prescribe a network address format or transport factory API.

Client constructors accept an explicit local outbound message limit. Oversized
local messages are rejected before queuing an RPC call. Backend size errors retain
`TooLarge { max }` across RPC and leave the channel usable. Other send errors and
all receive errors require replacing the channel. Opaque backend errors retain
diagnostic text across processes, rather than their original Rust error types.

RPC capabilities stay on a dedicated thread with a current-thread Tokio runtime
and `LocalSet`; only commands, replies, and client handles cross threads. A pending
receive response belongs to the channel, so dropping an operation future does not
consume its message. Per-channel actors run send and receive independently and
release their capabilities when the channel is dropped. Abandoned build results
are dropped rather than retained in a capability registry.

`serve` and `serve_builder` run on the caller's `LocalSet`. The server owns each
backend and drives its borrowed halves concurrently. Either direction terminating
ends the backend's pump; an interrupted send is never resumed on that channel.
Liveness timeouts remain the consumer's responsibility.

`CapnpBuilder::spawn` starts an RPC builder process with piped stdin/stdout and
inherited stderr. Derived channels keep the connection alive after the builder is
dropped. Dropping the last handle closes RPC, allows a short grace period, then
kills and reaps a process that does not exit. `capnp-echo` is a minimal builder
process for subprocess integration tests.

`channel.capnp` defines this protocol independently of the former transport
repository's Tor-specific plugin factory schema. It does not promise compatibility
with those plugins, peer authentication, ordering, or delivery confirmation.

Building requires the `capnp` compiler on `PATH`. The root Nix workspace supplies
it and retains schema files in the build source. Rust bindings are generated into
`OUT_DIR`, not committed to the repository.
