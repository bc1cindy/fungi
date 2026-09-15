@0xeacf7a61dba65b94;

struct SendResult {
  union {
    accepted @0 :Void;
    tooLarge @1 :UInt64;
    closed @2 :Void;
    failed @3 :Text;
  }
}

struct RecvResult {
  union {
    message @0 :Data;
    closed @1 :Void;
    failed @2 :Text;
  }
}

interface Channel {
  send @0 (message :Data) -> (result :SendResult);
  recv @1 () -> (result :RecvResult);
}

struct BuildResult {
  union {
    channel @0 :Channel;
    unreachable @1 :Void;
    failed @2 :Text;
  }
}

# Inputs are opaque backend tokens. An inbound builder receives empty data.
interface Builder {
  build @0 (input :Data) -> (result :BuildResult);
}
