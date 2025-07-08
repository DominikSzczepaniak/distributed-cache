// This is the entry point of your executable.
// It will:
//     Load application configuration (internal/config).
//     Initialize your pkg/cache instance (e.g., NewConcurrentMapCache). This instance will be your Raft state machine.
//     Initialize your Raft instance (pkg/raft). You'll pass your cache instance (wrapped by internal/cachefsm) to Raft as its state machine.
//     Initialize your HTTP/gRPC server (internal/server).
//     Start both the Raft node and the server.
// The main package here should be very thin, primarily orchestrating the startup.