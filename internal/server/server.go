// This package would define your actual cache server (e.g., HTTP handlers, gRPC service definitions).
// Its methods (HandleGet, HandleSet, HandleDelete) would:
//     For Get operations (read-only): Directly query the local pkg/cache instance.
//     For Set/Delete operations (mutations): Submit the command to the Raft leader (via raft.Apply(command)). The Raft layer will then replicate it, commit it, and eventually call your internal/cachefsm's Apply method on all nodes.
// This package connects your external API to your Raft cluster and your cache.