#pragma once

#include <memory>
#include <string>
#include <vector>

// PeerControlClient: sends Notify + Stop RPCs to peer kvtest instances.
//
// The httplib implementation (cmake mode) serializes the structured payload to
// the legacy JSON body and POSTs /notify, /stop. The brpc implementation
// (KVTEST_USE_BRPC, bazel mode) fills the typed kvtest_control proto messages
// and calls KvtestControl::Stub over a brpc::Channel. Both carry the same
// structured fields so the notify protocol semantics are identical across
// build systems; only the wire transport differs.
//
// Threading: Notify may be called concurrently from a thread pool; impls must
// be thread-safe. Notify is fire-and-forget (errors logged, never thrown).
class PeerControlClient {
public:
    virtual ~PeerControlClient() = default;

    // Notify a peer. action="warmup_done" targets the cache warmup path; an
    // empty action takes the normal notify_pipeline path.
    virtual void Notify(const std::string &host, int port, const std::string &action,
                        int sender, const std::vector<std::string> &keys, uint64_t size) = 0;

    // Stop a peer (graceful). Returns true on success.
    virtual bool Stop(const std::string &host, int port) = 0;
};

// Build the peer control client selected by the build system:
//   KVTEST_USE_BRPC -> brpc channel + KvtestControl::Stub
//   otherwise       -> httplib::Client (legacy /notify, /stop)
std::unique_ptr<PeerControlClient> MakePeerControlClient();
