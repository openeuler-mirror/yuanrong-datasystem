#include "peer_client.h"

#include "common/bthread_compat.h"
#include "common/simple_log.h"
#include "vendor/nlohmann_json.hpp"

#include <unordered_map>
#include <unordered_set>
#include <utility>

#ifdef KVTEST_USE_BRPC
#include "kvtest_control.pb.h"

#include <brpc/channel.h>
#include <brpc/controller.h>
#else
#include "vendor/httplib.h"
#endif

using json = nlohmann::json;

namespace {
// Split "http://host:port" or "host:port" into host/port. Returns false if the
// URL has no port. Kept here so both impls share one parsing routine.
bool SplitHostPort(const std::string &peerUrl, std::string &host, int &port) {
    std::string hostPort = peerUrl;
    if (hostPort.size() > 7 && hostPort.compare(0, 7, "http://") == 0) {
        hostPort = hostPort.substr(7);
    }
    auto colonPos = hostPort.find(':');
    if (colonPos == std::string::npos) return false;
    try {
        host = hostPort.substr(0, colonPos);
        port = std::stoi(hostPort.substr(colonPos + 1));
        return true;
    } catch (...) {
        return false;
    }
}
}  // namespace

#ifndef KVTEST_USE_BRPC
// ---------------------------------------------------------------------------
// httplib implementation (cmake mode): preserves the legacy wire format
// (POST /notify with JSON body, POST /stop) so un-upgraded peers still work.
// ---------------------------------------------------------------------------
class HttpPeerClient : public PeerControlClient {
public:
    void Notify(const std::string &host, int port, const std::string &action,
                int sender, const std::vector<std::string> &keys, uint64_t size) override {
        try {
            thread_local std::unordered_map<std::string,
                std::unique_ptr<httplib::Client>> clientCache;
            std::string ckey = host + ":" + std::to_string(port);
            auto &ref = clientCache[ckey];
            if (!ref) {
                ref = std::make_unique<httplib::Client>(host, port);
                ref->set_connection_timeout(2);
                ref->set_read_timeout(2);
            }
            // Reconstruct the original JSON body {action, sender, keys, size}.
            json j;
            if (!action.empty()) j["action"] = action;
            j["sender"] = sender;
            if (!keys.empty()) j["keys"] = keys;
            if (size > 0) j["size"] = size;
            ref->Post("/notify", j.dump(), "application/json");
        } catch (...) {
        }
    }

    bool Stop(const std::string &host, int port) override {
        try {
            httplib::Client cli(host, port);
            cli.set_connection_timeout(5);
            cli.set_read_timeout(5);
            auto res = cli.Post("/stop");
            return res && res->status == 200;
        } catch (...) {
            return false;
        }
    }
};
#else
// ---------------------------------------------------------------------------
// brpc implementation (bazel mode): kvtest_control::KvtestControl::Stub over a brpc::Channel.
// Channels are cached per host:port (brpc::Channel is thread-safe, so one per
// peer is shared across notify pool threads).
// ---------------------------------------------------------------------------
class BrpcPeerClient : public PeerControlClient {
public:
    ~BrpcPeerClient() override = default;

    void Notify(const std::string &host, int port, const std::string &action,
                int sender, const std::vector<std::string> &keys, uint64_t size) override {
        try {
            brpc::Channel *chan = GetOrCreateChannel(host, port);
            if (chan == nullptr) {
                // Channel Init failure is one-shot per peer (logged inside
                // GetOrCreateChannel); skip the per-call WARN here.
                return;
            }
            kvtest_control::NotifyReq req;
            req.set_action(action);
            req.set_sender(sender);
            *req.mutable_keys() = {keys.begin(), keys.end()};
            req.set_size(size);

            kvtest_control::NotifyResp resp;
            brpc::Controller cntl;
            // No retry: notify is fire-and-forget best-effort (matches the
            // httplib path's single-shot Post). Retrying a down peer just
            // amplifies the [R1][R2][R3] noise without helping recovery.
            cntl.set_max_retry(0);
            kvtest_control::KvtestControl::Stub stub(chan);
            // done=NULL = SYNCHRONOUS call: stub.Notify blocks until the
            // response arrives (or timeout), then returns. A non-null done
            // makes it async — the response callback fires later in a bthread
            // and would touch these stack-local cntl/resp after Notify()
            // returns (use-after-free -> EndRPC check failures + heap
            // corruption). Stack locals are safe only under the sync path.
            stub.Notify(&cntl, &req, &resp, /*done=*/nullptr);
            // De-dup WARNs per peer: log the first failure once, suppress
            // repeats until the peer recovers, then log a single INFO. This
            // keeps startup-race noise (writer notifying before readers are
            // up) from flooding the log every round.
            const std::string key = host + ":" + std::to_string(port);
            if (cntl.Failed()) {
                std::string err = cntl.ErrorText();
                bool firstFailure;
                {
                    std::lock_guard<kvtest::mutex> lock(mu_);
                    firstFailure = warned_peers_.insert(key).second;
                }
                if (firstFailure) {
                    SLOG_WARN("Notify RPC to " << key << " failed: " << err
                              << " (further failures suppressed until recovery)");
                }
            } else {
                bool wasWarned;
                {
                    std::lock_guard<kvtest::mutex> lock(mu_);
                    wasWarned = warned_peers_.erase(key) > 0;
                }
                if (wasWarned) {
                    SLOG_INFO("Notify to " << key << " recovered");
                }
            }
        } catch (...) {
        }
    }

    bool Stop(const std::string &host, int port) override {
        try {
            brpc::Channel *chan = GetOrCreateChannel(host, port);
            if (chan == nullptr) return false;
            kvtest_control::StopReq req;
            kvtest_control::StopResp resp;
            brpc::Controller cntl;
            kvtest_control::KvtestControl::Stub stub(chan);
            stub.Stop(&cntl, &req, &resp, /*done=*/nullptr);
            return !cntl.Failed();
        } catch (...) {
            return false;
        }
    }

private:
    brpc::Channel *GetOrCreateChannel(const std::string &host, int port) {
        std::string key = host + ":" + std::to_string(port);
        std::lock_guard<kvtest::mutex> lock(mu_);
        auto it = channels_.find(key);
        if (it != channels_.end()) return it->second.get();
        auto chan = std::make_unique<brpc::Channel>();
        brpc::ChannelOptions opts;
        opts.timeout_ms = 2000;
        opts.connection_group = "kvtest-peer";
        if (chan->Init(host.c_str(), port, &opts) != 0) {
            SLOG_WARN("brpc channel Init failed for " << host << ":" << port);
            return nullptr;
        }
        auto *raw = chan.get();
        channels_[key] = std::move(chan);
        return raw;
    }

    // Protects channels_ and warned_peers_. Acquired from BrpcPeerClient::
    // Notify (notifyPool_ worker, bthread in bazel mode) and from Stop
    // (stop.cpp std::thread batch). kvtest::mutex is bthread::Mutex in bazel
    // mode so a bthread worker blocks on contention without holding a pthread;
    // also works from the pthread Stop callers since bthread::Mutex is
    // pthread-compatible.
    kvtest::mutex mu_;
    std::unordered_map<std::string, std::unique_ptr<brpc::Channel>> channels_;
    // Peers whose Notify is currently in a failed state. First failure logs a
    // WARN; subsequent failures are suppressed; recovery logs INFO + erases.
    std::unordered_set<std::string> warned_peers_;
};
#endif

std::unique_ptr<PeerControlClient> MakePeerControlClient() {
#ifdef KVTEST_USE_BRPC
    return std::make_unique<BrpcPeerClient>();
#else
    return std::make_unique<HttpPeerClient>();
#endif
}
