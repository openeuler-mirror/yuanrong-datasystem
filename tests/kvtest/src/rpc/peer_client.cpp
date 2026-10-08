#include "peer_client.h"

#include "common/bthread_compat.h"
#include "common/simple_log.h"
#include "vendor/nlohmann_json.hpp"

#include <unordered_map>
#include <utility>

#ifdef KVTEST_USE_BRPC
#include "kvtest_control.pb.h"

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <memory>
#include <random>
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
    PeerNotifySkipCounts GetNotifySkipCounts() const override { return {}; }

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

    PeerNotifySkipCounts GetNotifySkipCounts() const override {
        return {cooldownSkipped_.load(std::memory_order_relaxed),
                recoveryProbeSkipped_.load(std::memory_order_relaxed)};
    }

    void Notify(const std::string &host, int port, const std::string &action,
                int sender, const std::vector<std::string> &keys, uint64_t size) override {
        const std::string key = host + ":" + std::to_string(port);
        const bool warmupDone = action == "warmup_done";
        ChannelLease lease;
        bool oneShot = false;
        try {
            // Warmup completion is sent once. Give it a real attempt even if
            // ordinary notifications to this peer are cooling down.
            lease = GetOrCreateChannel(host, port, key);
            if (warmupDone && !lease.channel) {
                // Bypass a cached failed socket as well as the per-peer cooldown.
                lease = {CreateChannel(host, port, "kvtest-peer-priority"), 0};
                oneShot = true;
            }
            if (!lease.channel) {
                if (!warmupDone) {
                    if (lease.skipReason == SkipReason::Cooldown) {
                        cooldownSkipped_.fetch_add(1, std::memory_order_relaxed);
                    } else if (lease.skipReason == SkipReason::RecoveryProbe) {
                        recoveryProbeSkipped_.fetch_add(1, std::memory_order_relaxed);
                    }
                }
                if (oneShot) SLOG_WARN("brpc channel Init failed for " << key);
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
            kvtest_control::KvtestControl::Stub stub(lease.channel.get());
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
            if (cntl.Failed()) {
                if (oneShot) {
                    SLOG_WARN("Notify RPC to " << key << " failed: " << cntl.ErrorText());
                } else {
                    RecordFailure(key, lease, cntl.ErrorText().c_str());
                }
            } else if (!oneShot) {
                RecordSuccess(key, lease);
            }
        } catch (...) {
            if (!oneShot && lease.channel) {
                RecordFailure(key, lease, "unexpected exception");
            }
        }
    }

    bool Stop(const std::string &host, int port) override {
        try {
            auto chan = CreateChannel(host, port, "kvtest-peer-stop");
            if (!chan) {
                SLOG_WARN("brpc channel Init failed for " << host << ":" << port);
                return false;
            }
            kvtest_control::StopReq req;
            kvtest_control::StopResp resp;
            brpc::Controller cntl;
            kvtest_control::KvtestControl::Stub stub(chan.get());
            stub.Stop(&cntl, &req, &resp, /*done=*/nullptr);
            return !cntl.Failed();
        } catch (...) {
            return false;
        }
    }

private:
    using Clock = std::chrono::steady_clock;

    enum class SkipReason { None, Cooldown, RecoveryProbe };

    struct ChannelLease {
        std::shared_ptr<brpc::Channel> channel;
        uint64_t generation = 0;
        SkipReason skipReason = SkipReason::None;
    };

    struct PeerState {
        std::shared_ptr<brpc::Channel> channel;
        Clock::time_point nextProbeAt{};
        uint64_t generation = 0;
        unsigned failureStreak = 0;
        bool probeInFlight = false;
        bool warned = false;
    };

    static std::shared_ptr<brpc::Channel> CreateChannel(const std::string &host, int port,
                                                        const char *connectionGroup = "kvtest-peer") {
        auto chan = std::make_shared<brpc::Channel>();
        brpc::ChannelOptions opts;
        opts.timeout_ms = 2000;
        opts.connection_group = connectionGroup;
        if (chan->Init(host.c_str(), port, &opts) != 0) {
            return nullptr;
        }
        return chan;
    }

    ChannelLease GetOrCreateChannel(const std::string &host, int port, const std::string &key) {
        uint64_t generation;
        {
            std::unique_lock<kvtest::mutex> lock(mu_);
            for (;;) {
                auto &peer = peers_[key];
                if (peer.failureStreak == 0) {
                    // A healthy Channel can serve concurrent RPCs even while
                    // the first RPC is still waiting for its response.
                    if (peer.channel) return {peer.channel, peer.generation};
                    if (peer.probeInFlight) {
                        // Channel creation is in progress; preserve this
                        // notification instead of dropping it at startup.
                        channelReady_.wait(lock);
                        continue;
                    }
                } else {
                    if (peer.probeInFlight) return {nullptr, 0, SkipReason::RecoveryProbe};
                    if (Clock::now() < peer.nextProbeAt) return {nullptr, 0, SkipReason::Cooldown};
                }
                peer.probeInFlight = true;
                generation = ++peer.generation;
                break;
            }
        }

        std::shared_ptr<brpc::Channel> chan;
        try {
            chan = CreateChannel(host, port);
        } catch (...) {
            RecordFailure(key, {nullptr, generation}, "brpc channel creation threw");
            return {};
        }
        if (!chan) {
            RecordFailure(key, {nullptr, generation}, "brpc channel Init failed");
            return {};
        }
        bool published = false;
        {
            std::lock_guard<kvtest::mutex> lock(mu_);
            auto it = peers_.find(key);
            if (it != peers_.end() && it->second.generation == generation && it->second.probeInFlight) {
                it->second.channel = chan;
                published = true;
            }
        }
        if (published) {
            // Wake initial-creation waiters before the first RPC finishes.
            channelReady_.notify_all();
            return {std::move(chan), generation};
        }
        return {};
    }

    // Borrow the error text so an allocation failure cannot prevent the
    // in-flight probe from being cleared. Logging happens after state cleanup.
    void RecordFailure(const std::string &key, const ChannelLease &lease, const char *error) {
        bool firstFailure = false;
        std::shared_ptr<brpc::Channel> retired;
        {
            std::lock_guard<kvtest::mutex> lock(mu_);
            auto it = peers_.find(key);
            if (it == peers_.end() || it->second.generation != lease.generation ||
                it->second.channel != lease.channel) {
                return;
            }
            auto &peer = it->second;
            retired = std::move(peer.channel);
            ++peer.generation;
            peer.probeInFlight = false;
            peer.failureStreak = std::min(peer.failureStreak + 1, 6u);
            static constexpr int backoffMs[] = {2000, 4000, 8000, 16000, 32000, 60000};
            int baseMs = backoffMs[peer.failureStreak - 1];
            std::uniform_int_distribution<int> jitter(baseMs * 4 / 5, baseMs);
            peer.nextProbeAt = Clock::now() + std::chrono::milliseconds(jitter(rng_));
            firstFailure = !peer.warned;
            peer.warned = true;
        }
        channelReady_.notify_all();
        if (firstFailure) {
            SLOG_WARN("Notify RPC to " << key << " failed: " << error
                      << " (further failures suppressed until recovery)");
        }
    }

    void RecordSuccess(const std::string &key, const ChannelLease &lease) {
        bool wasWarned = false;
        {
            std::lock_guard<kvtest::mutex> lock(mu_);
            auto it = peers_.find(key);
            if (it == peers_.end() || it->second.generation != lease.generation ||
                it->second.channel != lease.channel) {
                return;
            }
            auto &peer = it->second;
            peer.failureStreak = 0;
            peer.probeInFlight = false;
            peer.nextProbeAt = {};
            wasWarned = peer.warned;
            peer.warned = false;
        }
        if (wasWarned) SLOG_INFO("Notify to " << key << " recovered");
    }

    // mu_ protects peer state and the jitter generator.
    kvtest::mutex mu_;
    kvtest::condition_variable channelReady_;
    std::unordered_map<std::string, PeerState> peers_;
    std::mt19937 rng_{std::random_device{}()};
    std::atomic<uint64_t> cooldownSkipped_{0};
    std::atomic<uint64_t> recoveryProbeSkipped_{0};
};
#endif

std::unique_ptr<PeerControlClient> MakePeerControlClient() {
#ifdef KVTEST_USE_BRPC
    return std::make_unique<BrpcPeerClient>();
#else
    return std::make_unique<HttpPeerClient>();
#endif
}
