#include "stop.h"
#include "rpc/peer_client.h"
#include "common/simple_log.h"
#include <algorithm>
#include <thread>
#include <vector>
#include <atomic>

// StopOnePeer: send a Stop RPC to one peer via the build-system-selected
// transport (httplib POST /stop in cmake mode, KvtestControl::Stop in bazel
// mode). The PeerControlClient is shared across the concurrent batch threads;
// both impls are thread-safe.
static void StopOnePeer(PeerControlClient &client, const std::string &peerUrl,
                        std::atomic<int> &successCount) {
    std::string hostPort = peerUrl;
    if (hostPort.size() > 7 && hostPort.compare(0, 7, "http://") == 0) {
        hostPort = hostPort.substr(7);
    }
    auto colonPos = hostPort.find(':');
    if (colonPos == std::string::npos) {
        SLOG_WARN("Invalid peer URL: " << peerUrl);
        return;
    }
    std::string host = hostPort.substr(0, colonPos);
    try {
        int port = std::stoi(hostPort.substr(colonPos + 1));
        if (client.Stop(host, port)) {
            SLOG_INFO(peerUrl << " -> OK");
            successCount++;
        } else {
            SLOG_WARN(peerUrl << " -> ERROR (no response or RPC failed)");
        }
    } catch (const std::exception &e) {
        SLOG_WARN(peerUrl << " -> EXCEPTION: " << e.what());
    }
}

int StopAllPeers(const std::vector<std::string> &peers) {
    SLOG_INFO("Stopping " << peers.size() << " peers...");
    auto client = MakePeerControlClient();
    std::atomic<int> successCount{0};
    constexpr int kMaxConcurrent = 16;
    size_t idx = 0;

    while (idx < peers.size()) {
        int batchSize = std::min(kMaxConcurrent,
                                 static_cast<int>(peers.size() - idx));
        std::vector<std::thread> batch;
        for (int i = 0; i < batchSize; i++) {
            batch.emplace_back(StopOnePeer, std::ref(*client),
                               std::cref(peers[idx + i]),
                               std::ref(successCount));
        }
        for (auto &t : batch) t.join();
        idx += batchSize;
    }

    SLOG_INFO("Stop result: " << successCount.load() << "/" << peers.size() << " succeeded");
    return successCount.load();
}
