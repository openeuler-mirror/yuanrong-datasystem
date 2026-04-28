#include "stop.h"
#include "httplib.h"
#include "simple_log.h"
#include <algorithm>
#include <thread>
#include <vector>
#include <atomic>

static void StopOnePeer(const std::string &peerUrl, std::atomic<int> &successCount) {
    try {
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
        int port = std::stoi(hostPort.substr(colonPos + 1));

        httplib::Client cli(host, port);
        cli.set_connection_timeout(5);
        cli.set_read_timeout(5);

        auto res = cli.Post("/stop");
        if (res && res->status == 200) {
            SLOG_INFO(peerUrl << " -> OK");
            successCount++;
        } else {
            SLOG_WARN(peerUrl << " -> ERROR (status="
                      << (res ? std::to_string(res->status) : "no response") << ")");
        }
    } catch (const std::exception &e) {
        SLOG_WARN(peerUrl << " -> EXCEPTION: " << e.what());
    }
}

int StopAllPeers(const std::vector<std::string> &peers) {
    SLOG_INFO("Stopping " << peers.size() << " peers...");
    std::atomic<int> successCount{0};
    constexpr int kMaxConcurrent = 16;
    size_t idx = 0;

    while (idx < peers.size()) {
        int batchSize = std::min(kMaxConcurrent,
                                 static_cast<int>(peers.size() - idx));
        std::vector<std::thread> batch;
        for (int i = 0; i < batchSize; i++) {
            batch.emplace_back(StopOnePeer,
                               std::cref(peers[idx + i]),
                               std::ref(successCount));
        }
        for (auto &t : batch) t.join();
        idx += batchSize;
    }

    SLOG_INFO("Stop result: " << successCount.load() << "/" << peers.size() << " succeeded");
    return successCount.load();
}
