#include "stop.h"
#include "httplib.h"
#include <spdlog/spdlog.h>
#include <thread>
#include <vector>
#include <atomic>

static void StopOnePeer(const std::string &peerUrl, std::atomic<int> &successCount) {
    try {
        std::string hostPort = peerUrl;
        if (hostPort.substr(0, 7) == "http://") hostPort = hostPort.substr(7);

        auto colonPos = hostPort.find(':');
        if (colonPos == std::string::npos) {
            spdlog::warn("Invalid peer URL: {}", peerUrl);
            return;
        }

        std::string host = hostPort.substr(0, colonPos);
        int port = std::stoi(hostPort.substr(colonPos + 1));

        httplib::Client cli(host, port);
        cli.set_connection_timeout(5);
        cli.set_read_timeout(5);

        auto res = cli.Post("/stop");
        if (res && res->status == 200) {
            spdlog::info("{} -> OK", peerUrl);
            successCount++;
        } else {
            spdlog::warn("{} -> ERROR (status={})", peerUrl,
                         res ? std::to_string(res->status) : "no response");
        }
    } catch (const std::exception &e) {
        spdlog::warn("{} -> EXCEPTION: {}", peerUrl, e.what());
    }
}

int StopAllPeers(const std::vector<std::string> &peers) {
    spdlog::info("Stopping {} peers...", peers.size());
    std::atomic<int> successCount{0};
    std::vector<std::thread> threads;

    for (auto &peer : peers) {
        threads.emplace_back(StopOnePeer, std::cref(peer), std::ref(successCount));
    }

    for (auto &t : threads) t.join();

    spdlog::info("Stop result: {}/{} succeeded", successCount.load(), peers.size());
    return successCount.load();
}
