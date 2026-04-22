#include "http_server.h"
#include "data_pattern.h"
#include "httplib.h"
#include "simple_log.h"
#include <nlohmann/json.hpp>
#include <chrono>
#include <thread>

using json = nlohmann::json;
using namespace datasystem;

HttpServer::HttpServer(const Config &cfg, std::shared_ptr<KVClient> client,
                       MetricsCollector &metrics, std::atomic<bool> &running)
    : cfg_(cfg), client_(client), metrics_(metrics), running_(running),
      server_(std::make_unique<httplib::Server>()) {}

HttpServer::~HttpServer() { Stop(); }

void HttpServer::Start() {
    server_->Post("/notify", [this](const httplib::Request &req, httplib::Response &res) {
        HandleNotify(req.body);
        res.status = 200;
        res.set_content("ok", "text/plain");
    });

    server_->Get("/stats", [this](const httplib::Request &, httplib::Response &res) {
        res.status = 200;
        res.set_content(metrics_.GetStatsJson(), "application/json");
    });

    server_->Post("/stop", [this](const httplib::Request &, httplib::Response &res) {
        SLOG_INFO("Received /stop request");
        res.status = 200;
        res.set_content("stopping", "text/plain");
        running_ = false;
    });

    serverThread_ = std::thread([this]() {
        SLOG_INFO("HTTP server listening on port " << cfg_.listenPort);
        if (!server_->listen("0.0.0.0", cfg_.listenPort)) {
            SLOG_ERROR("Failed to start HTTP server on port " << cfg_.listenPort);
        }
    });
}

void HttpServer::Stop() {
    if (server_) server_->stop();
    if (serverThread_.joinable()) serverThread_.join();
    {
        std::lock_guard<std::mutex> lock(notifyMutex_);
        for (auto &t : notifyThreads_) {
            if (t.joinable()) t.join();
        }
        notifyThreads_.clear();
    }
}

std::string HttpServer::GenerateExpectedData(uint64_t size, int senderId) {
    return GeneratePatternData(size, senderId);
}

void HttpServer::HandleNotify(const std::string &body) {
    try {
        json j = json::parse(body);
        std::string key = j["key"];
        int sender = j["sender"];
        uint64_t expectedSize = j["size"];

        {
            std::lock_guard<std::mutex> lock(notifyMutex_);
            // Join old threads when exceeding limit to prevent unbounded growth
            constexpr size_t kMaxNotifyThreads = 100;
            if (notifyThreads_.size() >= kMaxNotifyThreads) {
                for (auto &t : notifyThreads_) {
                    if (t.joinable()) t.join();
                }
                notifyThreads_.clear();
            }
            notifyThreads_.emplace_back([this, key, sender, expectedSize]() {
                std::string val;
                auto start = std::chrono::steady_clock::now();
                Status rc = client_->Get(key, val);
                auto end = std::chrono::steady_clock::now();
                double latencyMs = std::chrono::duration<double, std::milli>(end - start).count();

                metrics_.Record("get", latencyMs, rc.IsOk());

                if (!rc.IsOk()) {
                    SLOG_WARN("Get failed: key=" << key << ", error=" << rc.GetMsg());
                    return;
                }

                if (val.size() != expectedSize) {
                    SLOG_WARN("Size mismatch: key=" << key << ", expected=" << expectedSize << ", got=" << val.size());
                    metrics_.RecordVerifyFail();
                    return;
                }

                std::string expected = GenerateExpectedData(expectedSize, sender);
                if (val != expected) {
                    SLOG_WARN("Content mismatch: key=" << key);
                    metrics_.RecordVerifyFail();
                }
            });
        }

    } catch (const std::exception &e) {
        SLOG_WARN("Parse notify body failed: " << e.what());
    }
}
