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
      server_(std::make_unique<httplib::Server>()) {
    // Build notify pipeline ops from config
    for (auto &name : cfg_.notifyPipeline) {
        auto fn = GetOpFunc(name);
        if (!fn) {
            SLOG_WARN("Unknown notify_pipeline op: " << name << ", skipping");
            continue;
        }
        notifyOps_.emplace_back(name, fn);
    }
}

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

void HttpServer::HandleNotify(const std::string &body) {
    try {
        json j = json::parse(body);
        std::string key = j["key"];
        int sender = j["sender"];
        uint64_t expectedSize = j["size"];

        {
            std::lock_guard<std::mutex> lock(notifyMutex_);
            constexpr size_t kMaxNotifyThreads = 100;
            if (notifyThreads_.size() >= kMaxNotifyThreads) {
                for (auto &t : notifyThreads_) {
                    if (t.joinable()) t.join();
                }
                notifyThreads_.clear();
            }
            notifyThreads_.emplace_back([this, key, sender, expectedSize]() {
                PipelineContext ctx;
                ctx.key = key;
                ctx.size = expectedSize;
                ctx.senderId = sender;
                ctx.data = GeneratePatternData(expectedSize, sender);
                ctx.client = client_;
                ctx.param.writeMode = WriteMode::NONE_L2_CACHE;
                ctx.param.ttlSecond = cfg_.ttlSeconds;

                ExecutePipeline(notifyOps_, ctx, metrics_,
                                metrics_.VerifyFailCounter());
            });
        }

    } catch (const std::exception &e) {
        SLOG_WARN("Parse notify body failed: " << e.what());
    }
}
