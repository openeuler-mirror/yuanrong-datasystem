#include "http_server.h"
#include "pipeline/cache_reader.h"
#include "pipeline/data_pattern.h"
#include "vendor/httplib.h"
#include "common/simple_log.h"
#include "vendor/nlohmann_json.hpp"
#include <algorithm>
#include <thread>

using json = nlohmann::json;
using namespace datasystem;

HttpServer::HttpServer(const Config &cfg, std::shared_ptr<KVClient> client,
                       MetricsCollector &metrics, std::atomic<bool> &running)
    : cfg_(cfg), client_(client), metrics_(metrics), running_(running),
      server_(std::make_unique<httplib::Server>()),
      notifyPool_(100) {
    for (auto &name : cfg_.notifyPipeline) {
        auto fn = GetOpFunc(name);
        if (!fn) {
            SLOG_WARN("Unknown notify_pipeline op: " << name << ", skipping");
            continue;
        }
        notifyOps_.emplace_back(name, fn);
        if (name == kOpSetStringView || name == kOpMemoryCopy) {
            notifyNeedsData_ = true;
        }
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

    server_->Post("/summary", [this](const httplib::Request &, httplib::Response &res) {
        metrics_.WriteSummary();
        res.status = 200;
        res.set_content("ok", "text/plain");
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
    notifyPool_.Stop();
}

void HttpServer::HandleNotify(const std::string &body) {
    try {
        json j = json::parse(body);
        std::string action = j.value("action", "");

        // Cache mode: warmup_done notification
        if (action == "warmup_done" && cacheReader_) {
            int sender = j["sender"];
            std::vector<std::string> warmupKeys;
            if (j.contains("keys") && j["keys"].is_array()) {
                for (auto &k : j["keys"]) warmupKeys.push_back(k.get<std::string>());
            }
            cacheReader_->OnWarmupDone(sender, warmupKeys);
            return;
        }

        // Parse keys for normal or evict notifications
        std::vector<std::string> keys;
        if (j.contains("keys") && j["keys"].is_array()) {
            for (auto &k : j["keys"]) keys.push_back(k.get<std::string>());
        } else if (j.contains("key")) {
            keys.push_back(j["key"].get<std::string>());
        }
        if (keys.empty()) return;

        // Cache mode: evict keys -> CacheReader
        if (cacheReader_) {
            cacheReader_->OnEvictKeys(keys);
            return;
        }

        // Non-cache mode: original notifyPipeline logic
        int sender = j["sender"];
        uint64_t expectedSize = j["size"];

        notifyPool_.Submit([this, keys = std::move(keys), sender, expectedSize]() {
            PipelineContext ctx;
            ctx.key = keys[0];
            ctx.batchKeys = keys;
            ctx.size = expectedSize;
            ctx.senderId = sender;
            if (notifyNeedsData_) {
                auto cacheKey = std::to_string(expectedSize) + "_" + std::to_string(sender);
                {
                    std::lock_guard<std::mutex> lock(pregenMutex_);
                    auto it = pregenData_.find(cacheKey);
                    if (it != pregenData_.end()) {
                        ctx.data = it->second;
                    } else {
                        ctx.data = GeneratePatternData(expectedSize, sender);
                        pregenData_[cacheKey] = ctx.data;
                    }
                }
            }
            ctx.client = client_;
            ctx.param.writeMode = WriteMode::NONE_L2_CACHE_EVICT;
            ctx.param.ttlSecond = cfg_.ttlSeconds;
            ctx.verifyFailCount = &metrics_.VerifyFailCounter();

            ExecutePipeline(notifyOps_, ctx, metrics_,
                            metrics_.VerifyFailCounter());
        });

    } catch (const std::exception &e) {
        SLOG_WARN("Parse notify body failed: " << e.what());
    }
}
