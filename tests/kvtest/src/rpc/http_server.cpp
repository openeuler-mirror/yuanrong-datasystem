#include "http_server.h"
#include "common/simple_log.h"
#include "vendor/nlohmann_json.hpp"
#include <thread>

using json = nlohmann::json;

HttpServer::HttpServer(const Config &cfg, std::shared_ptr<datasystem::KVClient> client,
                       MetricsCollector &metrics, std::atomic<bool> &running)
    : cfg_(cfg), running_(running), metrics_(metrics),
      dispatcher_(cfg, std::move(client), metrics),
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
    dispatcher_.Stop();
}

void HttpServer::HandleNotify(const std::string &body) {
    // Decode the JSON transport only; the notify pool / cache callbacks live
    // in NotifyDispatcher so the brpc path reuses the same semantics.
    try {
        json j = json::parse(body);
        std::string action = j.value("action", "");

        std::vector<std::string> keys;
        if (j.contains("keys") && j["keys"].is_array()) {
            for (auto &k : j["keys"]) keys.push_back(k.get<std::string>());
        } else if (j.contains("key")) {
            keys.push_back(j["key"].get<std::string>());
        }

        int sender = j.value("sender", 0);
        uint64_t size = j.value("size", 0ULL);

        dispatcher_.DispatchNotify(action, sender, keys, size);
    } catch (const std::exception &e) {
        SLOG_WARN("Parse notify body failed: " << e.what());
    }
}
