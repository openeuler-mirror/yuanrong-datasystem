#pragma once

#include <cstdio>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "vendor/httplib.h"
#include "vendor/nlohmann_json.hpp"

namespace kvtest {

// Runtime config-update HTTP endpoint for the standalone deployment test
// programs (coordinator_test / worker_test). It exposes POST /update_config
// (body = JSON object) and relays the body to the role-specific UpdateConfig
// entry, returning {"code": <StatusCode>, "message": <Status::ToString()>}.
//
// Uses the vendored header-only httplib + nlohmann_json (already pulled in via
// jf_service_discovery.h for both build backends), so it introduces no new
// third-party dependency and does not touch the SDK's own /health service.
class ConfigUpdateHttpServer {
public:
    // handler receives the raw request body and returns the JSON response.
    using UpdateHandler = std::function<std::string(const std::string &configJson)>;

    ConfigUpdateHttpServer(int port, UpdateHandler handler)
        : port_(port), handler_(std::move(handler))
    {
    }

    ~ConfigUpdateHttpServer() { Stop(); }

    ConfigUpdateHttpServer(const ConfigUpdateHttpServer &) = delete;
    ConfigUpdateHttpServer &operator=(const ConfigUpdateHttpServer &) = delete;

    // Start a listen thread. port_ <= 0 disables the endpoint (no-op).
    bool Start()
    {
        if (port_ <= 0) {
            return true;
        }
        server_ = std::make_unique<httplib::Server>();

        server_->Get("/healthz", [](const httplib::Request &, httplib::Response &res) {
            res.status = 200;
            res.set_content("ok", "text/plain");
        });

        server_->Post("/update_config", [this](const httplib::Request &req, httplib::Response &res) {
            res.status = 200;
            res.set_content(Handle(req.body), "application/json");
        });

        thread_ = std::thread([this]() {
            fprintf(stderr, "config update HTTP server listening on port %d\n", port_);
            if (!server_->listen("0.0.0.0", port_)) {
                fprintf(stderr, "config update HTTP server failed on port %d\n", port_);
            }
        });
        return true;
    }

    void Stop()
    {
        if (server_ != nullptr) {
            server_->stop();
        }
        if (thread_.joinable()) {
            thread_.join();
        }
    }

    int Port() const { return port_; }

private:
    std::string Handle(const std::string &body)
    {
        // Serialize handlers: DataWorker::UpdateConfig is not thread-safe and
        // concurrent updates would break DynamicFlagConfig's batch atomicity.
        std::lock_guard<std::mutex> lock(handlerMutex_);
        try {
            return handler_(body);
        } catch (const std::exception &e) {
            nlohmann::json resp = {
                { "code", -1 },
                { "message", std::string("config update handler threw: ") + e.what() }
            };
            return resp.dump();
        } catch (...) {
            nlohmann::json resp = { { "code", -1 }, { "message", "config update handler threw unknown exception" } };
            return resp.dump();
        }
    }

    int port_;
    UpdateHandler handler_;
    std::unique_ptr<httplib::Server> server_;
    std::thread thread_;
    std::mutex handlerMutex_;
};

// Convenience helper for role-side handlers.
inline std::string BuildUpdateConfigResponse(uint32_t code, const std::string &message)
{
    nlohmann::json resp = { { "code", code }, { "message", message } };
    return resp.dump();
}

}  // namespace kvtest