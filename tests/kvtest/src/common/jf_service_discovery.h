#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdio>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "datasystem/utils/coordinator_discovery.h"
#include "datasystem/utils/status.h"

#include "vendor/httplib.h"
#include "vendor/nlohmann_json.hpp"

namespace kvtest {

using json = nlohmann::json;

class JfClient {
public:
    explicit JfClient(const std::string &jfServerAddr, int defaultTtl = 30)
        : jfAddr_(jfServerAddr), defaultTtl_(defaultTtl)
    {
    }

    ~JfClient()
    {
        StopAllHeartbeats();
    }

    JfClient(const JfClient &) = delete;
    JfClient &operator=(const JfClient &) = delete;

    datasystem::Status RegisterService(const std::string &serviceName, int port)
    {
        auto rc = SendRegistration(serviceName, port);
        if (!rc.IsOk()) {
            return rc;
        }
        StartHeartbeat(serviceName, DetectLocalIp(), port);
        return datasystem::Status::OK();
    }

    datasystem::Status UnregisterService(const std::string &serviceName, int port)
    {
        std::string ip = DetectLocalIp();
        StopHeartbeat(serviceName, ip, port);
        json body = { { "service", serviceName }, { "port", port } };
        std::string resp;
        auto rc = HttpPost("/unregister", body.dump(), resp);
        if (!rc.IsOk())
            return rc;
        return datasystem::Status::OK();
    }

    datasystem::Status GetInstance(const std::string &serviceName, std::vector<std::string> &instances)
    {
        std::string resp;
        auto rc = HttpGet("/discover/" + serviceName, resp);
        if (!rc.IsOk())
            return rc;
        try {
            auto j = json::parse(resp);
            instances.clear();
            for (auto &addr : j["instances"]) {
                instances.push_back(addr.get<std::string>());
            }
        } catch (const std::exception &e) {
            return datasystem::Status(datasystem::K_RUNTIME_ERROR,
                                      std::string("JF discover response parse error: ") + e.what());
        }
        return datasystem::Status::OK();
    }

private:
    // Registration request only, no heartbeat start: ReRegister calls this
    // so an in-flight recovery cannot spawn a replacement heartbeat thread
    // for a key StopHeartbeat has already erased (unregister stays terminal).
    datasystem::Status SendRegistration(const std::string &serviceName, int port)
    {
        json body = { { "service", serviceName }, { "port", port }, { "ttl", defaultTtl_ } };
        std::string resp;
        auto rc = HttpPost("/register", body.dump(), resp);
        if (!rc.IsOk())
            return rc;
        try {
            auto j = json::parse(resp);
            if (!j.value("ok", false)) {
                return datasystem::Status(datasystem::K_RUNTIME_ERROR,
                                          "JF register failed: " + j.value("error", "unknown"));
            }
        } catch (const std::exception &e) {
            return datasystem::Status(datasystem::K_RUNTIME_ERROR,
                                      std::string("JF register response parse error: ") + e.what());
        }
        return datasystem::Status::OK();
    }

    enum class HeartbeatState { OK, NOT_REGISTERED, UNREACHABLE, STOPPED };

    // Per-key stop signal; the thread holds a shared_ptr so join never
    // dereferences map storage after the entry is erased.
    struct HeartbeatHandle {
        std::atomic<bool> stopped{ false };
        std::thread thread;
    };

    void StartHeartbeat(const std::string &service, const std::string &ip, int port)
    {
        std::string key = service + ":" + ip + ":" + std::to_string(port);
        std::lock_guard<std::mutex> lock(mutex_);
        if (!running_.load() || heartbeatThreads_.count(key) > 0)
            return;
        auto handle = std::make_shared<HeartbeatHandle>();
        handle->thread = std::thread([this, service, port, handle]() { HeartbeatLoop(service, port, handle); });
        heartbeatThreads_[key] = std::move(handle);
    }

    // TTL expiry while stalled (heartbeat 404) or unreachable (3 failed
    // rounds) recovers via an idempotent re-register.
    void HeartbeatLoop(const std::string &service, int port, const std::shared_ptr<HeartbeatHandle> &handle)
    {
        // ttl/6 (was ttl/3) tolerates two consecutive missed heartbeats
        // before TTL expiry. On a 1-core host the leader is CPU-starved
        // by brpc + SHA-256 recovery + watch fan-out after winning
        // election; the heartbeat pthread can be preempted >10s, so the
        // old ttl/3=10s interval left zero slack after one slow round.
        int interval = defaultTtl_ / 6;
        if (interval < 1) {
            interval = 1;
        }
        int unreachableRounds = 0;
        bool reregisterReported = false;
        while (!handle->stopped.load() && running_.load()) {
            {
                std::unique_lock<std::mutex> lk(mutex_);
                if (cv_.wait_for(lk, std::chrono::seconds(interval),
                                 [this, handle] { return !running_.load() || handle->stopped.load(); })) {
                    break;
                }
            }
            auto state = SendHeartbeatRound(service, port, handle);
            if (state == HeartbeatState::STOPPED) {
                break;
            }
            if (state == HeartbeatState::OK) {
                unreachableRounds = 0;
                continue;
            }
            if (handle->stopped.load()) {
                break;
            }
            if (state == HeartbeatState::NOT_REGISTERED || ++unreachableRounds >= 3) {
                ReRegister(service, port, reregisterReported);
                unreachableRounds = 0;
            }
        }
    }

    // One round: the scheduled attempt plus (network errors only) an
    // immediate 1s retry. A 404 is not retried; the caller re-registers.
    HeartbeatState SendHeartbeatRound(const std::string &service, int port,
                                      const std::shared_ptr<HeartbeatHandle> &handle)
    {
        json body = { { "service", service }, { "port", port } };
        std::string resp;
        int httpStatus = 0;
        auto rc = HttpPost("/heartbeat", body.dump(), resp, &httpStatus);
        if (rc.IsOk()) {
            return HeartbeatState::OK;
        }
        if (httpStatus == 404) {
            return HeartbeatState::NOT_REGISTERED;
        }
        fprintf(stderr, "JF heartbeat failed for %s:%d: %s\n", service.c_str(), port, rc.ToString().c_str());
        {
            std::unique_lock<std::mutex> lk(mutex_);
            if (cv_.wait_for(lk, std::chrono::seconds(1),
                             [this, handle] { return !running_.load() || handle->stopped.load(); })) {
                return HeartbeatState::STOPPED;
            }
        }
        rc = HttpPost("/heartbeat", body.dump(), resp, &httpStatus);
        if (rc.IsOk()) {
            return HeartbeatState::OK;
        }
        if (httpStatus == 404) {
            return HeartbeatState::NOT_REGISTERED;
        }
        fprintf(stderr, "JF heartbeat retry failed for %s:%d: %s\n", service.c_str(), port, rc.ToString().c_str());
        return HeartbeatState::UNREACHABLE;
    }

    // Runs on the heartbeat thread with no mutex held; log per state change.
    void ReRegister(const std::string &service, int port, bool &reported)
    {
        if (!reported) {
            fprintf(stderr, "JF registration lost for %s:%d, re-registering\n", service.c_str(), port);
        }
        auto rc = SendRegistration(service, port);
        if (rc.IsOk()) {
            fprintf(stderr, "JF re-register succeeded for %s:%d\n", service.c_str(), port);
            reported = false;
        } else {
            if (!reported) {
                fprintf(stderr, "JF re-register failed for %s:%d: %s\n", service.c_str(), port,
                        rc.ToString().c_str());
            }
            reported = true;
        }
    }

    // Per-key stop (running_ untouched, so register-after-unregister works).
    // Join before the caller sends the remote unregister: on return no
    // heartbeat thread exists for the key, keeping unregister terminal.
    void StopHeartbeat(const std::string &service, const std::string &ip, int port)
    {
        std::string key = service + ":" + ip + ":" + std::to_string(port);
        std::shared_ptr<HeartbeatHandle> handle;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto it = heartbeatThreads_.find(key);
            if (it != heartbeatThreads_.end()) {
                handle = it->second;
                heartbeatThreads_.erase(it);
            }
        }
        if (handle == nullptr) {
            return;
        }
        handle->stopped.store(true);
        cv_.notify_all();
        if (handle->thread.joinable()) {
            handle->thread.join();
        }
    }

    void StopAllHeartbeats()
    {
        running_.store(false);
        cv_.notify_all();
        std::vector<std::shared_ptr<HeartbeatHandle>> handles;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            for (auto &pair : heartbeatThreads_) {
                handles.push_back(pair.second);
            }
            heartbeatThreads_.clear();
        }
        // Join outside mutex_: a heartbeat thread mid-ReRegister needs
        // mutex_ inside StartHeartbeat; joining while holding it deadlocks.
        for (auto &handle : handles) {
            if (handle->thread.joinable()) {
                handle->thread.join();
            }
        }
    }

    datasystem::Status HttpPost(const std::string &path, const std::string &body, std::string &resp,
                                int *statusCode = nullptr)
    {
        auto cli = CreateClient();
        if (!cli) {
            return datasystem::Status(datasystem::K_RUNTIME_ERROR, "Cannot connect to JF server: " + jfAddr_);
        }
        auto res = cli->Post(path, body, "application/json");
        if (!res) {
            if (statusCode != nullptr) {
                *statusCode = 0;
            }
            return datasystem::Status(datasystem::K_RUNTIME_ERROR, "JF POST " + path + " failed: no response");
        }
        if (statusCode != nullptr) {
            *statusCode = res->status;
        }
        if (res->status != 200) {
            return datasystem::Status(datasystem::K_RUNTIME_ERROR,
                                      "JF POST " + path + " returned status " + std::to_string(res->status));
        }
        resp = res->body;
        return datasystem::Status::OK();
    }

    datasystem::Status HttpGet(const std::string &path, std::string &resp)
    {
        auto cli = CreateClient();
        if (!cli) {
            return datasystem::Status(datasystem::K_RUNTIME_ERROR, "Cannot connect to JF server: " + jfAddr_);
        }
        auto res = cli->Get(path);
        if (!res) {
            return datasystem::Status(datasystem::K_RUNTIME_ERROR, "JF GET " + path + " failed: no response");
        }
        if (res->status != 200) {
            return datasystem::Status(datasystem::K_RUNTIME_ERROR,
                                      "JF GET " + path + " returned status " + std::to_string(res->status));
        }
        resp = res->body;
        return datasystem::Status::OK();
    }

    std::unique_ptr<httplib::Client> CreateClient()
    {
        auto pos = jfAddr_.find(':');
        std::string host = (pos != std::string::npos) ? jfAddr_.substr(0, pos) : jfAddr_;
        int port = (pos != std::string::npos) ? std::stoi(jfAddr_.substr(pos + 1)) : 80;
        auto cli = std::make_unique<httplib::Client>(host.c_str(), port);
        // Without explicit timeouts, httplib::Client can block on connect()
        // for tens of seconds when the JF server's listen backlog overflows
        // (2000-worker startup burst). The coordinator heartbeat thread
        // would then miss TTL and be expired. 2s connect / 5s read bounds
        // a single failed heartbeat round so the next interval (TTL/3)
        // can retry, instead of blocking past TTL.
        cli->set_connection_timeout(2);
        cli->set_read_timeout(5);
        return cli;
    }

    static std::string DetectLocalIp()
    {
        const char *podIp = std::getenv("POD_IP");
        if (podIp && *podIp)
            return podIp;
        const char *hostIp = std::getenv("HOST_IP");
        if (hostIp && *hostIp)
            return hostIp;
        return "127.0.0.1";
    }

    std::string jfAddr_;
    int defaultTtl_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::atomic<bool> running_{ true };
    std::map<std::string, std::shared_ptr<HeartbeatHandle>> heartbeatThreads_;
};

class UserCoordinatorDiscovery : public datasystem::ICoordinatorDiscovery {
public:
    UserCoordinatorDiscovery(std::shared_ptr<JfClient> jfClient, std::string serviceName)
        : jfClient_(std::move(jfClient)), serviceName_(std::move(serviceName))
    {
    }

    datasystem::Status GetCoordinators(std::vector<std::string> &serviceList) override
    {
        auto rc = jfClient_->GetInstance(serviceName_, serviceList);
        if (rc.IsOk() && serviceList.empty()) {
            // A distinct error code lets worker startup tell "registry
            // briefly empty" apart from transport failures and retry it.
            return datasystem::Status(datasystem::K_NOT_FOUND,
                                      "JF discovery returned no instances for service " + serviceName_);
        }
        return rc;
    }

private:
    std::shared_ptr<JfClient> jfClient_;
    std::string serviceName_;
};

}  // namespace kvtest
