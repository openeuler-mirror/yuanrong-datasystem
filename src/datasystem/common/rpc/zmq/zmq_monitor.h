/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Zmq Monitor
 */

#ifndef DATASYSTEM_COMMON_RPC_ZMQ_MONITOR_H
#define DATASYSTEM_COMMON_RPC_ZMQ_MONITOR_H

#include <chrono>
#include <condition_variable>
#include <mutex>

#include "datasystem/common/rpc/zmq/zmq_context.h"
#include "datasystem/common/rpc/zmq/zmq_socket.h"
#include "datasystem/common/util/thread.h"

namespace datasystem {
class ZmqMonitor {
public:
    explicit ZmqMonitor(const std::shared_ptr<ZmqContext> &ctx);
    virtual ~ZmqMonitor();

    /**
     * Stop monitoring
     */
    void Stop();

    /**
     * Start monitoring
     */
    Status StartMonitoring();

    /**
     * Add a ZmqSocket to be monitored
     */
    Status AddZmqSocket(ZmqSocket &sock);

protected:
    std::atomic<bool> interrupt_;
    std::atomic<bool> initialize_;
    struct Event {
        uint16_t event_;  // id of the event as bitfield
        int32_t value_;   // value is either error code, fd or reconnect interval
    };

    virtual void OnEventConnected(const Event &t, const std::string &addr, const std::string &gatewayId);
    virtual void OnEventConnectDelayed(const Event &t, const std::string &addr, const std::string &gatewayId);
    virtual void OnEventConnectRetried(const Event &t, const std::string &addr, const std::string &gatewayId);
    virtual void OnEventListening(const Event &t, const std::string &addr, const std::string &gatewayId);
    virtual void OnEventBindFailed(const Event &t, const std::string &addr, const std::string &gatewayId);
    virtual void OnEventAccepted(const Event &t, const std::string &addr, const std::string &gatewayId);
    virtual void OnEventAcceptFailed(const Event &t, const std::string &addr, const std::string &gatewayId);
    virtual void OnEventClosed(const Event &t, const std::string &addr, const std::string &gatewayId);
    virtual void OnEventCloseFailed(const Event &t, const std::string &addr, const std::string &gatewayId);
    virtual void OnEventDisconnected(const Event &t, const std::string &addr, const std::string &gatewayId);
    virtual void OnEventHandshakeFailedNoDetail(const Event &t, const std::string &addr, const std::string &gatewayId);
    virtual void OnEventHandshakeFailedProtocol(const Event &t, const std::string &addr, const std::string &gatewayId);
    virtual void OnEventHandshakeFailedAuth(const Event &t, const std::string &addr, const std::string &gatewayId);
    virtual void OnEventHandshakeSucceeded(const Event &t, const std::string &addr, const std::string &gatewayId);
    virtual void OnEventUnknown(const Event &t, const std::string &addr, const std::string &gatewayId);

private:
    mutable std::mutex mux_;
    mutable std::condition_variable cv_;
    std::weak_ptr<ZmqContext> ctx_;
    Thread thrd_;
    std::unordered_map<void *, std::pair<std::string, bool>> monitorMap_;
    decltype(monitorMap_) shadowCopy_;  // Snapshot without any lock
    std::map<std::pair<std::string, int>, std::chrono::steady_clock::time_point> lastReportEvent_;
    bool mapChanged_;
    std::set<void *> pendingClose_;
    std::set<void *> errorState_;
    Status CheckEvent(void *handle, Event &t, std::string &addr);
    void ReportEvent(const Event &t, const std::string &addr, const std::string &gatewayId, bool &inError);
    void ReportErrorEvent(const Event &t, const std::string &addr, const std::string &gatewayId, bool &inError);
    Status Run(std::unique_ptr<zmq_pollitem_t[]> &items);
    bool InitPollItems(std::unique_ptr<zmq_pollitem_t[]> &items, size_t &sz);
    void SyncMonitorMap();
};
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_RPC_ZMQ_MONITOR_H
