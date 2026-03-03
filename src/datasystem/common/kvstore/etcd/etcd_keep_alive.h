/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: Interface to etcd.
 */
#ifndef DATASYSTEM_COMMON_KVSTORE_ETCD_ETCD_LEASEKEEPALLIVE_H
#define DATASYSTEM_COMMON_KVSTORE_ETCD_ETCD_LEASEKEEPALLIVE_H

#include <condition_variable>
#include <mutex>
#include <thread>

#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/kvstore/etcd/grpc_session.h"
#include "datasystem/common/util/event_subscribers.h"
#include "datasystem/common/util/locks.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/util/timer.h"
#include "etcd/api/etcdserverpb/rpc.grpc.pb.h"

namespace datasystem {
struct EtcdResponseHeader {
    // ID of the cluster which sent the response.
    uint64_t clusterId = 0;

    // ID of the member which sent the response.
    uint64_t memberId = 0;

    // Key-value store revision when the request was applied. For watch progress
    // responses, the header.revision indicates progress. All future events received
    // in this stream are guaranteed to have a higher revision number than the header
    // revision number.
    int64_t revision = 0;

    // The raft term when the request was applied.
    uint64_t raftTerm = 0;
};

enum KeepAliveEventType : uint32_t {
    LIVENESS_HEALTH_CHECK,
};

using LivenessHealthCheckEvent =
    EventSubscribers<LIVENESS_HEALTH_CHECK, std::function<Status(Timer &, const Status &lastStatus)>>;

struct EtcdResponse {
    EtcdResponseHeader header;
    std::string name;
    std::string key;
    std::string value;
    int64_t leaseId = 0;
    int64_t createRevision = 0;
    int64_t modRevision = 0;
    int64_t version = 0;
    int64_t ttl = 0;
};

class EtcdKeepAlive {
public:
    /**
     * @brief Constructor
     * @param[in] address Etcd address
     * @param[in] leaseId lease id
     */
    EtcdKeepAlive(const std::string &address, int64_t leaseId);

    /**
     * @brief Destructor
     */
    ~EtcdKeepAlive();

    /**
     * @brief Shutdown keep-alive and exit everything.
     * @return K_OK on success; the error code otherwise.
     */
    Status Shutdown();

    /**
     * @brief Return lease id get from ETCD server.
     * @return Lease id get from ETCD server.
     */
    int64_t Lease() const
    {
        return leaseId_;
    }

    /**
     * @brief Init lease stub and stream client, connect to ETCD server.
     * @param[in] authToken Token for etcd server.
     * @return K_OK on success; the error code otherwise.
     */
    Status Init(const std::string &authToken);

    /**
     * @brief The main loop for periodically talking with ETCD server.
     * @return K_OK on success; the error code otherwise.
     */
    Status Run(Timer &keepAliveTimeoutTimer);

    /**
     * @brief Send keep-alive RPC request to ETCD server to keep alive.
     * @return RPC status and etcd response pair.
     */
    std::pair<Status, EtcdResponse> KeepAlive();

    /**
     * @brief Obtain the current value of FLAGS_node_timeout_s and convert it to milliseconds as the lease time for etcd
     * to retain the worker lease ID.
     * @note The value of FLAGS_node_timeout_s can be dynamically changed by O&M personnel during worker running.
     * @return int64_t - the current value of FLAGS_node_timeout_s, in milliseconds.
     */
    static int64_t GetLeaseExpiredMs();

    /**
     * @brief Obtains the latest heartbeat interval for the worker to send heartbeat information to etcd.
     * @note The value of Flags_heartbeat_interval_ms can be dynamically changed by O&M personnel during worker running.
     * @return int64_t - the latest heartbeat interval
     */
    int64_t GetLeaseRenewIntervalMs();

private:
    /**
     * @brief Set error handle callback function, when keep-alive meets error, it will be called.
     * @param[out] timeTakenMilliseconds Time taken to run the function.
     * @return K_OK on success; the error code otherwise.
     */
    Status SendKeepAliveMessage(int64_t &timeTakenMilliseconds);

    // Async thread pool for keep-alive request, we need it because we need to handle timeout scenario.
    ThreadPool asyncTask_;

    // Async task result.
    std::future<std::pair<Status, EtcdResponse>> asyncRes_;

    // These 2 members provide synchronization on the asyncRes_ future so that a shutdown thread can wait for the
    // submitted async result to be completed.
    std::condition_variable asyncResCond_;
    std::mutex asyncResMtx_;

    // Check keep-alive is running or not.
    std::atomic<bool> shuttingDown_;

    // Protect shuttingDown flag during shutdowns
    WriterPrefRWLock shutdownLock_;

    // ETCD server address.
    std::string address_;

    // Lease id.
    int64_t leaseId_;

    // GRPC client context for keep-alive request.
    grpc::ClientContext context_;

    // GRPC completion queue for async read/write.
    grpc::CompletionQueue cq_;

    // Keep-alive response read from ETCD server.
    etcdserverpb::LeaseKeepAliveResponse rspPb_;

    // Lease stub.
    std::unique_ptr<GrpcSession<etcdserverpb::Lease>> leaseSession_;

    // Async client reader writer.
    using KeepAliveStream =
        grpc::ClientAsyncReaderWriter<etcdserverpb::LeaseKeepAliveRequest, etcdserverpb::LeaseKeepAliveResponse>;
    std::unique_ptr<KeepAliveStream> stream_;

    static const int kMillisecsPerSecond_ = 1000;
};
}  // namespace datasystem
#endif
