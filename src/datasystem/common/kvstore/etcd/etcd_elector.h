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
 * Description: Etcd elector declaration.
 */
#ifndef DATASYSTEM_COMMON_KVSTORE_ETCD_ETCD_ELECTOR_H
#define DATASYSTEM_COMMON_KVSTORE_ETCD_ETCD_ELECTOR_H

#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>

#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/kvstore/etcd/etcd_keep_alive.h"
#include "datasystem/common/kvstore/etcd/grpc_session.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/util/thread.h"
#include "etcd/api/etcdserverpb/rpc.grpc.pb.h"
#include "etcd/v3election.grpc.pb.h"

namespace datasystem {
class EtcdObserver;
class EtcdElector {
public:
    explicit EtcdElector(std::string address) : address_(std::move(address))
    {
    }

    ~EtcdElector() = default;

    /**
     * @brief Init Etcd address.
     */
    Status Init();

    /**
     * @brief Shutdown EtcdElector and clear everything.
     */
    void Shutdown();

    /**
     * @brief Campaign waits to acquire leadership in an election, returning a LeaderKey
     *        representing the leadership if successful.
     * @param[in] name The election's identifier for the campaign.
     * @param[in] lease The ID of the lease attached to leadership of the election.
     *            If the lease expires or is revoked before resigning leadership,
     *            then the leadership is transferred to the next campaigner, if any.
     * @param[in] value The initial proclaimed value set when the campaigner wins the election.
     * @param[out] response The etcd response.
     * @return Status of the call.
     */
    Status Campaign(const std::string &name, int64_t lease, const std::string &value, EtcdResponse &response);

    /**
     * Returns the current election proclamation, if any.
     * @param[in] name The election identifier for the leadership information.
     * @param[out] response The etcd response.
     * @return Status of the call.
     */
    Status Leader(const std::string &name, EtcdResponse &response);

    /**
     * @param[in] name The election identifier that corresponds to the leadership key.
     * @param[in] key An opaque key representing the ownership of the election.
     *            If the key is deleted, then leadership is lost.
     * @param[in] revision The creation revision of the key. It can be used to test for
     *            ownership of an election during transactions by testing the key's
     *            creation revision matches rev.
     * @param[in] lease The lease ID of the election leader.
     * @param[out] response The etcd response.
     * @return Status of the call
     */
    Status Resign(const std::string &name, const std::string &key, int64_t revision, int64_t lease,
                  EtcdResponse &response);

    /**
     * @brief Observe streams election proclamations in-order as made by the election's elected leaders.
     * @param[in] name The election identifier for the leadership information.
     * @param[in] callback Observe callback function, when election identifier is change, the function would be invoke.
     * @param[in] once Observe once or not.
     * @return EtcdObserver unique pointer.
     */
    std::unique_ptr<EtcdObserver> Observe(const std::string &name,
                                          const std::function<void(const EtcdResponse &)> &callback, bool once = false);

    /**
     * @brief Keeps the lease alive.
     * @param[in] ttl Time to live of the lease.
     * @return EtcdKeepAlive shared pointer.
     */
    std::unique_ptr<EtcdKeepAlive> LeaseKeepAlive(int ttl);

private:
    // ETCD server address.
    std::string address_;

    // Lease stub.
    std::unique_ptr<GrpcSession<etcdserverpb::Lease>> leaseSession_;

    // Election stub.
    std::unique_ptr<GrpcSession<v3electionpb::Election>> electionSession_;
};

class EtcdObserver {
public:
    EtcdObserver(std::string address, std::string name, std::function<void(const EtcdResponse &)> callback, bool once)
        : running_(false),
          name_(std::move(name)),
          callback_(std::move(callback)),
          once_(once),
          address_(std::move(address))
    {
    }

    ~EtcdObserver();

    /**
     * @brief Shutdown keep-alive and exit everything.
     */
    void Shutdown();

private:
    friend class EtcdElector;

    /**
     * @brief Init election stub, stream client to connect to ETCD server
     *        and run background thread to listen the key change.
     * @return K_OK on success; the error code otherwise.
     */
    Status Init();

    void Start();

    /**
     * @brief Observe the key change and call callback function to noify.
     */
    void Observe();

    // Check keep-alive is running or not.
    std::atomic<bool> running_;

    // Name for observe.
    std::string name_;

    // Observe callback function, when key changed, it will be call.
    std::function<void(const EtcdResponse &)> callback_;

    // Observe once or not.
    bool once_;

    // Background thread to handle the observe task.
    Thread thread_;

    // ETCD server address.
    std::string address_;

    // GRPC client context for observe request.
    std::unique_ptr<grpc::ClientContext> context_;

    // Election stub.
    std::unique_ptr<GrpcSession<v3electionpb::Election>> electionSession_;

    // Sync client reader for observe.
    std::unique_ptr<grpc::ClientReader<v3electionpb::LeaderResponse>> streamReader_;
};
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_KVSTORE_ETCD_ETCD_ELECTOR_H
