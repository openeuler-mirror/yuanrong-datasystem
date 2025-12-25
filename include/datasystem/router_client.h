/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: Router client for selecting worker.
 */
 
#ifndef DATASYSTEM_ROUTER_CLIENT_H
#define DATASYSTEM_ROUTER_CLIENT_H
 
#include <condition_variable>
#include <functional>
#include <memory>
#include <set>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include <map>
 
#include "datasystem/utils/sensitive_value.h"
#include "datasystem/utils/status.h"
 
namespace datasystem {
class EtcdStore;
class RandomData;
}  // namespace datasystem
 
namespace mvccpb {
class Event;
}  // namespace mvccpb
 
namespace datasystem {
class __attribute((visibility("default"))) RouterClient {
public:
    /**
     * @brief Construct RouterClient. If certificate authentication is enabled for the etcd to be connected, must
     *        specify etcdCa, etcdCert, etcdKey and etcdNameOverride.
     * @param[in] azName The AZ name of the worker address to be monitored.
     * @param[in] etcdAddress The Etcd address.
     * @param[in] etcdCa Root etcd certificate, optional parameters.
     * @param[in] etcdCert Etcd certificate chain, optional parameters.
     * @param[in] etcdKey Etcd private key, optional parameters.
     * @param[in] etcdDNSName Etcd DNS name, optional parameters.
     */
    RouterClient(const std::string &azName, const std::string &etcdAddress, const SensitiveValue &etcdCa = "",
                 const SensitiveValue &etcdCert = "", const SensitiveValue &etcdKey = "",
                 const std::string &etcdDNSName = "");
 
    ~RouterClient() = default;
 
    /**
     * @brief Connects to etcd to obtain the worker address and listens to the change of the specified AZ worker
     *        address.
     * @return Status of the call.
     */
    Status Init();
 
    /**
     * @brief Select a worker address. The worker where the targetWorkerHost node is located is preferred. Otherwise,
     *        other worker address is returned.
     * @param[in] targetWorkerHost Indicates the worker node that needs to be returned first.
     * @param[out] outIpAddr The returned worker address.
     * @return Status of the call.
     */
    Status SelectWorker(const std::string &targetWorkerHost, std::string &outIpAddr);
 
    /**
     * @brief Get the worker address by worker id.
     * @param[in] workerIds The worker ids.
     * @param[out] workerAddrs The worker addresses. Each address is represented as ip:port format. If worker address is
     * not found, represented as empty instead.
     * @return K_OK on any object success. Otherwise K_NOT_FOUND.
     */
    Status GetWorkerAddrByWorkerId(const std::vector<std::string> &workerIds,
                                   std::vector<std::string> &workerAddrs) const;
 
private:
    /**
     * @brief When the status of workers changes, a watch response is returned. This function is used to add or
     *        delete active worker addresses based on the returned type.
     * @param[in] event The worker change event.
     */
    void HandleClusterEvent(const mvccpb::Event &event);
 
    /**
     * @brief When the hash ring changes, a watch response is returned. This function is used to add or
     *        delete worker addresses based on the returned type.
     * @param[in] event The hash ring change event.
     */
    void HandleRingEvent(const mvccpb::Event &event);
 
    /**
     * @brief When the watch keys in etcd changes, a watch response is returned.
     * @param[in] event The etcd key change event.
     */
    void HandleEvent(mvccpb::Event &&event);
 
    /**
     * @brief Initial the active workers and workerid to workerAddr map when startup.
     * @param[out] nodeRevision The revision that the nodes initialization used.
     * @param[out] ringRevision The revision that the hash ring initialization used.
     */
    Status SetupInitialWorkers(int64_t &nodeRevision, int64_t &ringRevision);
 
    std::string azName_;
    std::string etcdAddress_;
    SensitiveValue etcdCa_;
    SensitiveValue etcdCert_;
    SensitiveValue etcdKey_;
    std::string etcdDNSName_;
    std::set<std::string> activeWorkerAddrs_;
    std::unordered_map<std::string, std::string> workerId2Addrs_;
    std::shared_ptr<RandomData> randomData_;
    // eventMutex_ is used to protect the read and write of activeWorkerAddrs_.
    mutable std::shared_timed_mutex eventMutex_;
    // etcdStore_ uses eventMutex_ and activeWorkerAddrs_, so needs to be destructed first.
    std::shared_ptr<EtcdStore> etcdStore_;
};
}  // namespace datasystem
#endif  // DATASYSTEM_ROUTER_CLIENT_H