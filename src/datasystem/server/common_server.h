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
 * Description: CommonServer is base rpc server of worker/master/gcs.
 */
#ifndef DATASYSTEM_SERVER_COMMON_SERVER_H
#define DATASYSTEM_SERVER_COMMON_SERVER_H
#include <memory>
#include <unordered_map>

#include <google/protobuf/any.pb.h>
#include <google/protobuf/repeated_field.h>

#include "datasystem/common/eventloop/event_loop.h"
#include "datasystem/common/rpc/rpc_channel.h"
#include "datasystem/common/rpc/rpc_helper.h"
#include "datasystem/common/rpc/rpc_server.h"
#include "datasystem/common/rpc/rpc_channel.h"
#include "datasystem/common/string_intern/string_ref.h"
#include "datasystem/common/util/status_helper.h"

#ifdef WITH_TESTS
#include "datasystem/server/generic_service_impl.h"
#endif

namespace datasystem {
class CommonServer {
public:
    /**
     * @brief Construct CommonServer.
     */
    CommonServer(HostPort hostPort, HostPort bindHostPort);

    virtual ~CommonServer() = default;

    /**
     * @brief Do some init work(such as register Common Rpc Service) before Start RpcServer.
     * @return Status of the call.
     */
    virtual Status Init();

    /**
     * @brief Start RpcServer to provide rpc service.
     * @return Status of the call.
     */
    virtual Status Start();

    /**
     * @brief Shutdown RpcServer.
     * @return Status of the call.
     */
    virtual Status Shutdown();

    /**
     * @brief Register a client to client manager.
     * @param[in] clientId The clientId.
     * @param[in] shmEnabled Indicates whether the client allows shared memory.
     * @param[in] socketFd The unix domain socket Fd.
     * @param[in] tenantId The tenant id.
     * @param[in] enableCrossNode Client is enable cross node connection or not.
     * @param[in] podName Client pod name.
     * @param[in] supportMultiShmRefCount Client support multi shm ref count or not.
     * @param[in] deviceId pipeline h2d device id
     * @param[out] lockId The lock id.
     * @return Status of the call.
     */
    virtual Status AddClient(const ClientKey &clientId, bool shmEnabled, int32_t socketFd, const std::string &tenantId,
                             bool enableCrossNode, const std::string &podName, bool supportMultiShmRefCount,
                             std::string deviceId, uint32_t &lockId) = 0;

    /**
     * @brief After restart crashed server, we need to do some recovery job according to the message from the client.
     * @param[in] clientId The id of the client.
     * @param[in] tenantId The token to be authenticated.
     * @param[in] reqToken Need construct obj uri.
     * @param[in] msg The message from the client.
     * @return Status of the call.
     */
    virtual Status ProcessServerReboot(const ClientKey &clientId, const std::string &tenantId,
                                       const std::string &reqToken,
                                       const google::protobuf::RepeatedPtrField<google::protobuf::Any> &msg) = 0;

    /**
     * @brief General method of cleaning the data of a client while the client disconnecting.
     * @param[in] clientId The client id of the corresponding client with the socket fd.
     */
    virtual void AfterClientLostHandler(const ClientKey &clientId) = 0;

    /**
     * @brief Obtains the threadpool usage of RpcService (interval-based, resets counters).
     * @param[in] serviceName The name of rpc service.
     */
    ThreadPool::ThreadPoolUsage GetRpcServicesUsage(const std::string &serviceName);

    ThreadPool::ThreadPoolUsage GetRpcServicesSnapshot(const std::string &serviceName);

    /**
     * @brief Get the pointer information for shared memory communication
     * @param[int] lockId Get shm info by lock id.
     * @param[out] fd File descriptor of the allocated shared memory segments.
     * @param[out] mmapSize Total size of shared memory segments.
     * @param[out] offset Offset from the base of the shared memory mmap.
     * @param[out] id The id of this shmUnit.
     * @return Status of the call.
     */
    virtual Status GetShmQueueUnit(uint32_t lockId, int &fd, uint64_t &mmapSize, ptrdiff_t &offset, ShmKey &id) = 0;

    virtual Status GetExclConnSockPath(std::string &sockPath) = 0;

protected:
    /**
     * @brief To define common service of rpc server.
     * @return Status of the call.
     */
    Status CreateGenericService();

    HostPort hostPort_;      // The process ip address and port.
    HostPort bindHostPort_;  // The bind ip address and port.
    RpcServer::Builder builder_;
#ifdef WITH_TESTS
    std::unique_ptr<GenericServiceImpl> genericSvc_{ nullptr };  // The generic rpc service.
#endif
    std::unique_ptr<RpcServer> rpcServer_{ nullptr };
    std::shared_ptr<SockEventLoop> eventLoop_{ nullptr };
    std::unordered_map<std::string, int> clientIdToSocketFd_;  // Record each clientId and its socket fd.
};
}  // namespace datasystem
#endif
