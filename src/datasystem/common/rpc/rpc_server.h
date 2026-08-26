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
 * Description: RPC Server.
 */
#ifndef DATASYSTEM_COMMON_RPC_RPC_SERVER_H
#define DATASYSTEM_COMMON_RPC_RPC_SERVER_H

#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>
#include <variant>

namespace brpc {
class Server;
}  // namespace brpc

namespace google {
namespace protobuf {
class Service;
}  // namespace protobuf
}  // namespace google

#include "datasystem/common/rpc/rpc_constants.h"
#include "datasystem/common/rpc/rpc_helper.h"
#include "datasystem/common/rpc/rpc_options.h"
#include "datasystem/common/rpc/rpc_service_base.h"
#include "datasystem/common/rpc/rpc_service_cfg.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_pool.h"

DS_DECLARE_int32(rpc_thread_num);
DS_DECLARE_int32(v);

namespace datasystem {

class RpcServer final : public Interruptible {
public:
    /**
     * @brief A builder class to create a RpcServer.
     */
    class Builder {
    public:
        Builder()
        {
        }
        ~Builder() = default;

        /**
         * @brief Enable RPC verbose logging.
         * @return Reference to this builder.
         */
        Builder &SetDebug()
        {
            FLAGS_v = RPC_LOG_LEVEL;
            return *this;
        }

        /**
         * @brief Add RPC service.
         * @param[in] svc Generated RPC service (RpcServiceBase subclass).
         * @param[in] svcEle Configuration about number of rpc threads.
         * @return Reference to this builder.
         */
        Builder &AddService(RpcServiceBase *svc, const RpcServiceCfg &svcEle)
        {
            svcList_.emplace_back(svc, svcEle);
            return *this;
        }

        /**
         * @brief Set the callback function.
         * @param[in] callback The callback function.
         * @return Reference to this builder.
         */
        Builder &SetPreStartCallback(std::function<Status()> &&callback)
        {
            preStartCallback_ = std::move(callback);
            return *this;
        }

        /**
         * @brief Set brpc listen address.
         * @param[in] addr IP address.
         * @param[in] port Port number.
         * @return Reference to this builder.
         */
        Builder &SetBrpcAddr(const std::string &addr, int port)
        {
            brpcAddr_ = addr;
            brpcPort_ = port;
            return *this;
        }

        /**
         * @brief Build and start RPC server.
         *
         * Only builds the server skeleton; does NOT start brpc. The caller must
         * register brpc adapter services via AddBrpcService(), then call
         * server->StartBrpcServer(brpcAddr_, brpcPort_) explicitly.
         * See WorkerOCServer::Init() for the canonical usage pattern.
         *
         * @param[out] server Built rpc server.
         * @return Status of the call.
         */
        Status BuildAndStart(std::unique_ptr<RpcServer> &server) const;

        /**
         * @brief Init RPC server.
         * @param[out] Init rpc server.
         * @return Status of the call.
         */
        Status Init(std::unique_ptr<RpcServer> &server) const;

#ifdef WITH_TESTS
        /**
         * @brief Init and start RPC server.
         * @param[in/out] The rpc server.
         * @return Status of the call.
         */
        Status InitAndStart(std::unique_ptr<RpcServer> &server) const
        {
            RETURN_IF_NOT_OK(Init(server));
            RETURN_IF_NOT_OK(BuildAndStart(server));
            return Status::OK();
        }
#endif

    private:
        std::vector<std::pair<RpcServiceBase *, RpcServiceCfg>> svcList_;
        std::function<Status()> preStartCallback_{};
        std::string brpcAddr_;
        int brpcPort_ = 0;
    };

    /**
     * @brief Passkey Idiom. Only Builder can call the constructor and some
     * restricted functions.
     */
    class Token {
    public:
        ~Token() = default;

    private:
        friend class Builder;
        Token() = default;
    };

    /**
     * @note Only Builder can call the constructor
     * @param[in] key Token.
     */
    explicit RpcServer(Token key);
    ~RpcServer() noexcept;
    RpcServer(const RpcServer &) = delete;
    RpcServer &operator=(const RpcServer &) = delete;
    RpcServer(RpcServer &&) = delete;
    RpcServer &operator=(RpcServer &&) = delete;

    /**
     * @brief Server initialization
     */
    Status Init();

    /**
     * @brief Shutdown a server.
     */
    void Shutdown();

    /**
     * @brief Post an interrupt signal (no-op; brpc server lifecycle is externally driven).
     */
    void Interrupt() override;

    /**
     * @brief Check if the server is being interrupted.
     * @return Whether it is interrupted.
     */
    bool IsInterrupted() const override;

    /**
     * @brief Register a brpc protobuf service.
     * @param[in] service The protobuf service to register.
     * @return Status of the call.
     */
    Status AddBrpcService(google::protobuf::Service *service);

    /**
     * @brief Register brpc services synchronously through the provided registrar.
     * @note Call only before StartBrpcServer() from the external single-threaded lifecycle. Do not call concurrently
     * with AddBrpcService(), StartBrpcServer(), or StopBrpcServer(). The registrar must not retain the server
     * reference.
     * @param[in] registrar Registrar invoked synchronously with the owned brpc server.
     * @return Status returned by the registrar.
     */
    Status AddBrpcServices(const std::function<Status(brpc::Server &)> &registrar);

    /**
     * @brief Start the brpc server listening on the given address and port.
     * @param[in] addr IP address.
     * @param[in] port Port number.
     * @return Status of the call.
     */
    Status StartBrpcServer(const std::string &addr, int port);

    /**
     * @brief Stop the brpc server synchronously (Stop + Join + reset).
     * Thread-safe and idempotent: brpcStopMtx_ serializes concurrent calls.
     */
    void StopBrpcServer();

    /**
     * @brief Check if brpc mode is enabled.
     * @return True if brpc mode is enabled.
     */
    bool IsBrpc() const
    {
        return true;
    }

    /**
     * @brief Query what ports the server is listening.
     * @return Listening port strings.
     */

    /**
     * @brief Obtains the threadpool usage of RpcService (interval-based, resets counters).
     * @param[in] serviceName The name of rpc service.
     */
    ThreadPool::ThreadPoolUsage GetRpcServicesUsage(const std::string &serviceName) const;

    ThreadPool::ThreadPoolUsage GetRpcServicesSnapshot(const std::string &serviceName) const;

private:
    friend class RpcServer::Builder;

    /**
     * @brief Bind to a endpoint.
     * @param[in] endpoint Endpoint to bind to.
     */
    Status Bind(const std::string &endpoint);

    /**
     * @brief Register a Service
     * @note Caller owns the service object pointer and must ensure it is not deallocated when the server is
     * running.
     * @param[in] svc Service to be registered.
     * @param[in] svcEle Configuration about number of rpc threads.
     * @return Status of the call.
     */
    Status RegisterService(RpcServiceBase *svc, const RpcServiceCfg &cfg);

    std::map<std::string, RpcServiceBase *> svcMap_;
    std::unique_ptr<brpc::Server> brpcServer_;
    std::mutex brpcStopMtx_;  ///< Serializes StopBrpcServer() concurrent calls.
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_RPC_SERVER_H
