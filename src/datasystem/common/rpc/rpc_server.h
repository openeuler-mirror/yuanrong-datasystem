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

#include <memory>
#include <utility>
#include <vector>
#include <variant>

#include "datasystem/common/rpc/rpc_constants.h"
#include "datasystem/common/rpc/rpc_credential.h"
#include "datasystem/common/rpc/rpc_helper.h"
#include "datasystem/common/rpc/rpc_options.h"
#include "datasystem/common/rpc/rpc_service_cfg.h"
#include "datasystem/common/rpc/zmq/zmq_auth.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_pool.h"

DS_DECLARE_int32(rpc_thread_num);
DS_DECLARE_int32(v);

namespace datasystem {
class ZmqServerImpl;
class ZmqService;

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
         * @brief Enable zmq logging.
         * @return Reference to this builder.
         */
        Builder &SetDebug()
        {
            FLAGS_v = RPC_LOG_LEVEL;
            return *this;
        }

        /**
         * @brief Add ZMQ end point
         * @param[in] v Endpoint string.
         * @return Reference to this builder.
         */
        Builder &AddEndPoint(std::string v)
        {
            endPts_.push_back(std::move(v));
            return *this;
        }

        /**
         * @brief Add ZMQ service.
         * @param[in] svc ZMQ service.
         * @param[in] svcEle Configuration about number of rpc threads.
         * @return Reference to this builder.
         */
        template <typename T>
        Builder &AddService(T *svc, const RpcServiceCfg &svcEle)
        {
            auto *service = static_cast<ZmqService *>(svc);
            svcList_.emplace_back(service, svcEle);
            return *this;
        }

        /**
         * @brief Set up server credential.
         * @param[in] cred Server credential.
         * @return Reference to this builder.
         */
        Builder &SetCredential(const RpcCredential &cred)
        {
            cred_ = cred;
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
         * @brief Build and start RPC server.
         * @param[in] Built rpc server.
         * @return A rpc server.
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
        std::vector<std::string> endPts_;
        std::vector<std::pair<std::variant<ZmqService *>, RpcServiceCfg>> svcList_;
        RpcCredential cred_;
        std::function<Status()> preStartCallback_{};
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
     * @param[in] cred Server credential.
     */
    explicit RpcServer(Token key, const RpcCredential &cred = RpcCredential());
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
     * @brief Bring up the server.
     */
    Status Run();

    /**
     * @brief Shutdown a server.
     */
    void Shutdown();

    /**
     * @brief Post an interrupt signal.
     */
    void Interrupt() override;

    /**
     * @brief Check if the server is being interrupted.
     * @return Whether it is interrupted.
     */
    bool IsInterrupted() const override;

    /**
     * @brief Query what ports the server is listening.
     * @return Listening port strings.
     */
    std::vector<std::string> GetListeningPorts() const;

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
     running.
     * @param[in] svc Service to be registered.
     * @param[in] svcEle Configuration about number of rpc threads.
     * @return Status of the call.
     */
    Status RegisterService(ZmqService *svc, const RpcServiceCfg &cfg);

    /**
     * @brief Initialize an authentication handler for this server to enable authentication.
     * @return Status of the call.
     */
    Status InitAuthHandler();

    std::variant<std::unique_ptr<ZmqServerImpl>> pimpl_;
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_RPC_SERVER_H
