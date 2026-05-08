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
 * Description: Zmq Server Implementation.
 */
#ifndef DATASYSTEM_COMMON_RPC_ZMQ_SERVER_IMPL_H
#define DATASYSTEM_COMMON_RPC_ZMQ_SERVER_IMPL_H

#include <atomic>
#include <cstdint>
#include <deque>
#include <functional>
#include <future>
#include <map>
#include <optional>
#include <thread>
#include <vector>

#include <poll.h>

#include "datasystem/common/log/log.h"
#include "datasystem/common/eventloop/event_loop.h"
#include "datasystem/common/rpc/rpc_auth_key_manager.h"
#include "datasystem/common/rpc/rpc_helper.h"
#include "datasystem/common/rpc/zmq/zmq_auth.h"
#include "datasystem/common/rpc/zmq/zmq_common.h"
#include "datasystem/common/rpc/zmq/zmq_epoll.h"
#include "datasystem/common/rpc/zmq/zmq_payload.h"
#include "datasystem/common/rpc/zmq/zmq_service.h"
#include "datasystem/common/rpc/zmq/zmq_socket.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/util/wait_post.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
// A class to handle direct tcp/ip or uds connections since they tend to be the bottleneck
class IOService {
public:
    explicit IOService(int id, ZmqEpoll *in, ZmqEpoll *out);
    ~IOService();
    Status Init();
    void Stop();
    Status AddFd(int fd, ZmqService *svc, bool uds);

    Status Send(int fd, ZmqMetaMsgFrames &p);

private:
    int id_;
    struct FdEvent : public BaseHint {
        explicit FdEvent(int fd, ZmqService *svc)
            : fd_(fd),
              uds_(false),
              addRoute_(true),
              svc_(svc),
              decoder_(std::make_unique<ZmqMsgDecoder>(fd)),
              encoder_(std::make_unique<ZmqMsgEncoder>(fd))
        {
        }
        ~FdEvent() = default;
        WriterPrefRWLock outMux_;
        int fd_;
        bool uds_;
        bool addRoute_;
        ZmqService *svc_;
        std::unique_ptr<ZmqMsgDecoder> decoder_;
        std::unique_ptr<ZmqMsgEncoder> encoder_;
        std::deque<ZmqMetaMsgFrames> outMsgFrames_;
    };
    mutable WriterPrefRWLock ioLock_;
    ZmqEpoll *inPoller_;
    ZmqEpoll *outPoller_;
    std::map<int, std::pair<ZmqEpollHandle, std::shared_ptr<FdEvent>>> inBoundHandles_;
    std::map<int, std::pair<ZmqEpollHandle, std::shared_ptr<FdEvent>>> outBoundHandles_;

    Status ClientToService(ZmqPollEntry *pe, EventsVal events);
    Status ServiceToClient(ZmqPollEntry *pe, EventsVal events);
    Status RemoveFd(int fd);
};

class SockEventService {
public:
    SockEventService();
    ~SockEventService();

    Status Init();

    void Stop();

    Status AddFd(ZmqService *svc, int fd, bool uds);

    Status ServiceToClient(int fd, ZmqMetaMsgFrames &p);

private:
    WriterPrefRWLock fdLock_;
    std::map<int, int> fdIoSvc_;
    std::unique_ptr<std::vector<std::unique_ptr<IOService>>> io_;
    std::unique_ptr<std::vector<std::unique_ptr<ZmqEpoll>>> pollers_;
    int64_t nextIoSvc_;
};

/**
 * @brief Implementation class of server.
 */
class ZmqServerImpl : public Interruptible {
public:
    /**
     * @brief Passkey Idiom. Only RpcServer can call the constructor and some
     * restricted functions.
     */
    class Token {
    public:
        ~Token() = default;

    private:
        friend class RpcServer;
        Token() = default;
    };
    /**
     * @brief The only way to build is to go through RpcServer class.
     * @param[in] key Token.
     * @param[in] cred Server credential.
     */
    explicit ZmqServerImpl(Token key, const RpcCredential &cred = RpcCredential());
    /**
     * @brief Destructor.
     */
    virtual ~ZmqServerImpl();

    /**
     * @brief Perform server initialization
     */
    Status Init();

    /**
     * @brief Bind to a service end point.
     * @param[in] endpoint Endpoint string.
     * @return Status of call.
     */
    Status Bind(const std::string &endpoint);

    /**
     * @brief Start the server.
     * @note Must call BInd first before Run.
     * @return Status of call.
     */
    Status Run();

    /**
     * @brief Shutdown down a server.
     */
    void Shutdown();

    /**
     * @brief Implementation of virtual function of Interruptible class.
     * @return Whether it is interrupted.
     */
    bool IsInterrupted() const override
    {
        return globalInterrupt_;
    }

    /**
     * @brief Post an interrupt signal.
     */
    void Interrupt() override
    {
        globalInterrupt_ = true;
    }

    /**
     * @brief Query what ports the server is listening.
     * @return Listening port strings.
     */
    std::vector<std::string> GetListeningPorts() const
    {
        return tcpListeningPorts_;
    }

    /**
     * @brief Register a Service.
     * @note Caller owns the service object pointer and must ensure it is not deallocated when the server is
     running.
     * @param[in] key Token.
     * @param[in] svc Service.
     * @param[in] svcCfg The service config.
     * @return Status of call.
     */
    Status RegisterService(Token key, ZmqService *svc, const RpcServiceCfg &svcCfg);

    /**
     * @brief Initialize authentication handler for this server to enable authentication.
     * @return Status of the call.
     */
    Status InitAuthHandler();

    /**
     * @brief Obtains the threadpool usage of RpcService (interval-based, resets counters).
     * @param[in] serviceName The name of rpc service.
     */
    ThreadPool::ThreadPoolUsage GetRpcServicesUsage(const std::string &serviceName);

    ThreadPool::ThreadPoolUsage GetRpcServicesSnapshot(const std::string &serviceName);

    /**
     * @brief Report the list of end points this proxy is listen to
     */
    std::vector<std::string> GetFrontendEndPtList() const
    {
        return frontendEndPtList_;
    }

    /**
     * @brief Provide a poller to be used by registered service.
     * @note Do not use this poller for long running i/o
     */
    std::shared_ptr<ZmqEpoll> GetPoller()
    {
        return poller_;
    }

    /**
     * @brief Provide a SockEventService to be used by registered service
     */
    std::shared_ptr<SockEventService> GetSockEventService()
    {
        return ses_;
    }

private:
    std::shared_ptr<ZmqContext> ctx_;
    int ffd_;  // frontend_ event fd
    int sfd_;  // for interrupt
    std::unordered_map<int, ZmqService *> zmqfds_;
    std::vector<std::string> frontendEndPtList_;
    std::atomic<bool> globalInterrupt_;
    std::unique_ptr<ThreadPool> thrdPool_;
    std::shared_ptr<ZmqSocket> frontend_;
    std::shared_ptr<ZmqEpoll> poller_;
    std::shared_ptr<SockEventService> ses_;
    RpcCredential cred_;
    RPC_AUTH_TYPE authMechanism_;
    std::vector<std::string> tcpListeningPorts_;

    /**
     * This is the map of all registered service.
     */
    std::unordered_map<std::string, ZmqService *> svcMap_;

    /**
     * Thread that start the proxy.
     */
    std::unique_ptr<std::shared_future<Status>> thrd_;

    /**
     * @brief Start the authentication handler thread. The thread will perform authentications for the incoming
     * connections over ZAP protocol. See ZMQ RFC 27 for more details.
     * @note There shall be only one handler within a process.
     * @return Status of the call.
     */
    Status StartAllThreads();
    void StopAllServices();
    Status ClientToService(ZmqMsgFrames &&frames);
    Status ServiceToClient(const MetaPb &meta, ZmqMsgFrames &&frames);
    Status StartProxy();
    Status ZmqFrontendToSvc();
    Status SendErrorToClient(const MetaPb &meta, const Status &status);
    Status ProcessReply(ZmqService *svc);
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_ZMQ_SERVER_IMPL_H
