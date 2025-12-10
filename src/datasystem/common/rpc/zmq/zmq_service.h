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
 * Description: Zmq Service.
 */
#ifndef DATASYSTEM_COMMON_RPC_ZMQ_SERVICE_H
#define DATASYSTEM_COMMON_RPC_ZMQ_SERVICE_H

#include <sys/epoll.h>
#include <sys/eventfd.h>

#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <future>
#include <map>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

#include "datasystem/common/log/log.h"
#include "datasystem/common/rpc/rpc_service_cfg.h"
#include "datasystem/common/rpc/zmq/rpc_service_method.h"
#include "datasystem/common/rpc/zmq/zmq_common.h"
#include "datasystem/common/rpc/zmq/zmq_epoll.h"
#include "datasystem/common/rpc/zmq/zmq_msg_decoder.h"
#include "datasystem/common/rpc/zmq/zmq_msg_queue.h"
#include "datasystem/common/rpc/zmq/zmq_payload.h"
#include "datasystem/common/rpc/zmq/zmq_socket.h"
#include "datasystem/common/eventloop/timer_queue.h"
#include "datasystem/common/util/queue/queue.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/rpc/zmq/work_agent.h"

namespace datasystem {
typedef MsgQueRef<ZmqMetaMsgFrames, ZmqMetaMsgFrames> ZmqServerMsgQueRef;
typedef MsgQueMgr<ZmqMetaMsgFrames, ZmqMetaMsgFrames> ZmqServerMsgMgr;
typedef decltype(epoll_event::events) EventsVal;
class SockEventService;
class WorkAgent;
/**
 * @brief An abstract class for RPC service.
 * The ZMQ plugin will generate a subclass, and the user will supply the virtual method implementation.
 */
class ZmqService {
public:
    ZmqService();
    virtual ~ZmqService();
    virtual std::string FullServiceName() const = 0;
    virtual std::string ServiceName() const = 0;
    /**
     * @brief This virtual function is called by zmq_plugin generated code and is mainly
     * used in ZMQ event loop.
     * @param[in] sock Zmq server message queue reference.
     * @param[in] meta Meta Pb.
     * @param[in] inMsg In frame.
     * @return Status of call.
     */
    virtual Status CallMethod(std::shared_ptr<ZmqServerMsgQueRef> sock, MetaPb meta, ZmqMsgFrames &&inMsg,
                              int64_t seqNo) = 0;
    virtual Status DirectCallMethod(MetaPb meta, ZmqMsgFrames &&inMsg, int64_t seqNo, ZmqMsgFrames &outMsg) = 0;

    Status Init(RpcServiceCfg cfg, void *proxy);

    int32_t NumRegularSockets() const
    {
        return cfg_.numRegularSockets_;
    }

    int32_t NumStreamSockets() const
    {
        return cfg_.numStreamSockets_;
    }

    bool NeedStreamSupport() const
    {
        return streamSupport_;
    }

    void UpdateStreamWorkerSeqNo(int32_t id, int64_t nextSeqNo);

    void MarkStreamWorkerAvailable(int32_t id);

    void Print(std::ostream &out) const
    {
        out << "Service " << ServiceName() << "\nStream support: " << (streamSupport_ ? "true" : "false")
            << "\nNum regular sockets: " << cfg_.numRegularSockets_
            << "\nNum stream sockets: " << cfg_.numStreamSockets_ << "\nHigh Water Mark: " << cfg_.hwm_ << std::endl;
    }

    friend std::ostream &operator<<(std::ostream &out, const ZmqService &svc)
    {
        svc.Print(out);
        return out;
    }

    auto GetEventFd() const
    {
        return outfd_;
    }

    Status GetExclConnSockPath(std::string &sockPath)
    {
        sockPath = exclSockPath_;
        return Status::OK();
    }

    Status ServiceRequest(MetaPb &&meta, ZmqMsgFrames &&msgs);

    Status ServiceReply(ZmqMetaMsgFrames *msg);

    static Status ParseWorkerId(const std::string &workerId, ZmqService *&svc, int32_t &id);

    static Status SendStatus(ZmqMsgFramesRef &frames, const MetaPb &meta, const Status &rc,
                             const std::function<Status(ZmqMetaMsgFramesRef)> &f);

    static Status SendAll(ZmqMsgFramesRef &frames, const MetaPb &meta,
                          const std::function<Status(ZmqMetaMsgFramesRef)> &f);

    /**
     * @brief This function is used mainly in the destructor of ServerStreamBase.
     * @note Do not use it in any other places.
     * @return Service name.
     */
    std::string StreamGetSvcName() const
    {
        return serviceName_;
    }

    Status DirectExecInternalMethod(ZmqMetaMsgFrames &inFrames, ZmqMetaMsgFrames &outFrames);

protected:
    /**
     * This map is populated by zmq_plugin generated code.
     */
    std::map<int32_t, std::shared_ptr<RpcServiceMethod>> methodMap_;
    /**
     * The following will be set up when we call RpcServer::RegisterService
     */
    RpcServiceCfg cfg_;
    std::string serviceName_;
    // A place where we can park the payloads
    std::unique_ptr<ZmqPayloadBank> payloadBank_;
    // Cache all the methods that will send payload
    std::set<int> methodsHavePayloadRecv_;

private:
    /**
     * Worker control block for each thread in the pool.
     */
    class WorkerCB {
    public:
        enum class StreamState : uint8_t { NONE = 0, CREATE, IN_USE, END_OF_STREAM };
        explicit WorkerCB(ZmqService *svc, int32_t id);
        ~WorkerCB();

        /**
         * Entry function for each thread.
         */
        Status WorkerEntry();
        Status WorkerEntryWithoutMsgQ(ZmqMetaMsgFrames &inMsg, ZmqMetaMsgFrames &outMsg);
        Status StreamWorkerEntry();

        void CloseSocket()
        {
            WriteLock xlock(&inUse_);
            if (worker_) {
                worker_->Close(false);
            }
        }

        Status ConnectToBackend(std::shared_ptr<ZmqServerMsgMgr> &mgr, int hwm);

        std::string GetWorkerId() const;

        auto &GetStreamWA()
        {
            return streamWA_;
        }

        void WakeUpStreamWorker()
        {
            streamWA_.cv_.notify_all();
        }

    private:
        Status HandleInternalRq(int fd, const MetaPb &meta, ZmqMsgFrames &inMsg, ZmqMsgFrames &replyMsg);
        Status WorkerEntryImpl(MetaPb &meta, ZmqMsgFrames &inMsg, ZmqMsgFrames &replyMsg);
        Status StreamWorkerEntryImpl();
        Status ProcessStreamRpcRq(const MetaPb &meta, ZmqMsgFrames &inMsg, ZmqMsgFrames &replyMsg);
        Status ProcessHandshakeRq(const MetaPb &meta, ZmqMsgFrames &inMsg, ZmqMsgFrames &replyMsg);
        Status ProcessPayloadRq(const MetaPb &meta, ZmqMsgFrames &inMsg, ZmqMsgFrames &replyMsg);

        ZmqService *impl_;
        std::shared_ptr<ZmqServerMsgQueRef> worker_;
        int32_t workerId_;
        mutable WriterPrefRWLock inUse_;  // sync with Close() and RecvMsg()
        struct StreamWorkArea {
            std::mutex mux_;
            std::condition_variable cv_;
            MetaPb meta_;
            int64_t nextSeqNo_{ 0 };
            StreamState state_{ StreamState::NONE };
            std::chrono::steady_clock::time_point lastAccessTime_;
            void Reset()
            {
                nextSeqNo_ = 0;
                state_ = StreamState::NONE;
                meta_.Clear();
            }
        } streamWA_;
    };
    std::string GetWorkerIdStr(int32_t i) const;
    Status ConnectToBackend(std::shared_ptr<ZmqServerMsgMgr> &backend, size_t unaryHWM, size_t streamHWM);
    Status GetAvailStreamWorker(MetaPb meta, std::string &workerId);
    void Stop();
    std::map<int32_t, std::string> GetAvailWorkerId() const;
    Status BackendToFrontend(std::shared_ptr<ZmqServerMsgMgr> &mgr);
    Status FrontendToBackend(int fd, EventType type, ZmqMetaMsgFrames &p, bool addRoute);
    void StartHWMTimer();
    Status CreateWorkerCBs();
    Status CreateBackendMgr();
    Status ServiceToClient(ZmqMetaMsgFrames &frames);
    Status HandleRqFromProxy();
    Status ProcessAccept(int fd);
    Status BindTcpIpPort(const std::vector<std::string> &frontendEndPtList);
    Status BindUnixPath();
    Status RouteToRegBackend(ZmqMetaMsgFrames &p);
    Status InitEventLoop();
    Status ValidateAndAdjustCfg();
    void CheckHWMRatio();
    Status AddListenFd(int fd);
    Status HandleInternalMethods(ZmqMetaMsgFrames &p);
    Status ParkPayloadIfNeeded(ZmqMetaMsgFrames &p, ZmqMsgFrames &payload);
    Status ProcessPayloadGetRq(MetaPb &meta, ZmqMsgFrames &inMsg, ZmqMsgFrames &replyMsg);
    void AddRoute(MetaPb &meta, int fd);
    void DeleteRoute(int fd);
    Status InitThreadPool();
    Status SendErrorMaxExclusive(WorkAgent *workAgent, const ThreadPool::ThreadPoolUsage &poolUsage);

    std::unique_ptr<ThreadPool> thrdPool_{ nullptr };
    std::map<int32_t, std::shared_ptr<WorkerCB>> workerCBs_;
    std::deque<std::shared_ptr<WorkerCB>> availWorkers_;
    std::mutex streamLock_;
    std::condition_variable streamCv_;
    std::deque<std::shared_ptr<WorkerCB>> availStreamWorkers_;

    std::unique_ptr<Queue<ZmqMetaMsgFrames>> rqQueue_{ nullptr };
    std::unique_ptr<Queue<ZmqMetaMsgFrames>> replyQueue_{ nullptr };

    friend class ZmqServerImpl;
    friend class IOService;
    void *proxy_;
    int outfd_;             // For replyQueue_.
    int infd_;              // For rqQueue_.
    int tcpfd_;             // If bypass ZmqServiceImpl
    int exclListenFd_;
    HostPort tcpHostPort_;  // If bypass ZmqServiceImpl
    std::atomic<int64_t> nextWorker_;
    std::atomic<bool> globalInterrupt_;
    bool streamSupport_;
    bool multiDestinations_;
    std::vector<std::string> sockPath_;
    std::string exclSockPath_;
    bool unlinkSocketPathOnExit_;
    bool tcpDirect_;
    std::shared_ptr<ZmqServerMsgMgr> backendMgr_{ nullptr };
    ZmqServerMsgMgr::MsgRoutingFn routingFn_{ nullptr };
    ZmqServerMsgMgr::MsgRoutingFn multiRoutingFn_{ nullptr };
    std::shared_ptr<ZmqEpoll> inBoundPoller_;
    std::map<int, ZmqEpollHandle> fdHandles_;
    std::shared_ptr<SockEventService> io_;
    std::unique_ptr<TimerQueue::TimerImpl> hwmCheck_;
    WriterPrefRWLock routeMux_;
    std::map<std::string, std::set<int>> routes_;
    std::map<int, std::string> fdToGateway_;

    std::vector<std::unique_ptr<WorkAgent>> workAgents_;
    std::unique_ptr<ThreadPool> workAgentThreadPool_ { nullptr };
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_ZMQ_SERVICE_H
