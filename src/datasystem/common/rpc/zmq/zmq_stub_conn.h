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
 * Description: Stub connection.
 */
#ifndef DATASYSTEM_COMMON_RPC_ZMQ_STUB_CONN_H
#define DATASYSTEM_COMMON_RPC_ZMQ_STUB_CONN_H

#include <atomic>
#include <cstring>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <tuple>
#include <unordered_map>
#include <vector>

#include <sys/eventfd.h>

#include <unistd.h>
#include <pthread.h>

#include "datasystem/common/log/log.h"
#include "datasystem/common/log/logging.h"
#include "datasystem/common/rpc/rpc_channel.h"
#include "datasystem/common/rpc/rpc_credential.h"
#include "datasystem/common/rpc/rpc_options.h"
#include "datasystem/common/rpc/unix_sock_fd.h"
#include "datasystem/common/rpc/zmq/zmq_common.h"
#include "datasystem/common/rpc/zmq/zmq_context.h"
#include "datasystem/common/rpc/zmq/zmq_epoll.h"
#include "datasystem/common/rpc/zmq/zmq_msg_decoder.h"
#include "datasystem/common/rpc/zmq/zmq_msg_queue.h"
#include "datasystem/common/rpc/zmq/zmq_socket.h"
#include "datasystem/common/rpc/zmq/zmq_stub.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/wait_post.h"
#include "datasystem/utils/status.h"

namespace datasystem {
/**
 * A class used as the key for unordered_map.
 * @note Main use is to determine whether a connection (zmq/uds/tcp) can be shared.
 */
struct ZmqConnKey {
    std::string channelEndPoint_;
    std::string svcName_;
    int channelNo_;
    RpcCredential cred_;
    bool svcNameAsKey_;  // See comment in operator==

    ZmqConnKey();
    ~ZmqConnKey() = default;

    bool operator==(const ZmqConnKey &rhs) const;
};
}  // namespace datasystem

namespace std {
template <>
struct hash<datasystem::ZmqConnKey> {
    size_t operator()(const datasystem::ZmqConnKey &key) const;
};
}  // namespace std

namespace datasystem {
// We will maintain a table for self-managed uds or tcp/ip sockets.
// Each row in the table has a mutex for concurrent access.
// First column is the service name
// Second column is the connection end point in the form of ipc://<pathname> or tcp://<HostPort>
// Third column is the file descriptor of the connection socket.
// Fourth column are the epoll handles for pollers. One inbound and one outbound.
// Fifth column is the decoder for receiving rpc reply.
// Sixth column is a simpler way to tell if 2nd column is ipc:// or tcp://
// Primary key(s) on this table is constructed from ZmqConnKey
// Rest of the columns are for establishing connection
class SockConnEntry : public std::enable_shared_from_this<SockConnEntry>, public BaseHint {
public:
    explicit SockConnEntry(ZmqMsgMgr *backend);
    ~SockConnEntry();

    std::string svcName_;   // 1st column
    std::string endPoint_;  // 2nd column
    struct FdConn : public BaseHint {
        std::unique_ptr<WriterPrefRWLock> outMux_;
        int fd_{ ZMQ_NO_FILE_FD };                // 3rd column
        ZmqEpollHandle inHandle_{ nullptr };      // 4th column
        ZmqEpollHandle outHandle_{ nullptr };     // part of 4th column
        std::unique_ptr<ZmqMsgDecoder> decoder_;  // 5th column
        std::unique_ptr<ZmqMsgEncoder> encoder_;  // part of 5th column
        ZmqEpoll *inPoller_{ nullptr };
        ZmqEpoll *outPoller_{ nullptr };
        std::unique_ptr<std::deque<std::pair<EventType, ZmqMsgFrames>>> outMsgQueue_;
    };

    std::vector<std::shared_ptr<SockConnEntry::FdConn>> pools_;
    bool uds_;  // 6th column
    Status connectRc_{ K_UNKNOWN_ERROR, "for initializing" };
    std::atomic<int> retryCount_;
    std::chrono::steady_clock::time_point lastFailedAttempt;
    std::atomic<bool> connInProgress_;

    auto BackendMgr()
    {
        return backend_;
    }

    /**
     * @brief Stub registration is asynchronous.
     * For uds/tcp connection, stub must wait until the connection is established.
     * @param[in] timeout (in millisecond) The timeout.
     * @return Status of the call.
     */
    Status WaitForConnected(int64_t timeout);

    /**
     * @brief Add an event if a fd is ready to be read.
     * @param fd
     * @param fdConn
     * @return Status of the call.
     */
    Status AddEvent(int fd, std::shared_ptr<SockConnEntry::FdConn> fdConn);

    /**
     * @brief Return a FdConn from the pool that is ready to use
     */
    void GetNextFd(bool forceV2mtp, std::shared_ptr<SockConnEntry::FdConn> &fdConn);

    mutable WaitPost wp_;
    mutable WriterPrefRWLock mux_;

private:
    friend class ZmqSockConnHelper;
    ZmqMsgMgr *backend_;
    std::atomic<int64_t> idx_;
    Status FrontendToBackend(int fd, std::shared_ptr<SockConnEntry::FdConn> fdConn) const;
    Status SetSvcFdInvalid(int fd, std::shared_ptr<SockConnEntry::FdConn> &fdPtr);
    Status BackendToFrontend(int fd, std::shared_ptr<SockConnEntry::FdConn> fdConn);
    Status FillFdConn(int fd, std::shared_ptr<SockConnEntry::FdConn> &fdConn, ZmqCallBackFunc inFunc,
                      ZmqCallBackFunc outFunc);
};

/**
 * @brief ZmqFrontend wraps a ZMQ dealer socket and is used on the client stub
 */
class ZmqFrontend : public std::enable_shared_from_this<ZmqFrontend> {
public:
    /**
     * @brief Constructor
     * @param ctx
     * @param channel
     * @param backendMgr User provided ZmqMsgMgr for returning rpc reply
     */
    ZmqFrontend(const std::shared_ptr<ZmqContext> &ctx, std::shared_ptr<RpcChannel> channel, ZmqMsgMgr *backendMgr);

    /**
     * @brief Destructor
     */
    virtual ~ZmqFrontend();

    /**
     * @brief Initialize the structure
     * @return Status object
     */
    virtual Status Init();

    /**
     * @brief Perform shutdown
     */
    void Stop();

    /**
     * @brief Notify a rpc message is available to be picked up
     */
    Status SendMsg(SockConnEntry *cInfo, ZmqMetaMsgFrames &&que, int timeout);

    /**
     * Interrupt all running threads
     */
    void InterruptAll();

    /**
     * @brief Check if the peer is alive
     * @param threshold
     * @return true/false
     */
    bool IsPeerAlive(uint32_t threshold);

    /**
     * @brief A const function to create a ZMQ dealer socket
     * @param[in] ctx ZMQ context
     * @param[in] channel destination
     * @param[out] out On success, a ZMQ dealer socket will be created
     * @return Status object
     */
    Status InitFrontend(std::weak_ptr<ZmqContext> ctx, const std::shared_ptr<RpcChannel> &channel,
                        std::shared_ptr<ZmqSocket> &out) const;

    /**
     * @return Credential associated with the channel
     */
    RpcCredential GetCredential() const;

    /**
     * @return Gateway ID of the ZMQ dealer socket
     */
    std::string GetGatewayId() const;

    /**
     * @return ZMQ end point of the destination
     */
    std::string GetZmqEndPoint() const;

    /**
     * @brief Update the liveness according to the timeout time
     * @param[in] timeoutMs Timeout milliseconds
     */
    void UpdateLiveness(int32_t timeoutMs);

    /**
     * @brief Reset liveness to maxLiveness_
     */
    void ResetLiveness();

    /**
     * @brief return the ZmqMsgMgr used in this connection
     * @return
     */
    auto GetBackendMgr()
    {
        return backendMgr_;
    }

protected:
    std::weak_ptr<ZmqContext> ctx_;
    std::shared_ptr<RpcChannel> channel_;
    std::shared_ptr<ZmqSocket> frontend_;
    mutable WriterPrefRWLock livenessMux_;
    std::atomic<uint32_t> liveness_;
    std::atomic<uint32_t> maxLiveness_;
    std::atomic<uint32_t> heartbeatInterval_;
    ZmqMsgMgr *backendMgr_;

    /**
     * Async thread to route rpc message send/receive rpc message from the ZMQ dealer socket
     * @note zmq socket is not thread safe and if only one thread is doing the work for all
     * ZMQ sockets, it will not scale up. It is best to one thread per zmq socket.
     */
    virtual Status WorkerEntry();

private:
    std::atomic<bool> interrupt_;
    eventfd_t efd_;
    int ffd_;
    std::string heartBeatID_;
    Thread async_;
    typedef std::pair<SockConnEntry *, ZmqMetaMsgFrames> ConnMsgFrames;
    std::unique_ptr<Queue<ConnMsgFrames>> msgQue_;
    WaitPost initWp_;
    std::atomic<bool> initialized_ = { false };
    std::atomic<uint64_t> frontendQueLastHandledTimeMs_ = { 0 };

    Status RouteToZmqSocket(const MetaPb &meta, ZmqMsgFrames &&p);
    Status SendHeartBeats();
    Status ZmqSocketToBackend();
    Status HandleEvent(int timeout);
    Status BackendToFrontend();
    Status RouteToUnixSocket(const std::shared_ptr<SockConnEntry> &connInfo, const MetaPb &meta, ZmqMsgFrames &&p);
#ifdef USE_URMA
    Status ExchangeJfr();
#endif
};

struct StubInfo;
/**
 * @brief Abstract base class for ZmqStubConn.
 * Contain common members and functions.
 */
class ZmqBaseStubConn : public std::enable_shared_from_this<ZmqBaseStubConn> {
public:
    /**
     * @brief Constructor
     * @param[in] ctx ZMQ context
     */
    explicit ZmqBaseStubConn(const ZmqConnKey &key, ZmqMsgMgr *backend);

    /**
     * Destructor
     */
    virtual ~ZmqBaseStubConn() = default;

    /**
     * @brief Perform shutdown
     */
    virtual void Shutdown();

    /**
     * @brief Initialization
     * @return OK if successful
     */
    virtual Status Init();

    /**
     * @brief Interface to create a message queue for a given stub.
     * @param[in] info The zmq stub info from RegisterStub.
     * @param[out] mQue The message queue reference to return to.
     * @param[in] opts The zmq options.
     * @return Status of the call.
     */
    virtual Status CreateMsgQ(const std::shared_ptr<StubInfo> &info, ZmqMsgQueRef &mQue, const RpcOptions &opts) = 0;

    /**
     * @brief Check if the peer is alive.
     * @param[in] threshold (in second) The threshold for testing.
     * @return True if the peer has response within threshold second, false otherwise.
     */
    virtual bool IsPeerAlive(uint32_t threshold) = 0;

    /**
     * @brief Cache the current connection
     */
    virtual void CacheSession(bool) = 0;

    /**
     * @brief Stub registration is asynchronous.
     * For uds connection, stub must wait until the connection is established.
     * @param[in] info The stub info handle returned previously used to identify service.
     * @param[in] timeout (in millisecond) The timeout.
     * @return Status of the call.
     */
    Status WaitForConnect(const std::shared_ptr<StubInfo> &info, int64_t timeout);

    /**
     * Wrapper function
     * @return
     */
    Status GetMsgQ(ZmqMsgQueRef &msgQue, const RpcOptions &opts = RpcOptions(), const std::string &suffix = "")
    {
        return backendMgr_->CreateMsgQ(msgQue, opts, suffix);
    }

    /**
     * @brief Function for inherited class to report error to client
     * @return Always ok
     */
    static Status ReportErrorToClient(const std::string &qID, const MetaPb &meta, StatusCode code,
                                      const std::string &msg, ZmqMsgMgr *backendMgr);

    /**
     * @brief Return the ZmqMsgMgr used in the connection
     * @return
     */
    ZmqMsgMgr *BackendMgr()
    {
        return backendMgr_;
    }

    /**
     * Return the frontend
     * @return
     */
    virtual ZmqFrontend *Frontend() = 0;

protected:
    std::atomic<bool> interrupt_;
    const ZmqConnKey key_;
    ZmqMsgMgr *backendMgr_;
};

/**
 * @brief StubInfo contains information for a registered stub.
 */
struct StubInfo : public std::enable_shared_from_this<StubInfo> {
    // The followings don't change once set.
    int64_t stubId_ = 0;
    std::string svcName_;
    bool uds_ = false;
    bool tcpDirect_ = false;
    std::string sockName_;
    std::shared_ptr<RpcChannel> channel_;
    int channelNo_{ 0 };
    size_t connPoolSz_{ 1 };
    std::weak_ptr<ZmqBaseStubConn> conn_;
    std::weak_ptr<SockConnEntry> connInfo_;
};

/**
 * A class for establish connection for self-managed uds or tcp/ip sockets
 */
class ZmqSockConnHelper {
public:
    ZmqSockConnHelper();
    ~ZmqSockConnHelper();

    /**
     * @brief Register a stub that has uds or tcp/ip direct connection enabled.
     * @param info StubInfo
     * @param key See ZmqConnKey for the meaning of key
     * @return Status
     */
    Status RegisterStub(const std::shared_ptr<StubInfo> &info, const ZmqConnKey &key,
                        const std::shared_ptr<ZmqBaseStubConn> &conn, std::shared_ptr<SockConnEntry> &sockConn,
                        bool upsert);

    /**
     * @brief Stop all the background threads
     */
    void InterruptAll();

    void AddStubToConnList(const std::shared_ptr<StubInfo> &cInfo);

private:
    enum ConnThreadState : uint8_t { CONN_THRD_NONE = 0, CONN_THRD_INPROGESS, CONN_THRD_RUNNING };
    std::mutex reconnectMux_;
    std::mutex connThreadsMux_;
    std::condition_variable reconnectCv_;
    std::condition_variable connThreadsCv_;
    mutable WriterPrefRWLock connMapMux_;
    std::unique_ptr<ThreadPool> thrd_;
    std::unordered_map<ZmqConnKey, std::weak_ptr<SockConnEntry>> connMap_;
    typedef std::pair<std::shared_ptr<SockConnEntry>, std::shared_ptr<StubInfo>> ReconnectInfo;
    std::deque<std::weak_ptr<StubInfo>> reconnectList_;
    int numThreads_;
    ConnThreadState connState_;

    Status GetEndPoint(const ReconnectInfo &cInfo, std::string &path, int timeout);
    void HandleConnectListAfterCheck(std::shared_ptr<StubInfo> &info, std::shared_ptr<SockConnEntry> &connInfo,
                                     std::unique_lock<std::mutex> &lock, UnixSockFd &sock, ReconnectInfo &cInfo);
    void HandleConnectList();
    Status StubConnect(const ReconnectInfo &cInfo, UnixSockFd &sock);
    Status SetCacheConn(std::shared_ptr<SockConnEntry::FdConn> &fdConn, const ReconnectInfo &cInfo, UnixSockFd &sock);
    Status StartThreads();
};

/**
 * @brief A ZmqStubConn class manages a single connection.
 * Multiple ZmqStub objects can share the same ZmqStubConn as long as
 * the stubs have identical zmq end point.
 *
 * A stub joins the shared connection using the RegisterStub api
 * and disconnect using the UnregisterStub api.
 *
 * Multiple ZmqStubConn objects are managed by ZmqStubConnMgr class.
 */
class ZmqStubConn : public ZmqBaseStubConn {
public:
    /**
     * @brief Constructor
     * @param channel
     * @param ctx
     */
    explicit ZmqStubConn(const ZmqConnKey &key, ZmqMsgMgr *backend, std::shared_ptr<ZmqFrontend> frontend);

    /**
     * Destructor
     */
    ~ZmqStubConn() override;

    /**
     * @brief Used by ZmqStubConnMgr to initialize a connection.
     * @param[in] option (can contain authentication credential).
     */
    Status Init() override;

    /**
     * @brief Perform shutdown
     */
    void Shutdown() override;

    /**
     * @brief Check if the peer is alive.
     * @param[in] threshold (in second) The threshold for testing.
     * @return True if the peer has response within threshold second, false otherwise.
     */
    bool IsPeerAlive(uint32_t threshold) override;

    /**
     * @brief Interface to create a message queue for a given stub.
     * @param[in] info The zmq stub info from RegisterStub.
     * @param[out] mQue The message queue reference to return to.
     * @param[in] opts The zmq options.
     * @return Status of the call.
     */
    Status CreateMsgQ(const std::shared_ptr<StubInfo> &info, ZmqMsgQueRef &mQue, const RpcOptions &opts) override;

    /**
     * @brief Cache or un-Cache the current session
     * @param cache
     */
    void CacheSession(bool cache) override;

    /**
     * @brief Return the frontend object
     * @return
     */
    ZmqFrontend *Frontend() override
    {
        return frontend_.get();
    }

private:
    std::shared_ptr<ZmqFrontend> frontend_;
};

/**
 * @brief A singleton class for all connections for the current process.
 */
class ZmqStubConnMgrImpl final {
public:
    ZmqStubConnMgrImpl();
    ~ZmqStubConnMgrImpl();

    /**
     * @brief Main function used by ZmqStub to get a shared connection (or create a new one if not found).
     * @param[in] channel The channel to connect to.
     * @param[in] svcName The service.
     * @param[out] conn The connection returned.
     * @return A shared ZmqStubConn is returned.
     */
    Status GetConn(ZmqStub *stub, std::shared_ptr<StubInfo> &handle, const std::shared_ptr<RpcChannel> &channel,
                   int32_t timeoutMs, std::shared_ptr<ZmqBaseStubConn> &conn, std::shared_ptr<SockConnEntry> &sockConn);

    /**
     * @brief Increment shared reference count of a shared connection.
     * @note Cache a connection until the process terminates by increasing its shared pointer reference.
     * @param[in] channel The end point to increment.
     * @return Status of the call.
     */
    void IncConnRef(const ZmqConnKey &key);

    /**
     * @brief Decrement the shared reference count of a shared connection.
     * @param[in] key The channel to decrement.
     */
    void DecConnRef(const ZmqConnKey &key);

    /**
     * @brief Unregister ZmqStub handler
     */
    void UnregisterStub();

    /**
     * @brief Encode destination (either a ZMQ socket or a domain socket) onto the queue name
     * @param info
     * @return OK if successful
     */
    static std::string EncodeQueName(const std::shared_ptr<StubInfo> &info);

    /**
     * @brief Perform instance shutdown
     */
    void Shutdown();

    /**
     * @brief Provide a poller for any party that is interested
     * @return
     */
    ZmqEpoll *GetNextPoller();

    void AutoReconnect(std::shared_ptr<SockConnEntry> &connInfo);

private:
    friend class ZmqSockConnHelper;
    std::atomic<bool> interrupt_;
    std::atomic<bool> initialize_;
    mutable WriterPrefRWLock stubMux_;
    mutable WriterPrefRWLock connMux_;
    std::shared_ptr<ZmqContext> ctx_;
    std::atomic<int64_t> stubId_;
    std::atomic<int64_t> nextMgrId_;
    std::atomic<int64_t> nextPollerId_;
    std::unordered_map<ZmqConnKey, std::shared_ptr<ZmqBaseStubConn>> ref_;
    std::vector<std::shared_ptr<ZmqMsgMgr>> backends_;
    std::vector<std::unique_ptr<ZmqEpoll>> pollers_;
    std::unique_ptr<ZmqSockConnHelper> sockConnHelper_;
    std::unordered_map<ZmqConnKey, std::weak_ptr<ZmqBaseStubConn>> connMap_;
    std::unordered_map<int64_t, std::shared_ptr<StubInfo>> stubInfos_;
    std::unique_ptr<Thread> unregStubThrd_{ nullptr };
    WaitPost cvLock_;

    Status RegisterStub(ZmqStub *stub, std::shared_ptr<StubInfo> &handle, const std::shared_ptr<RpcChannel> &channel);

    ZmqMsgMgr *GetNextBackendMgr();

    std::shared_ptr<ZmqContext> GetZmqContext();

    static ZmqConnKey CreatePrimaryKey(StubInfo *info);

    /**
     * @brief Reverse of EncodeQueName to retrieve the destination (either a ZMQ socket or a domain socket)
     * @param[in] queName
     * @param[out] frontend
     * @param[in] connInfo
     * @return OK if successful
     */
    static Status DecodeQueName(const std::string &queName, ZmqFrontend *&frontend, SockConnEntry *&connInfo,
                                int64_t &stubId);

    /**
     * @brief A call back function to dispatch the messages to the designated destination
     * @return
     */
    virtual Status Outbound(const std::string &, ZmqMetaMsgFrames &&);

    Status StartAllThreads();

    std::shared_ptr<ZmqBaseStubConn> GetExistingConn(const ZmqConnKey &key);
    Status GetZmqFrontendFromPtr(int64_t stubId, ZmqFrontend *rawPtr, std::shared_ptr<ZmqFrontend> &frontend);
};

class ZmqStubConnMgr final {
public:
    static ZmqStubConnMgr *Instance();
    ~ZmqStubConnMgr();
    void AfterFork();

    /**
     * @brief Passkey idiom.
     * @note This ensure no one can call the ZmqStubConnMgr constructor
     */
    class Token {
    public:
        ~Token() = default;

    private:
        friend ZmqStubConnMgr *ZmqStubConnMgr::Instance();
        Token() = default;
    };

    /**
     * Constructor but only Instance() can invoke it.
     */
    ZmqStubConnMgr(Token t, Logging &inject);

    /**
     * @brief Main function used by ZmqStub to get a shared connection (or create a new one if not found).
     * @param[in] channel The channel to connect to.
     * @param[in] svcName The service.
     * @param[out] conn The connection returned.
     * @return A shared ZmqStubConn is returned.
     */
    Status GetConn(ZmqStub *stub, std::shared_ptr<StubInfo> &handle, const std::shared_ptr<RpcChannel> &channel,
                   int32_t timeoutMs, std::shared_ptr<ZmqBaseStubConn> &conn, std::shared_ptr<SockConnEntry> &sockConn)
    {
        std::shared_lock<std::shared_timed_mutex> lock(implMux_);
        CHECK_FAIL_RETURN_STATUS(impl_ != nullptr, K_SHUTTING_DOWN, "Instance shut down");
        return impl_->GetConn(stub, handle, channel, timeoutMs, conn, sockConn);
    }

    /**
     * @brief Increment shared reference count of a shared connection.
     * @note Cache a connection until the process terminates by increasing its shared pointer reference.
     * @param[in] channel The end point to increment.
     * @return Status of the call.
     */
    void IncConnRef(const ZmqConnKey &key)
    {
        std::shared_lock<std::shared_timed_mutex> lock(implMux_);
        if (impl_) {
            impl_->IncConnRef(key);
        }
    }

    /**
     * @brief Decrement the shared reference count of a shared connection.
     * @param[in] key The channel to decrement.
     */
    void DecConnRef(const ZmqConnKey &key)
    {
        std::shared_lock<std::shared_timed_mutex> lock(implMux_);
        if (impl_) {
            impl_->DecConnRef(key);
        }
    }

    /**
     * @brief Return a poller for any party that is interested
     * @return
     */
    ZmqEpoll *GetPoller()
    {
        std::shared_lock<std::shared_timed_mutex> lock(implMux_);
        if (impl_) {
            return impl_->GetNextPoller();
        }
        return nullptr;
    }

    static bool LoggingOn()
    {
        return logging_;
    }

private:
    static bool logging_;
    friend class ZmqSockConnHelper;
    friend class SockConnEntry;
    static std::once_flag init_;
    static std::unique_ptr<ZmqStubConnMgr> instance_;
    mutable std::shared_timed_mutex implMux_;
    std::unique_ptr<ZmqStubConnMgrImpl> impl_;
    Logging &log_;  // Not used but force this singleton dependency on the singleton Logging.
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_ZMQ_STUB_CONN_H
