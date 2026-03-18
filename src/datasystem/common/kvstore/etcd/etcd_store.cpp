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

#include "datasystem/common/kvstore/etcd/etcd_store.h"

#include <etcd/api/mvccpb/kv.pb.h>
#include <signal.h>
#include <sstream>
#include <thread>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/kvstore/etcd/etcd_keep_alive.h"
#include "datasystem/common/kvstore/etcd/grpc_session.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/log/spdlog/provider.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/utils/status.h"
#include "etcd/api/etcdserverpb/rpc.grpc.pb.h"
#include "etcd/api/etcdserverpb/rpc.pb.h"

DS_DECLARE_string(etcd_address);
DS_DEFINE_string(other_cluster_names, "", "Specify other az names using the same etcd. Split by ','");
DS_DEFINE_validator(other_cluster_names, &Validator::ValidateOtherAzNames);
DS_DECLARE_uint32(node_timeout_s);
DS_DECLARE_uint32(node_dead_timeout_s);
DS_DECLARE_bool(auto_del_dead_node);
DS_DECLARE_string(cluster_name);
DS_DEFINE_string(host_id_env_name, "", "Environment variable name used to obtain the current host_id.");

DS_DEFINE_string(etcd_username, "", "Username for etcd authentication");
DS_DEFINE_string(etcd_password, "", "Password for etcd authentication");
DS_DEFINE_uint32(etcd_token_refresh_interval_s, 30, "etcd authentication token refresh interval in seconds");

namespace datasystem {

std::string KeepAliveValue::ToString() const
{
    std::stringstream ss;
    ss << timestamp;
    ss << ";" << state;
    if (!hostId.empty()) {
        ss << ";" << hostId;
    }
    return ss.str();
}

Status KeepAliveValue::FromString(const std::string &str, KeepAliveValue &value)
{
    value = {};
    auto firstPos = str.find(';');
    if (firstPos == std::string::npos) {
        RETURN_STATUS(K_INVALID,
                      FormatString("Invalid keep alive value: %s, must have at least two fields split by ';'", str));
    }
    auto secondPos = str.find(';', firstPos + 1);
    value.timestamp = str.substr(0, firstPos);
    if (secondPos == std::string::npos) {
        value.state = str.substr(firstPos + 1);
        return Status::OK();
    }
    value.state = str.substr(firstPos + 1, secondPos - firstPos - 1);
    value.hostId = str.substr(secondPos + 1);
    return Status::OK();
}

EtcdStore::EtcdStore(const std::string &address) : address_(address)
{
}

EtcdStore::EtcdStore(const std::string &address, const std::string &etcdCa, const SensitiveValue &etcdCert,
                     const SensitiveValue &etcdKey, std::string targetNameOverride)
    : address_(address),
      clientCurveKit_({ etcdCa, etcdCert, etcdKey, std::move(targetNameOverride) }),
      isRouterClientCurveConnect_(true)
{
}

Status EtcdStore::Init()
{
    if (!isRouterClientCurveConnect_) {
        RETURN_IF_NOT_OK(GrpcSession<etcdserverpb::KV>::CreateSession(address_, rpcSession_));
        RETURN_IF_NOT_OK(GrpcSession<etcdserverpb::Lease>::CreateSession(address_, leaseSession_));
    } else {
        RETURN_IF_NOT_OK(GrpcSession<etcdserverpb::KV>::CreateSession(address_, rpcSession_, clientCurveKit_.etcdCa,
                                                                      clientCurveKit_.etcdCert, clientCurveKit_.etcdKey,
                                                                      clientCurveKit_.etcdNameOverride));
        RETURN_IF_NOT_OK(GrpcSession<etcdserverpb::Lease>::CreateSession(
            address_, leaseSession_, clientCurveKit_.etcdCa, clientCurveKit_.etcdCert, clientCurveKit_.etcdKey,
            clientCurveKit_.etcdNameOverride));
    }
    RETURN_IF_EXCEPTION_OCCURS(keepAlivePool_ =
                                   std::make_unique<ThreadPool>(NUM_KEEPALIVE_THREADS, 0, "EtcdKeepAlive"));
    RETURN_IF_EXCEPTION_OCCURS(watchRunPool_ = std::make_unique<ThreadPool>(NUM_WATCH_THREADS, 0, "EtcdWatch"));
    GrpcSessionBase::SetIsKeepAliveTimeoutHandler(std::bind(&EtcdStore::IsKeepAliveTimeout, this));
    return Status::OK();
}

Status EtcdStore::Authenticate(std::string username, const SensitiveValue &password, uint32_t tokenRefreshInterval)
{
    if (username.empty() || password.Empty()) {
        return Status::OK();
    }

    // Save credentials for the background refresh loop
    username_ = std::move(username);
    password_ = password;
    tokenRefreshInterval_ = tokenRefreshInterval;

    if (authSession_ == nullptr) {
        if (!isRouterClientCurveConnect_) {
            RETURN_IF_NOT_OK(GrpcSession<etcdserverpb::Auth>::CreateSession(address_, authSession_));
        } else {
            RETURN_IF_NOT_OK(GrpcSession<etcdserverpb::Auth>::CreateSession(
                address_, authSession_, clientCurveKit_.etcdCa, clientCurveKit_.etcdCert, clientCurveKit_.etcdKey,
                clientCurveKit_.etcdNameOverride));
        }
    }

    // Initial synchronous authentication to secure the first token
    RETURN_IF_NOT_OK(PerformAuthRequest());

    // Start the background token refresh thread if not already running
    if (tokenRefreshThread_ == nullptr) {
        stopTokenRefresh_.store(false);
        tokenRefreshThread_ = std::make_unique<Thread>(&EtcdStore::TokenRefreshLoop, this);
    }

    return Status::OK();
}

Status EtcdStore::PerformAuthRequest()
{
    etcdserverpb::AuthenticateRequest req;
    req.set_name(username_);
    req.set_password(password_.GetData(), password_.GetSize());
    etcdserverpb::AuthenticateResponse rsp;

    // Send the RPC request
    Status status = authSession_->SendRpc("Authenticate", req, rsp, &etcdserverpb::Auth::Stub::Authenticate, "");
    if (status.IsOk()) {
        // Acquire write lock to safely update the token
        std::unique_lock<std::shared_mutex> lock(tokenMutex_);
        authToken_ = rsp.token();
    }

    return status;
}

void EtcdStore::TokenRefreshLoop()
{
    LOG(INFO) << "Etcd token background refresh thread started. The interval is " << tokenRefreshInterval_
              << " seconds.";
    Timer timer;
    const uint32_t sleepIntervalMs = 10;
    while (!stopTokenRefresh_.load()) {
        while (timer.ElapsedSecond() < tokenRefreshInterval_ && !stopTokenRefresh_) {
            std::this_thread::sleep_for(std::chrono::milliseconds(sleepIntervalMs));
        }

        if (stopTokenRefresh_.load()) {
            break;
        }
        timer.Reset();

        // Proactively refresh the token
        LOG_IF_ERROR(PerformAuthRequest(), "Failed to refresh etcd auth token.");
    }
    LOG(INFO) << "Etcd token background refresh thread stopped.";
}

std::string EtcdStore::GetAuthToken()
{
    // Acquire read lock, allowing concurrent reads by multiple RPC calls
    std::shared_lock<std::shared_mutex> lock(tokenMutex_);
    return authToken_;
}

EtcdStore::~EtcdStore()
{
    LOG(INFO) << "EtcdStore is ready to destruct";
    LOG_IF_ERROR(Shutdown(), "Destructor shutting down EtcdStore, but an error was given.");
}

void EtcdStore::Close()
{
    LOG_IF_ERROR(Shutdown(), "Close operation shut down EtcdStore, but an error was given.");
}

Status EtcdStore::WatchShutdown()
{
    // Main thread and retry thread share watchEvents_ pointer
    WriteLock lock(&watchLock_);

    if (watchEvents_) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(watchEvents_->Shutdown(),
                                         "EtcdStore shuts down EtcdWatch but an error was given. Ignore and continue");
        watchRunPool_.reset();
        watchEvents_.reset();
    }
    return Status::OK();
}

Status EtcdStore::Shutdown()
{
    // Shutdown protocol for keep alive:
    // 1) Get a lock. This protects the leaseKeepAlive_ from being changed (the running thread drops and recreates it
    //    if it is in an error handling retry loop).
    // 2) Set the exit flag. This will flag the outer monitoring thread loop so that it will exit once its child thread
    //    quits.
    // 3) Call shutdown to the lease keep alive if it is running.  This causes the inner child thread to interrupt and
    //    quit.
    // 4) Release the lock. This allows the parent monitoring thread to move forward and quit gracefully if it was stuck
    //    on the lock during a retry attempt of error paths.
    // 5) shutdown the thread pool. Waits for the keep alive monitoring thread and inner keep alive thread to exit.
    // 6) Free the keep alive if it was not already done.  Lock not needed here as all threads are gone.
    //
    // Note:
    // The running child thread doing keep-alive does not hold any read lock while its using the leaseKeepAlive_.
    // This shutdown protocol is the only other user of the pointer and it will always shutdown all the threads first
    // before freeing the leaseKeepAlive_, therefore a read lock is not needed in the main loop.
    // Unblock pending events
    if (leaseSession_) {
        leaseSession_->Shutdown();
    }

    {
        WriteLock lock(&keepAliveLock_);
        keepAliveExit_ = true;
        if (leaseKeepAlive_ != nullptr) {
            // Shutdown any running keep alive.
            Status rc = leaseKeepAlive_->Shutdown();
            if (rc.IsError()) {
                LOG(WARNING) << "EtcdStore shuts down EtcdKeepAlive but an error was given. Ignore and continue: "
                             << rc.ToString();
            }
        }
    }

    keepAlivePool_.reset();  // waits for child threads to quit
    leaseKeepAlive_.reset();

    watchExit_ = true;
    LOG_IF_ERROR(WatchShutdown(), "EtcdStore WatchShutdown failed");

    if (tokenRefreshThread_ != nullptr && tokenRefreshThread_->joinable()) {
        stopTokenRefresh_ = true;
        tokenRefreshThread_->join();
    }

    return Status::OK();
}

Status EtcdStore::CreateTable(const std::string &tableName, const std::string &tablePrefix)
{
    std::lock_guard<std::shared_timed_mutex> lck(mutex_);
    CHECK_FAIL_RETURN_STATUS(tableMap_.find(tableName) == tableMap_.end(), K_DUPLICATED,
                             "The table already exists. tableName:" + tableName);
    if (!FLAGS_cluster_name.empty()) {
        tableMap_.emplace(tableName, "/" + FLAGS_cluster_name + tablePrefix);
    } else {
        tableMap_.emplace(tableName, tablePrefix);
    }

    if (!FLAGS_other_cluster_names.empty()) {
        for (auto &azName : Split(FLAGS_other_cluster_names, ",")) {
            if (azName != FLAGS_cluster_name) {
                std::lock_guard<std::shared_timed_mutex> lck(otherAzTblMutex_);
                otherAzTableMap_[tableName].emplace_back("/" + azName + tablePrefix);
            }
        }
    }

    return Status::OK();
}

Status EtcdStore::DropTable(const std::string &tableName)
{
    std::lock_guard<std::shared_timed_mutex> lck(mutex_);
    auto iter = tableMap_.find(tableName);
    CHECK_FAIL_RETURN_STATUS(iter != tableMap_.end(), K_RUNTIME_ERROR,
                             "The table does not exist. tableName:" + tableName);
    etcdserverpb::DeleteRangeRequest req;
    std::string etcdKey = iter->second + "/";
    req.set_key(etcdKey);
    req.set_range_end(StringPlusOne(etcdKey));
    etcdserverpb::DeleteRangeResponse rsp;
    RETURN_IF_NOT_OK(rpcSession_->SendRpc("DropTable::etcd_kv_DeleteRange", req, rsp,
                                          &etcdserverpb::KV::Stub::DeleteRange, GetAuthToken()));
    iter = tableMap_.erase(iter);
    return Status::OK();
}

Status EtcdStore::Put(const std::string &tableName, const std::string &key, const std::string &value)
{
    return Put(tableName, key, value, nullptr);
}

Status EtcdStore::Put(const std::string &tableName, const std::string &key, const std::string &value, int32_t timeoutMs,
                      uint64_t asyncElapse)
{
    return Put(tableName, key, value, nullptr, timeoutMs, asyncElapse);
}

Status EtcdStore::Put(const std::string &tableName, const std::string &key, const std::string &value, int64_t *version,
                      int32_t timeoutMs, uint64_t asyncElapse)
{
    std::shared_lock<std::shared_timed_mutex> lck(mutex_);
    TableMap::const_iterator iter = tableMap_.find(tableName);
    CHECK_FAIL_RETURN_STATUS(iter != tableMap_.cend(), K_RUNTIME_ERROR,
                             "The table does not exist. tableName:" + tableName);
    CHECK_FAIL_RETURN_STATUS(
        !(keepAliveTimeoutTimer_.ElapsedMilliSecond() > FLAGS_node_dead_timeout_s * MS_PER_SECOND
          && FLAGS_auto_del_dead_node),
        K_RUNTIME_ERROR,
        FormatString("local node is failed, keepAliveTimeoutTimer ElapsedMilliSecond %ld,  not put data to etcd",
                     keepAliveTimeoutTimer_.ElapsedMilliSecond()));
    etcdserverpb::PutRequest req;
    std::string etcdKey = iter->second + "/" + key;
    req.set_key(etcdKey);
    req.set_value(value);
    etcdserverpb::PutResponse rsp;
    VLOG(1) << "Calling rpc to put object with key " << etcdKey;
    RETURN_IF_NOT_OK(rpcSession_->SendRpc("Put::etcd_kv_Put", req, rsp, &etcdserverpb::KV::Stub::Put, GetAuthToken(),
                                          value.size(), timeoutMs, asyncElapse));
    if (version != nullptr) {
        *version = rsp.prev_kv().version() + 1;
    }
    return Status::OK();
}

Status EtcdStore::BatchPut(const std::unordered_map<std::string, BatchInfoPutToEtcd> &metaInfos)
{
    std::shared_lock<std::shared_timed_mutex> lck(mutex_);
    Transaction txn(GetAuthToken());
    txn.StartTransaction();
    for (const auto &info : metaInfos) {
        const auto &tableName = info.second.tableName;
        VLOG(1) << "Calling rpc to put object with key " << info.second.etcdKey;
        TableMap::const_iterator iter;
        {
            std::shared_lock<std::shared_timed_mutex> lck(mutex_);
            iter = tableMap_.find(tableName);
        }
        CHECK_FAIL_RETURN_STATUS(iter != tableMap_.cend(), K_RUNTIME_ERROR,
                                 "The table does not exist. tableName:" + tableName);
        auto key = info.second.etcdKey;
        std::string etcdKey = iter->second + "/" + key;
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(txn.Put(etcdKey, info.second.meta), "Transaction Put failed");
    }
    return txn.Commit();
}

Status EtcdStore::PutWithLeaseId(const std::string &tableName, const std::string &key, const std::string &value,
                                 const int64_t leaseId)
{
    std::shared_lock<std::shared_timed_mutex> lck(mutex_);
    TableMap::const_iterator iter = tableMap_.find(tableName);
    CHECK_FAIL_RETURN_STATUS(iter != tableMap_.cend(), K_RUNTIME_ERROR,
                             "The table does not exist. tableName:" + tableName);
    etcdserverpb::PutRequest req;
    std::string etcdKey = iter->second + "/" + key;
    req.set_key(etcdKey);
    req.set_value(value);
    req.set_lease(leaseId);
    etcdserverpb::PutResponse rsp;
    return rpcSession_->SendRpc("Put::etcd_kv_Put", req, rsp, &etcdserverpb::KV::Stub::Put, GetAuthToken(),
                                value.size());
}

Status EtcdStore::GetLeaseID(const int64_t ttlInSec, std::atomic<int64_t> &leaseId)
{
    CHECK_FAIL_RETURN_STATUS(leaseSession_ != nullptr, K_KVSTORE_ERROR,
                             "The lease session was not created successfully");
    etcdserverpb::LeaseGrantRequest req;
    etcdserverpb::LeaseGrantResponse rsp;
    req.set_ttl(ttlInSec);
    req.set_id(0);
    const int secToMs = 1000;
    auto timeoutMs = std::min<uint64_t>(ttlInSec * secToMs, MIN_RPC_TIMEOUT_MS);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        leaseSession_->AsyncSendRpc("LeaseGrant", req, rsp, &etcdserverpb::Lease::Stub::AsyncLeaseGrant, GetAuthToken(),
                                    timeoutMs),
        "LeaseGrant error: " + rsp.error());
    leaseId = rsp.id();
    return Status::OK();
}

Status EtcdStore::GetLeaseIDWithReconnectIfError(const int64_t ttlInSec, std::atomic<int64_t> &leaseId)
{
    INJECT_POINT("GetLeaseIDWithReconnectIfError.Error");
    Status rc = GetLeaseID(ttlInSec, leaseId);
    LOG_IF_ERROR(rc, "GetLeaseID failed");
    if (rc.GetCode() == K_RPC_UNAVAILABLE) {
        std::unique_ptr<GrpcSession<etcdserverpb::Lease>> leaseSession;
        RETURN_IF_NOT_OK(GrpcSession<etcdserverpb::Lease>::CreateSession(address_, leaseSession));
        leaseSession_->Shutdown();
        leaseSession_ = std::move(leaseSession);
    }
    return rc;
}

Status EtcdStore::RunKeepAliveTask(Timer &keepAliveTimeoutTimer, Timer &deathTimer)
{
    std::future<Status> fStatus;
    // This block is protected by write lock for the leaseKeepAlive_ pointer to make it threadsafe
    // during a shutdown (cannot drop and recreate the pointer if shutdown is in progress).
    {
        INJECT_POINT("worker.RunKeepAliveTask");
        WriteLock lock(&keepAliveLock_);
        CHECK_FAIL_RETURN_STATUS(!(keepAliveExit_), K_OK,
                                 "KeepAlive has been interrupted for shutdown. Do not recreate.");
        // If this is a retry case where we are relaunching the keep alive, then explicitly destroy the old instance of
        // the class first and then recreate it.
        if (leaseKeepAlive_) {
            LOG_IF_ERROR(leaseKeepAlive_->Shutdown(), "EtcdKeepAlive shutdown failed");
            leaseKeepAlive_.reset();
        }
        LOG(INFO) << "Creating lease with expiry time: " << FLAGS_node_timeout_s;
        RETURN_IF_NOT_OK(GetLeaseIDWithReconnectIfError(FLAGS_node_timeout_s, leaseId_));
        leaseKeepAlive_ = std::make_unique<EtcdKeepAlive>(address_, leaseId_);
        LOG(INFO) << "Creating new lease KeepAlive object for lease " << leaseId_
                  << " with heartbeat interval timeout: " << leaseKeepAlive_->GetLeaseRenewIntervalMs();
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(leaseKeepAlive_->Init(GetAuthToken()), "Could not initialize lease keepalive");
        keepAliveTimeout_ = false;
        RETURN_IF_NOT_OK(AutoCreate());
        // Now invoke keep alive thread main loop
        auto traceId = Trace::Instance().GetTraceID();
        fStatus = keepAlivePool_->Submit([this, traceId, &keepAliveTimeoutTimer] {
            TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
            Status res = leaseKeepAlive_->Run(keepAliveTimeoutTimer);
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
                res, FormatString("etcd keep alive run failed, keep alive timer ElapsedMilliSecond: %ld",
                                  keepAliveTimeoutTimer.ElapsedMilliSecond()));
            return Status::OK();
        });
        deathTimer.Clear();
    }

    // Wait for this child thread and then return its rc to caller
    // Do not process the return code here.
    fStatus.wait();
    return fStatus.get();
}

Status EtcdStore::AutoCreate()
{
    INJECT_POINT("AutoCreate");
    LOG(INFO) << "Sending cluster node to etcd and establish lease.";
    // set timestamp before each put
    int64_t timeStamp = std::chrono::system_clock::now().time_since_epoch().count();
    keepAliveValue_.timestamp = std::to_string(timeStamp);
    CHECK_FAIL_RETURN_STATUS(!keepAliveValue_.state.empty(), K_INVALID, "Node state should not be empty.");
    RETURN_IF_NOT_OK(PutWithLeaseId(keepAliveTableName_, keepAliveKey_, keepAliveValue_.ToString(), leaseId_));
    // Only use "start" and "restart" tag the first time doing Put()
    // After that remove the tag and append "recover", because the next time using it should be something
    // like network recovery.
    if (keepAliveValue_.state == "start" || keepAliveValue_.state == "restart") {
        keepAliveValue_.state = "recover";
    }
    return Status::OK();
}

Status EtcdStore::InitKeepAlive(const std::string &tableName, const std::string &key, bool isRestart,
                                bool isEtcdAvailableWhenStart)
{
    keepAliveTableName_ = tableName;
    keepAliveKey_ = key;
    std::string hostId;
    if (!FLAGS_host_id_env_name.empty()) {
        hostId = GetStringFromEnv(FLAGS_host_id_env_name.c_str(), "");
        if (hostId.empty()) {
            LOG(WARNING) << FormatString("host_id env [%s] is empty when worker registers to etcd.",
                                         FLAGS_host_id_env_name);
        } else {
            LOG(INFO) << "Host id is " << hostId << " from env " << FLAGS_host_id_env_name;
        }
    }
    // "_" is a placeholder, it will be replaced by timestamp later when a connection to etcd is built.
    keepAliveValue_.timestamp = "_";
    keepAliveValue_.hostId = hostId;
    if (!isEtcdAvailableWhenStart) {
        keepAliveValue_.state = "d_rst";
    } else if (isRestart) {
        keepAliveValue_.state = "restart";
    } else {
        keepAliveValue_.state = "start";
    }
    return LaunchKeepAliveThreads();
}

Status EtcdStore::UpdateNodeState(const std::string &state)
{
    CHECK_FAIL_RETURN_STATUS(!IsKeepAliveTimeout(), K_NOT_READY,
                             "The key written to the cluster table must be bound to a lease");
    KeepAliveValue value = keepAliveValue_;
    value.state = state;
    RETURN_IF_NOT_OK(PutWithLeaseId(keepAliveTableName_, keepAliveKey_, value.ToString(), leaseId_));
    return Status::OK();
}

Status EtcdStore::HandleKeepAliveFailed()
{
    std::string etcdKey;
    {
        std::shared_lock<std::shared_timed_mutex> lck(mutex_);
        TableMap::const_iterator iter = tableMap_.find(keepAliveTableName_);
        CHECK_FAIL_RETURN_STATUS(iter != tableMap_.cend(), K_RUNTIME_ERROR,
                                 "The table does not exist. tableName:" + keepAliveTableName_);
        etcdserverpb::PutRequest req;
        etcdKey = iter->second + "/" + keepAliveKey_;
    }
    LOG(INFO) << FormatString("Due to network failure, a fake node deletion event[key: %s] needs to be generated",
                              etcdKey);
    mvccpb::Event fakeEvent;
    auto fakeKv = fakeEvent.mutable_kv();
    fakeKv->set_key(std::move(etcdKey));
    fakeKv->set_value("");
    fakeEvent.set_type(mvccpb::Event_EventType::Event_EventType_DELETE);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(eventHandler_ != nullptr, K_RUNTIME_ERROR, "eventHandler_ is nullptr");
    eventHandler_(std::move(fakeEvent));
    return Status::OK();
}

Status EtcdStore::LaunchKeepAliveThreads()
{
    // Launch an outer thread to act as a parent that waits for the child thread to return a response (and drives retry
    // logic to re-establish a new lease and connection to etcd).
    // RunKeepAlive() will launch and execute the second (child) thread.
    auto traceId = Trace::Instance().GetTraceID();
    LOG(INFO) << "Starting KeepAlive monitoring thread";
    keepAlivePool_->Execute([this, traceId] {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
        int RETRY_DELAY = 5;
        static int networkFailedConfirmMinTimes = 3;
        int networkFailedConfirmTimes = 0;
        int64_t leaseId = 0;
        bool needHandleKeepAlivefailed = true;  // Avoid generating multiple duplicate fake events.
        keepAliveTimeoutTimer_.Reset();
        Timer deathTimer;
        while (!keepAliveExit_) {
            Status rc = RunKeepAliveTask(keepAliveTimeoutTimer_, deathTimer);
            INJECT_POINT("EtcdStore.LaunchKeepAliveThreads.shutdown", [&rc]() {
                rc = Status::OK();
                return;
            });
            if (rc.IsOk()) {
                // If the thread completed with success, it is a normal shutdown case so we terminate this loop and
                // allow the parent monitor thread to gracefully exit.
                LOG(INFO) << "Keep alive thread completed with success rc";
                keepAliveExit_ = true;
                continue;
            }
            keepAliveTimeout_ = true;
            if (keepAliveTimeoutTimer_.ElapsedMilliSecond() < EtcdKeepAlive::GetLeaseExpiredMs()) {
                continue;
            }
            LOG(INFO) << "Keep alive task completed with error: " << rc.ToString()
                      << "keep alive failed time: " << keepAliveTimeoutTimer_.ElapsedMilliSecond();
            if (leaseId != leaseId_) {
                leaseId = leaseId_;
                networkFailedConfirmTimes = 0;
                needHandleKeepAlivefailed = true;
            }
            bool etcdAvaliableWhenNetworkFaile = false;
            if (needHandleKeepAlivefailed && checkEtcdStateWhenNetworkFailedHandler_ != nullptr) {
                etcdAvaliableWhenNetworkFaile = checkEtcdStateWhenNetworkFailedHandler_();
            }
            if (etcdAvaliableWhenNetworkFaile && ++networkFailedConfirmTimes >= networkFailedConfirmMinTimes) {
                auto rc1 = HandleKeepAliveFailed();
                LOG_IF_ERROR(rc1, "add remove event failed when keep alive failed");
                if (rc1.IsOk()) {
                    LOG(INFO) << "Confirmed to be a network failure";
                    needHandleKeepAlivefailed = false;
                    // Theoretically, the kill signal should not be sent through this timer, but in order to prevent the
                    // suicide mechanism from failing due to unknown reasons, this timer is used as a guarantee, and the
                    // deviation is set to 10s.
                    deathTimer.AdjustTimeoutAndReset((FLAGS_node_dead_timeout_s - FLAGS_node_timeout_s + 10)
                                                     * SECS_TO_MS);
                }
            } else if (deathTimer.IsTimeout()) {
                LOG(INFO) << "The node scaling time has been reached and the lease has not been renewed. Scaling down "
                             "should be performed.";
                Provider::Instance().FlushLogs();
                (void)raise(SIGKILL);
            } else {
                LOG(INFO) << "Etcd is currently not available for keepAlive, we only need to retry, no other "
                             "additional operations are required.";
            }
            if (etcdAvaliableWhenNetworkFaile
                && keepAliveTimeoutTimer_.ElapsedMilliSecond() > FLAGS_node_dead_timeout_s * MS_PER_SECOND
                && FLAGS_auto_del_dead_node) {
                LOG(WARNING) << FormatString(
                    "local node failed time %ld has been reached %ld, need kill local worker, Scaling down.");
                Provider::Instance().FlushLogs();
                (void)raise(SIGKILL);
            }
            // Allow some time for network to recover and then retry to create the keep alive again.
            INJECT_POINT("EtcdStore.LaunchKeepAliveThreads.loopQuickly",
                         [&RETRY_DELAY](int timeS) { RETRY_DELAY = timeS; });
            std::this_thread::sleep_for(std::chrono::seconds(RETRY_DELAY));
            LOG(INFO) << "Retry to recreate keep alive";
        }
        LOG(INFO) << "Shutting down keep alive monitoring thread.";
    });
    return Status::OK();
}

Status EtcdStore::InitWatch(std::unique_ptr<std::unordered_map<std::string, int64_t>> &&prefixMap,
                            const std::function<Status()> &writable)
{
    // Main thread and retry thread share watchEvents_ pointer
    WriteLock lock(&watchLock_);
    if (!isRouterClientCurveConnect_) {
        watchEvents_ = std::make_unique<EtcdWatch>(address_, std::move(prefixMap));
    } else {
        watchEvents_ = std::make_unique<EtcdWatch>(address_, std::move(prefixMap), clientCurveKit_);
    }
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(eventHandler_ != nullptr, K_RUNTIME_ERROR,
                                         "checkEtcdStateWhenNetworkFailedHandler_ is nullptr");
    watchEvents_->SetWatchEventHandler(eventHandler_);
    watchEvents_->SetCheckEtcdStateHandler(writable);
    using PrefixSearchForWatch = Status (EtcdStore::*)(const std::string &, EtcdRangeGetVector &, int64_t &);
    watchEvents_->SetPrefixSearchHandler(std::bind((PrefixSearchForWatch)&EtcdStore::PrefixSearch, this,
                                                   std::placeholders::_1, std::placeholders::_2,
                                                   std::placeholders::_3));
    watchEvents_->SetUpdateClusterInfoInRocksDbHandler(updateClusterInfoInRocksDbHandler_);
    LOG_IF_ERROR(watchEvents_->Init(GetAuthToken()), "Could not initiate watch for monitoring events");
    return Status::OK();
}

Status EtcdStore::ReInitWatch()
{
    RETURN_IF_NOT_OK(watchEvents_->RetrieveEventActively());
    // Create a new watch stream
    auto rc = watchEvents_->Init(GetAuthToken());
    if (rc.IsError()) {
        watchEvents_->ShutdownEtcd();
    }
    return rc;
}

Status EtcdStore::WatchRun()
{
    const int RETRY_DELAY = 5;

    if (watchExit_) {
        return Status::OK();
    }
    // Start watch and monitor for errors
    do {
        // Start background threads to poll streams
        Status rc = watchEvents_->Run();
        INJECT_POINT("EtcdStore.WatchRun.shutdown", [this]() {
            this->watchExit_ = true;
            return Status::OK();
        });
        while (!watchExit_ && rc.IsError()) {
            LOG(WARNING) << "Etcd watch failed, with rc: " << rc.ToString() << ", try to watch again.";

            // Before reinitializing etcd watch, we only shut down the producer, because during etcdwatch failure,
            // there are still other threads generating events.
            watchEvents_->CloseProducer();

            // Don't sleep if already destructor is called
            if (watchExit_) {
                return Status::OK();
            }
            // Wait for Etcd to recover or restart
            std::this_thread::sleep_for(std::chrono::seconds(RETRY_DELAY));
            // unset the watch exit flag
            RETURN_OK_IF_TRUE(watchExit_.exchange(false));

            rc = ReInitWatch();
            if (rc.IsOk()) {
                LOG(INFO) << "Recreating watch success";
            } else {
                LOG(ERROR) << "Fail to reinitialize watch, with rc: " << rc.ToString();
                continue;
            }
        };
    } while (!watchExit_);

    return Status::OK();
}

Status EtcdStore::WatchEvents(const std::string &tableName, const std::string &key, bool ifWatchOtherAz,
                              int64_t startRevision)
{
    return WatchEvents({ { tableName, key, ifWatchOtherAz, startRevision } });
}

Status EtcdStore::WatchEvents(const std::vector<WatchElement> &watchKeys)
{
    RETURN_IF_NOT_OK(CreateTable(ETCD_HEALTH_CHECK_TABLE, ETCD_HEALTH_CHECK_TABLE));
    auto prefixToWatch = std::make_unique<std::unordered_map<std::string, int64_t>>();

    for (const auto &watchKey : watchKeys) {
        const auto &tableName = watchKey.tableName;
        const auto &key = watchKey.key;
        bool ifWatchOtherAz = watchKey.ifWatchOtherAz;
        auto startRevision = watchKey.startRevision;
        {
            std::shared_lock<std::shared_timed_mutex> lck(mutex_);
            TableMap::const_iterator iter = tableMap_.find(tableName);
            CHECK_FAIL_RETURN_STATUS(iter != tableMap_.cend(), K_RUNTIME_ERROR,
                                     "The table does not exist. tableName:" + tableName);
            prefixToWatch->emplace(iter->second + "/" + key, startRevision);
        }
        if (ifWatchOtherAz) {
            std::shared_lock<std::shared_timed_mutex> lck(otherAzTblMutex_);
            auto iter = otherAzTableMap_.find(tableName);
            if (iter == otherAzTableMap_.end()) {
                continue;
            }
            for (std::string &prefix : iter->second) {
                prefixToWatch->emplace(prefix + "/" + key, startRevision);
            }
        }
    }

    // Create a watch stream
    LOG(INFO) << "All prefix need to watch: " << MapToString(*prefixToWatch);
    RETURN_IF_NOT_OK(InitWatch(std::move(prefixToWatch), std::bind(&EtcdStore::Writable, this)));

    auto traceId = Trace::Instance().GetTraceID();
    // Starts watch and waits for any errors from Etcd in background
    watchRunPool_->Execute([this, traceId] {
        // If any errors log
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
        (void)this->WatchRun();
    });
    // Start the event compensation thread.
    watchRunPool_->Execute([this, traceId]() mutable {
        // If any errors log
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
        watchEvents_->RetrieveEventPassively();
    });
    // Before returning to user block until threads are up
    // This ensure real watch is started before user can do other operations
    RETURN_IF_NOT_OK(watchEvents_->WaitForRunStartup());
    return Status::OK();
}

Status EtcdStore::ListTables(std::vector<std::string> &tables)
{
    std::shared_lock<std::shared_timed_mutex> lck(mutex_);
    std::transform(tableMap_.begin(), tableMap_.end(), std::back_inserter(tables),
                   [](const auto &item) { return item.first; });
    return Status::OK();
}

Status EtcdStore::RawGet(const std::string &etcdKey, RangeSearchResult &res, int64_t reqRevision, int32_t timeoutMs)
{
    etcdserverpb::RangeRequest req;
    req.set_key(etcdKey);
    // If revision is less or equal to zero, the range is over the newest key-value store.
    req.set_revision(reqRevision);
    etcdserverpb::RangeResponse rsp;
    VLOG(1) << "Calling rpc to get object with key " << etcdKey;
    RETURN_IF_NOT_OK(rpcSession_->SendRpc("Get::etcd_kv_Range", req, rsp, &etcdserverpb::KV::Stub::Range,
                                          GetAuthToken(), 0, timeoutMs));
    VLOG(1) << "Return from rpc after get object of key " << etcdKey << ". Value size: " << rsp.kvs_size();
    if (rsp.kvs_size() == 0) {
        RETURN_STATUS(K_NOT_FOUND, "The key does not exist in etcd. key:" + etcdKey);
    }

    if (rsp.kvs_size() > 1) {
        RETURN_STATUS(K_KVSTORE_ERROR, "The key value in etcd is not unique. key:" + etcdKey);
    }
    CHECK_FAIL_RETURN_STATUS(rsp.kvs(0).key() == etcdKey, K_KVSTORE_ERROR,
                             "The key returned by etcd is inconsistent with the requested key.");
    res.ParseKeyValue(rsp.kvs(0));
    return Status::OK();
}

Status EtcdStore::Get(const std::string &tableName, const std::string &key, RangeSearchResult &res, int32_t timeoutMs)
{
    std::shared_lock<std::shared_timed_mutex> lck(mutex_);
    TableMap::const_iterator iter = tableMap_.find(tableName);
    CHECK_FAIL_RETURN_STATUS(iter != tableMap_.end(), K_RUNTIME_ERROR,
                             "The table does not exist. tableName:" + tableName);
    std::string etcdKey = iter->second + "/" + key;
    return RawGet(etcdKey, res, 0, timeoutMs);
}

Status EtcdStore::Get(const std::string &tableName, const std::string &key, std::string &value)
{
    RangeSearchResult res;
    RETURN_IF_NOT_OK(Get(tableName, key, res));
    value = std::move(res.value);
    return Status::OK();
}

namespace {
std::string RemovePrefix(const std::string &input, const std::string &prefix)
{
    std::string newStr;
    auto pos = input.find(prefix);
    if (pos != std::string::npos) {
        newStr = input.substr(pos + prefix.size(), input.size());
    } else {
        newStr = input;
    }
    return newStr;
}
}  // namespace

Status EtcdStore::GetAll(const std::string &tableName, std::vector<std::pair<std::string, std::string>> &outKeyValues)
{
    // If revision is less or equal to zero, the range is over the newest key-value store.
    int64_t reqRevision = 0;
    return GetAll(tableName, reqRevision, outKeyValues);
}

Status EtcdStore::GetAll(const std::string &tableName, int64_t reqRevision,
                         std::vector<std::pair<std::string, std::string>> &outKeyValues)
{
    int64_t rspRevision;
    return GetAll(tableName, reqRevision, outKeyValues, rspRevision);
}

Status EtcdStore::GetAll(const std::string &tableName, std::vector<std::pair<std::string, std::string>> &outKeyValues,
                         int64_t &rspRevision)
{
    int64_t reqRevision = 0;
    return GetAll(tableName, reqRevision, outKeyValues, rspRevision);
}

Status EtcdStore::GetAll(const std::string &tableName, int64_t reqRevision,
                         std::vector<std::pair<std::string, std::string>> &outKeyValues, int64_t &rspRevision)
{
    std::shared_lock<std::shared_timed_mutex> lck(mutex_);
    TableMap::const_iterator iter = tableMap_.find(tableName);
    CHECK_FAIL_RETURN_STATUS(iter != tableMap_.cend(), K_RUNTIME_ERROR,
                             "The table does not exist. tableName:" + tableName);
    etcdserverpb::RangeRequest req;
    std::string etcdKey = iter->second + "/";
    req.set_key(etcdKey);
    req.set_range_end(StringPlusOne(etcdKey));
    req.set_revision(reqRevision);
    etcdserverpb::RangeResponse rsp;
    RETURN_IF_NOT_OK(
        rpcSession_->SendRpc("GetAll::etcd_kv_Range", req, rsp, &etcdserverpb::KV::Stub::Range, GetAuthToken()));
    for (auto &result : rsp.kvs()) {
        std::string key = RemovePrefix(result.key(), iter->second + "/");
        outKeyValues.emplace_back(std::make_pair(key, result.value()));
    }
    rspRevision = rsp.header().revision();
    return Status::OK();
}

Status EtcdStore::GetOtherAzAllHashRing(int64_t revision,
                                        std::vector<std::pair<std::string, std::string>> &outKeyValues)
{
    std::vector<std::string> allOtherAzHashRingTable;
    {
        std::shared_lock<std::shared_timed_mutex> lck(otherAzTblMutex_);
        auto iter = otherAzTableMap_.find(ETCD_RING_PREFIX);
        if (iter == otherAzTableMap_.end()) {
            return Status::OK();
        }
        allOtherAzHashRingTable = iter->second;
    }
    for (std::string &prefix : allOtherAzHashRingTable) {
        etcdserverpb::RangeRequest req;
        std::string etcdKey = prefix + "/";
        req.set_key(etcdKey);
        req.set_range_end(StringPlusOne(etcdKey));
        req.set_revision(revision);
        etcdserverpb::RangeResponse rsp;
        RETURN_IF_NOT_OK(
            rpcSession_->SendRpc("GetAll::etcd_kv_Range", req, rsp, &etcdserverpb::KV::Stub::Range, GetAuthToken()));
        for (auto &result : rsp.kvs()) {
            std::string key;
            size_t pos = result.key().find(ETCD_RING_PREFIX);
            if (pos == std::string::npos) {
                key = result.key();
            } else {
                auto subStr = result.key().substr(0, pos + 1);
                size_t firstSlashPos = subStr.find('/');
                size_t secondSlashPos = subStr.find('/', firstSlashPos + 1);
                key = secondSlashPos == std::string::npos
                          ? result.key()
                          : result.key().substr(firstSlashPos + 1, secondSlashPos - firstSlashPos - 1);
            }
            outKeyValues.emplace_back(std::make_pair(key, result.value()));
        }
    }
    return Status::OK();
}

Status EtcdStore::GetOtherAzAllValue(const std::string &tableName, int64_t revision,
                                     std::vector<std::pair<std::string, std::string>> &outKeyValues)
{
    std::shared_lock<std::shared_timed_mutex> lck(otherAzTblMutex_);
    auto iter = otherAzTableMap_.find(tableName);
    if (iter == otherAzTableMap_.end()) {
        return Status::OK();
    }
    for (std::string &prefix : iter->second) {
        etcdserverpb::RangeRequest req;
        std::string etcdKey = prefix + "/";
        req.set_key(etcdKey);
        req.set_range_end(StringPlusOne(etcdKey));
        req.set_revision(revision);
        etcdserverpb::RangeResponse rsp;
        RETURN_IF_NOT_OK(
            rpcSession_->SendRpc("GetAll::etcd_kv_Range", req, rsp, &etcdserverpb::KV::Stub::Range, GetAuthToken()));
        for (auto &result : rsp.kvs()) {
            std::string key = RemovePrefix(result.key(), prefix + "/");
            outKeyValues.emplace_back(std::make_pair(key, result.value()));
        }
    }
    return Status::OK();
}

Status EtcdStore::PrefixSearch(const std::string &tableName, const std::string &prefixKey,
                               std::vector<std::pair<std::string, std::string>> &outKeyValues)
{
    return RangeSearch(tableName, prefixKey, prefixKey, outKeyValues);
}

Status EtcdStore::PrefixSearch(const std::string &prefixKey, EtcdRangeGetVector &outKeyValues, int64_t &revision)
{
    etcdserverpb::RangeRequest req;
    req.set_key(prefixKey);
    req.set_range_end(StringPlusOne(prefixKey));
    etcdserverpb::RangeResponse rsp;
    auto rc =
        rpcSession_->SendRpc("PrefixSearch::etcd_kv_Range", req, rsp, &etcdserverpb::KV::Stub::Range, GetAuthToken());
    if (rc.IsOk()) {
        for (const auto &result : rsp.kvs()) {
            RangeSearchResult outResult;
            outResult.key = result.key();
            outResult.value = result.value();
            outResult.version = result.version();
            outResult.modRevision = result.mod_revision();
            outKeyValues.emplace_back(std::move(outResult));
        }
        revision = rsp.header().revision();
    } else {
        LOG(ERROR) << FormatString("Fail to get %s in etcd, with rc: %s", prefixKey, rc.ToString());
        return rc;
    }
    return Status::OK();
}

Status EtcdStore::RangeSearch(const std::string &tableName, const std::string &begin, const std::string &end,
                              std::vector<std::pair<std::string, std::string>> &outKeyValues)
{
    std::shared_lock<std::shared_timed_mutex> lck(mutex_);
    TableMap::const_iterator iter = tableMap_.find(tableName);
    CHECK_FAIL_RETURN_STATUS(iter != tableMap_.cend(), K_RUNTIME_ERROR,
                             "The table does not exist. tableName:" + tableName);
    etcdserverpb::RangeRequest req;
    std::string keyBegin = iter->second + "/" + begin;
    std::string keyEnd = iter->second + "/" + end;
    req.set_key(keyBegin);
    req.set_range_end(StringPlusOne(keyEnd));
    etcdserverpb::RangeResponse rsp;
    RETURN_IF_NOT_OK(
        rpcSession_->SendRpc("PrefixSearch::etcd_kv_Range", req, rsp, &etcdserverpb::KV::Stub::Range, GetAuthToken()));
    if (rsp.kvs_size() == 0) {
        RETURN_STATUS(K_NOT_FOUND, FormatString("The key does not exist in etcd. range[ %s, %s ]", keyBegin, keyEnd));
    }
    for (auto &result : rsp.kvs()) {
        std::string key = RemovePrefix(result.key(), iter->second + "/");
        outKeyValues.emplace_back(std::make_pair(key, result.value()));
    }
    return Status::OK();
}

Status EtcdStore::Delete(const std::string &tableName, const std::string &key)
{
    return Delete(tableName, key, 0, SEND_RPC_TIMEOUT_MS_DEFAULT);
}

Status EtcdStore::Delete(const std::string &tableName, const std::string &key, uint64_t asyncElapse, int timeoutMs)
{
    std::shared_lock<std::shared_timed_mutex> lck(mutex_);
    TableMap::const_iterator iter = tableMap_.find(tableName);
    CHECK_FAIL_RETURN_STATUS(iter != tableMap_.cend(), K_RUNTIME_ERROR,
                             "The table does not exist. tableName:" + tableName);
    etcdserverpb::DeleteRangeRequest req;
    std::string etcdKey = iter->second + "/" + key;
    req.set_key(etcdKey);
    etcdserverpb::DeleteRangeResponse rsp;
    VLOG(1) << "Calling rpc to remove object with key " << etcdKey;
    RETURN_IF_NOT_OK(rpcSession_->SendRpc("Delete::etcd_kv_DeleteRange", req, rsp, &etcdserverpb::KV::Stub::DeleteRange,
                                          GetAuthToken(), 0, timeoutMs, asyncElapse));
    VLOG(1) << "After calling rpc to remove object with key " << etcdKey << ", num: " << rsp.deleted();
    RETURN_OK_IF_TRUE(rsp.deleted() == 1);
    if (rsp.deleted() == 0) {
        RETURN_STATUS(StatusCode::K_NOT_FOUND, etcdKey + " not found in kvstore.");
    }
    RETURN_STATUS(StatusCode::K_KVSTORE_ERROR,
                  "The number of deleted is incorrect. deleted:" + std::to_string(rsp.deleted()));
}

namespace {
Status DoTransaction(const std::string &realKey, int64_t version, const std::string &value,
                     const std::string &authToken)
{
    Transaction txn(authToken);
    txn.StartTransaction();
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(txn.CompareKeyVersion(realKey, version), "CompareKeyVersion failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(txn.Put(realKey, value), "Transaction Put failed");
    return txn.Commit();
}
}  // namespace

Status EtcdStore::GetRealKey(const std::string &tableName, const std::string &key, std::string &realKey)
{
    std::shared_lock<std::shared_timed_mutex> lck(mutex_);
    TableMap::const_iterator iter = tableMap_.find(tableName);
    CHECK_FAIL_RETURN_STATUS(iter != tableMap_.cend(), K_RUNTIME_ERROR,
                             "The table does not exist. tableName:" + tableName);
    realKey = iter->second + "/" + key;
    return Status::OK();
}

Status EtcdStore::CAS(const std::string &tableName, const std::string &key, const EtcdProcessFunction &processFunc)
{
    RangeSearchResult res;
    return CAS(tableName, key, processFunc, res);
}

Status EtcdStore::CAS(const std::string &tableName, const std::string &key, const EtcdProcessFunction &processFunc,
                      RangeSearchResult &res)
{
    std::string realKey;
    RETURN_IF_NOT_OK(GetRealKey(tableName, key, realKey));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(processFunc != nullptr, K_INVALID, "Etcd process function resolve error.");
    uint32_t retryNum = 0;
    uint32_t errorRetryNum = 0;
    Status lastErr;
    // When the processFunc function fails to be executed or etcd error, a maximum of CAS_ERROR_MAX_RETRY_NUM attempts
    // are performed; When etcd transaction fails to be executed, will keep retrying until successful;
    while (errorRetryNum < CAS_ERROR_MAX_RETRY_NUM) {
        ++retryNum;
        // Random sleep 0-10ms, avoid CAS conflicts.
        std::this_thread::sleep_for(std::chrono::microseconds(randomData_.GetRandomUint32(0, CAS_MAX_SLEEP_TIME_US)));
        // 1. If the key does not exist in etcd, means the first insert the key-value to etcd, it should continue
        // processing.
        Status status = Get(tableName, key, res, CAS_GET_TIMEOUT_MS);
        bool firstInit = status.GetCode() == K_NOT_FOUND;
        if (!firstInit && status.IsError()) {  // Rectify the fault that the key does not exist.
            LOG(WARNING) << FormatString("Get key[%s] from table[%s] failed, with status: %s, give up retry.", key,
                                         tableName, status.ToString());
            lastErr = status;
            break;
        }
        // 2. Generate a new value based on the current value
        std::unique_ptr<std::string> newValue;
        bool retry{ true };
        status = processFunc(res.value, newValue, retry);
        if (status.IsError()) {
            if (!retry) {
                return status;
            }
            errorRetryNum++;
            lastErr = status;
            continue;
        }
        // 3. Return if the data not need to be written to etcd
        if (newValue == nullptr) {
            VLOG(1) << "CAS no value need to write ";
            return Status::OK();
        }
        // 4. Execute a transaction. If the key-value is not changed, the transaction is successfully written.
        if (status = DoTransaction(realKey, res.version, *newValue, GetAuthToken()), status.IsError()) {
            lastErr = status;
            errorRetryNum++;
            continue;
        }

        // the key version after put is the next version of compareVersion, it's guaranteed by the etcd transaction
        auto successVersion = res.version + 1;
        LOG(INFO) << FormatString("CAS is successfully executed. The retryNum:%u, firstInit:%d, new version:%d",
                                  retryNum, firstInit, successVersion);
        return Status::OK();
    }
    return lastErr;
}

Status EtcdStore::CAS(const std::string &tableName, const std::string &key, const std::string &oldValue,
                      const std::string &newValue)
{
    std::shared_lock<std::shared_timed_mutex> lck(mutex_);
    TableMap::const_iterator iter = tableMap_.find(tableName);
    CHECK_FAIL_RETURN_STATUS(iter != tableMap_.cend(), K_RUNTIME_ERROR,
                             "The table does not exist. tableName:" + tableName);
    std::string realKey = iter->second + "/" + key;
    RangeSearchResult res;
    Status status = Get(tableName, key, res);
    Transaction txn(GetAuthToken());
    txn.StartTransaction();
    if (status.IsError() && status.GetCode() == K_NOT_FOUND) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(txn.CompareKeyVersion(realKey, 0), "CompareKeyVersion failed");
    } else {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(txn.CompareKeyValue(realKey, oldValue), "CompareKeyVersion failed");
    }
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(txn.Put(realKey, newValue), "Transaction Put failed");
    return txn.Commit();
}

Status EtcdStore::GetEtcdPrefix(const std::string &tableName, std::string &prefix)
{
    TableMap::const_iterator iter = tableMap_.find(tableName);
    CHECK_FAIL_RETURN_STATUS(iter != tableMap_.cend(), K_RUNTIME_ERROR,
                             "The table does not exist. tableName:" + tableName);
    prefix = iter->second;
    return Status::OK();
}

Status EtcdStore::InformEtcdReconciliationDone(const HostPort &workerAddr)
{
    CHECK_FAIL_RETURN_STATUS(!IsKeepAliveTimeout(), K_NOT_READY,
                             "The key written to the cluster table must be bound to a lease");
    std::string valueStr;
    RETURN_IF_NOT_OK(Get(ETCD_CLUSTER_TABLE, workerAddr.ToString(), valueStr));
    KeepAliveValue value;
    RETURN_IF_NOT_OK(KeepAliveValue::FromString(valueStr, value));
    INJECT_POINT("recover.toReady.delay");
    if (value.state == "restart" || value.state == "recover") {
        value.state = ETCD_NODE_READY;
        RETURN_IF_NOT_OK(PutWithLeaseId(ETCD_CLUSTER_TABLE, workerAddr.ToString(), value.ToString(), CheckLeaseId()));
    }
    return Status::OK();
}

std::unique_ptr<GrpcSession<etcdserverpb::KV>> Transaction::rpcSession_ = nullptr;
std::once_flag Transaction::flag_;
std::atomic<int> Transaction::num_{ 0 };

Transaction::Transaction(std::string authToken) : authToken_(std::move(authToken))
{
    (void)num_.fetch_add(1);
    std::call_once(flag_, []() {
        Status rc = GrpcSession<etcdserverpb::KV>::CreateSession(FLAGS_etcd_address, rpcSession_);
        LOG_IF_ERROR(rc, "Transaction CreateSession failed!");
    });
}

Transaction::~Transaction()
{
    (void)num_.fetch_sub(1);
    if (num_ == 0) {
        rpcSession_->Shutdown();
    }
}

void Transaction::StartTransaction()
{
    if (txnReq_ != nullptr) {
        LOG(WARNING) << "Start Transaction twice!";
    }
    txnReq_ = std::make_unique<etcdserverpb::TxnRequest>();
}

Status Transaction::Commit()
{
    RETURN_RUNTIME_ERROR_IF_NULL(txnReq_);
    RETURN_OK_IF_TRUE(txnReq_->success_size() == 0);
    AutoDereference autoDef(this);
    INJECT_POINT("etcd.txn.commit");
    etcdserverpb::TxnResponse rsp;
    RETURN_IF_NOT_OK(rpcSession_->AsyncSendRpc("Txn", *txnReq_, rsp, &etcdserverpb::KV::Stub::AsyncTxn, authToken_));
    if (!rsp.succeeded()) {
        RETURN_STATUS(StatusCode::K_TRY_AGAIN, "Transaction comparison failed, maybe need to update req and try again");
    }
    return Status::OK();
}

void Transaction::Clean()
{
    txnReq_.reset();
}

Status Transaction::CompareKeyValue(const std::string &key, const std::string &oldValue)
{
    RETURN_RUNTIME_ERROR_IF_NULL(txnReq_);
    auto compare = txnReq_->add_compare();
    compare->set_result(etcdserverpb::Compare_CompareResult_EQUAL);
    compare->set_target(etcdserverpb::Compare_CompareTarget_VALUE);
    compare->set_key(key);
    compare->set_value(oldValue);
    return Status::OK();
}

Status Transaction::CompareKeyVersion(const std::string &key, int64_t version)
{
    RETURN_RUNTIME_ERROR_IF_NULL(txnReq_);
    auto compare = txnReq_->add_compare();
    compare->set_result(etcdserverpb::Compare_CompareResult_EQUAL);
    compare->set_target(etcdserverpb::Compare_CompareTarget_VERSION);
    compare->set_key(key);
    compare->set_version(version);
    return Status::OK();
}

Status Transaction::Put(const std::string &key, const std::string &value)
{
    RETURN_RUNTIME_ERROR_IF_NULL(txnReq_);
    auto req = std::make_unique<etcdserverpb::PutRequest>();
    req->set_key(key);
    req->set_value(value);
    auto *newReq = txnReq_->add_success();
    newReq->set_allocated_request_put(req.release());
    return Status::OK();
}

Status Transaction::Delete(const std::string &key)
{
    RETURN_RUNTIME_ERROR_IF_NULL(txnReq_);
    auto req = std::make_unique<etcdserverpb::DeleteRangeRequest>();
    req->set_key(key);
    auto *newReq = txnReq_->add_success();
    newReq->set_allocated_request_delete_range(req.release());
    return Status::OK();
}
}  // namespace datasystem
