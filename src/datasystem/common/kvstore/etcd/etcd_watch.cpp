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
 * Description: Etcd watch implementation.
 */
#include "datasystem/common/eventloop/timer_queue.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/kvstore/etcd/etcd_watch.h"
#include "datasystem/common/util/status_helper.h"

constexpr uint32_t WATCH_TASK_THREAD_NUM = 2;
using std::chrono::milliseconds;

namespace datasystem {

std::string GetEventMsg(const mvccpb::Event &event)
{
    static auto toString = [](mvccpb::Event::EventType type) -> std::string {
        std::string name;
        switch (type) {
            case mvccpb::Event::EventType::Event_EventType_PUT:
                name = "PUT";
                break;
            case mvccpb::Event::EventType::Event_EventType_DELETE:
                name = "DELETE";
                break;
            default:
                name = "OTHER";
                break;
        }
        return name;
    };
    std::stringstream s;
    s << "key: " << event.kv().key() << ", version: " << event.kv().version()
      << ", mod_revision: " << event.kv().mod_revision() << ", event type: " << toString(event.type());
    return s.str();
}

EtcdWatch::EtcdWatch(std::string address, std::unique_ptr<std::unordered_map<std::string, int64_t>> &&prefixMap)
    : watchPool_(WATCH_TASK_THREAD_NUM),
      shuttingDown_(false),
      address_(std::move(address)),
      prefixMap_(std::move(prefixMap)),
      startupSyncPoint_(false)
{
    keyVersion_ = std::make_unique<std::unordered_map<std::string, VersionInfo>>();
}

EtcdWatch::EtcdWatch(std::string address, std::unique_ptr<std::unordered_map<std::string, int64_t>> &&prefixMap,
                     RouterClientCurveKit clientKit)
    : watchPool_(WATCH_TASK_THREAD_NUM),
      shuttingDown_(false),
      address_(std::move(address)),
      prefixMap_(std::move(prefixMap)),
      startupSyncPoint_(false),
      clientCurveKit_(std::move(clientKit)),
      isRouterClientCurveConnect_(true)
{
    keyVersion_ = std::make_unique<std::unordered_map<std::string, VersionInfo>>();
}

template <typename T>
void FillEtcdHeader(const T &pb, EtcdWatchResponse &rsp)
{
    rsp.header.clusterId = pb.header().cluster_id();
    rsp.header.memberId = pb.header().member_id();
    rsp.header.revision = pb.header().revision();
    rsp.header.raftTerm = pb.header().raft_term();
}

EtcdWatch::~EtcdWatch()
{
    LOG_IF_ERROR(Shutdown(), "Destructor shutting down EtcdWatch, but an error was given.");
}

Status EtcdWatch::Shutdown()
{
    {
        WriteLock lock(&shutdownLock_);
        // no-op and quit if shutting down was already set to true
        RETURN_OK_IF_TRUE(shuttingDown_.exchange(true));
    }

    retrieveEventWaitPost_.Set();

    VLOG(1) << "EtcdWatch is being shutdown.";

    // Cancel the client context
    // This will unblock any Next() call
    (void)context_->TryCancel();

    (void)etcdEventQue_.Abort();

    // Exit consumer thread. The Run() parent thread of the consumer thread does not wait() or get() its std::future.
    // It is safe for this shutdown thread to check it for valid() and wait for it.
    if (consumerStatus_.valid()) {
        (void)consumerStatus_.wait();
    }

    // Exit producer thread. The Run() parent thread of the producer does a wait() followed by get() on its std::future.
    // Shutdown waits for the cleanup and shutdown of the producer. It determines this by waiting for the future to
    // become invalid when the Run() thread does std::future::get()
    {
        std::unique_lock<std::mutex> lck(producerMtx_);
        producerCond_.wait(lck, [this]() { return !producerStatus_.valid(); });
    }

    VLOG(1) << "EtcdWatch shutdown completed.";
    return Status::OK();
}

void EtcdWatch::SetWatchEventHandler(std::function<void(mvccpb::Event &&event)> eventHandler)
{
    eventHandler_ = std::move(eventHandler);
}

void EtcdWatch::SetCheckEtcdStateHandler(std::function<Status()> checkEtcdStateHandler)
{
    checkEtcdStateHandler_ = std::move(checkEtcdStateHandler);
}

Status EtcdWatch::Init(const std::string &authToken)
{
    cq_ = std::make_unique<grpc::CompletionQueue>();
    context_ = std::make_unique<grpc::ClientContext>();
    if (!isRouterClientCurveConnect_) {
        RETURN_IF_NOT_OK(GrpcSession<etcdserverpb::Watch>::CreateSession(address_, watchSession_));
    } else {
        RETURN_IF_NOT_OK(GrpcSession<etcdserverpb::Watch>::CreateSession(
            address_, watchSession_, clientCurveKit_.etcdCa, clientCurveKit_.etcdCert, clientCurveKit_.etcdKey,
            clientCurveKit_.etcdNameOverride));
    }
    CHECK_FAIL_RETURN_STATUS(watchSession_ != nullptr, K_KVSTORE_ERROR, "GRPC watch session could not be created");
    if (!authToken.empty()) {
        context_->AddMetadata("token", authToken);
    }
    stream_ = watchSession_->Stub()->AsyncWatch(context_.get(), cq_.get(), (void *)WATCH_CREATE);
    CHECK_FAIL_RETURN_STATUS(stream_ != nullptr, K_KVSTORE_ERROR,
                             "GRPC stream connection for watch could not be created");
    RETURN_IF_NOT_OK_APPEND_MSG(ProcessWatchResponse((void *)WATCH_CREATE), "Init stream grpc failed!");
    return Status::OK();
}

Status EtcdWatch::WaitForRunStartup()
{
    const int waitForStartTimeoutSecond = 10;
    std::unique_lock<std::mutex> lk(waitMtx_);
    if (!waitCond_.wait_for(lk, std::chrono::seconds(waitForStartTimeoutSecond),
                            [this]() { return startupSyncPoint_; })) {
        RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, "EtcdKeepAlive::Run() thread failed to reach startup sync point.");
    }
    return Status::OK();
}

Status EtcdWatch::Run()
{
    Status rc;
    {
        // Submitting a task can only be done while synchronized behind the shutdown lock
        ReadLock lock(&shutdownLock_);
        if (shuttingDown_) {
            // Do not launch any task if we are shutting down
            return Status::OK();
        }
        // This thread polls Etcd stream for any events
        // And returns error if anything is wrong with Etcd
        producerStatus_ = watchPool_.Submit([this] {
            TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(WatchEvents(), "Watch Event thread failed");
            return Status::OK();
        });

        // Consumer thread to run user defined functions upon events
        if (!consumerStatus_.valid()) {
            consumerStatus_ = watchPool_.Submit([this] {
                TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
                return EventHandler();
            });
        }
    }

    // Wait for any potential errors from Etcd
    producerStatus_.wait();

    // Calling producerStatus_.get() changes the std::future back to invalid state.
    // A shutdown thread might be waiting for this event so it needs to be protected with lock and notified
    {
        std::unique_lock<std::mutex> lck(producerMtx_);
        rc = producerStatus_.get();
    }
    producerCond_.notify_one();
    return rc;
}

void ParseWatchRsp(etcdserverpb::WatchResponse &rsp, EtcdWatchResponse &response)
{
    FillEtcdHeader(rsp, response);
    response.watchId = rsp.watch_id();
    response.created = rsp.created();
    response.canceled = rsp.canceled();
    response.compactRevision = rsp.compact_revision();
    response.events.clear();
    for (int i = 0; i < rsp.events_size(); i++) {
        response.events.push_back(rsp.events(i));
    }
}

Status EtcdWatch::EventHandler()
{
    const int logEveryTSec = 60;
    while (!shuttingDown_) {
        LOG_EVERY_T(INFO, logEveryTSec) << "waitting event count: " << etcdEventQue_.Size();
        mvccpb::Event event;
        // Block until something is available in the queue
        (void)etcdEventQue_.Pop(event);
        if (shuttingDown_) {
            return Status::OK();
        }
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(eventHandler_ != nullptr, K_INVALID,
                                             "Watch callback function initializes failed.");
        INJECT_POINT("EventHandler.wait");
        int64_t curModRevision = 0;
        auto key = event.kv().key();
        auto modRevision = event.kv().mod_revision();
        auto version = event.kv().version();
        if (CheckIfEventExpired(key, modRevision, curModRevision)) {
            LOG(INFO) << "receive an old event: " << GetEventMsg(event) << ", current mod_revision: " << curModRevision
                      << ", ignore it.";
            continue;
        }
        auto eventType = event.type();
        if (eventType == mvccpb::Event::EventType::Event_EventType_DELETE
            && event.kv().key().find(ETCD_CLUSTER_TABLE) != std::string::npos) {
            auto rc = checkEtcdStateHandler_();
            if (rc.IsError()) {
                LOG(INFO) << FormatString("Etcd is not writable[rc: %s], so ignore event: %s", rc.ToString(),
                                          GetEventMsg(event));
                continue;
            }
        }
        LOG(INFO) << "Enqueue event " << GetEventMsg(event);
        updateClusterInfoInRocksDbHandler_(event);
        eventHandler_(std::move(event));
        LOG_IF_ERROR(UpdateKeyVersion(key, modRevision, version, eventType), "update failed.");
    }
    return Status::OK();
}

inline void EtcdWatch::StoreEvents(EtcdWatchResponse &response)
{
    INJECT_POINT("EtcdWatch.StoreEvents.IgnoreEvent", []() { return; });
    LOG(INFO) << "Received an ETCD watch response, Watch ID: " << response.watchId
              << " Compact Version: " << response.compactRevision << " revision no.: " << response.header.revision;
    if (!response.created && !response.canceled) {
        // Push new events for consumer thread to read
        for (auto &event : response.events) {
            etcdEventQue_.Push(std::move(event));
        }
    }
}

Status EtcdWatch::ProcessWatchResponse(const void *tag)
{
    bool ok = false;
    void *ret_tag = nullptr;
    bool nextSuccess = cq_->Next(&ret_tag, &ok);

    if (ok) {
        if (nextSuccess) {
            if (tag != ret_tag) {
                return Status(K_RUNTIME_ERROR, "EtcdWatch got event but the tag was unexpected");
            }
            return Status::OK();
        }

        return Status(K_SHUTTING_DOWN, "Watcher Shutdown is called by server");
    }
    return Status(K_RPC_UNAVAILABLE, "Failed to get next event from completion queue");
}

void EtcdWatch::ShutdownEtcd()
{
    // Prevents Shutting down twice
    if (!stream_) {
        return;
    }

    // WritesDone and Finish is called to inform the Etcd that the node is stopping.
    // When we cancel the call using Try_cancel, these calls will return a K_RPC_UNAVAILABLE
    // We still have these calls as part of proper shutdown
    (void)stream_->WritesDone((void *)WRITES_DONE);
    Status rc = ProcessWatchResponse((void *)WRITES_DONE);
    // K_RPC_UNAVAILABLE is expected error during shutdown
    if (rc.IsError() && rc.GetCode() != K_RPC_UNAVAILABLE) {
        LOG(ERROR) << "WritesDone was unsuccessful with error : " << rc.GetMsg();
    }

    grpc::Status status;
    (void)stream_->Finish(&status, (void *)this);
    rc = ProcessWatchResponse((void *)this);
    LOG_IF_ERROR(rc, "Finish was unsuccessful");
    if (rc.IsOk()) {
        // CANCELLED is expected error during shutdown
        if (!status.ok() && status.error_code() != grpc::StatusCode::CANCELLED) {
            LOG(ERROR) << "Finish was unsuccessful with Error: " << status.error_message();
        } else if (status.error_code() == grpc::StatusCode::CANCELLED) {
            LOG(INFO) << "Finish completed with result: cancelled";
        }
    }

    // Exit the completion queue
    (void)cq_->Shutdown();

    // Draining out the completion queue. This operation is necessary to remove outstanding objects from cq
    // to prevent memory leakage.
    bool ok = false;
    void *tag = nullptr;
    while (cq_->Next(&tag, &ok)) {
        ;
    }

    if (watchSession_) {
        watchSession_->Shutdown();
    }
    stream_.reset();
    watchSession_.reset();

    LOG(INFO) << "Etcd watch thread shutting down.";
}

Status EtcdWatch::CreateWatch()
{
    std::shared_lock<std::shared_timed_mutex> lck(prefixMapMutex_);
    for (const auto &it : *prefixMap_) {
        etcdserverpb::WatchRequest watchReq;
        etcdserverpb::WatchCreateRequest createReq;
        createReq.set_key(it.first);
        createReq.set_range_end(it.first + "\xFF");
        createReq.set_start_revision(it.second + 1);
        watchReq.mutable_create_request()->CopyFrom(createReq);
        stream_->Write(watchReq, (void *)WATCH_WRITE);
        Status rc = ProcessWatchResponse((void *)WATCH_WRITE);
        if (rc.IsError()) {
            LOG(ERROR) << "Watcher failed internally in write with status: " << rc.GetMsg();
            return rc;
        }
    }

    return Status::OK();
}

Status EtcdWatch::ReadWatchStream()
{
    etcdserverpb::WatchResponse rspPb;
    EtcdWatchResponse response;

    // Send an async read request
    stream_->Read(&rspPb, (void *)WATCH_READ);

    // Wait for Read request to end
    Status rc = ProcessWatchResponse((void *)WATCH_READ);
    if (rc.IsOk()) {
        // Normal case where we got an event.
        // Process the response and then do another read for next event
        ParseWatchRsp(rspPb, response);
        StoreEvents(response);
    }

    return rc;
}

Status EtcdWatch::WatchEvents()
{
    if (shuttingDown_) {
        return Status::OK();
    }

    {
        // We have reached the critical point that indicates the watch is now running.
        // The startup logic has been waiting for this moment and will resume work once we notify here.
        std::lock_guard<std::mutex> lk(waitMtx_);
        startupSyncPoint_ = true;
    }
    waitCond_.notify_all();

    RETURN_IF_NOT_OK(CreateWatch());

    do {
        Status rc = ReadWatchStream();
        if (rc.IsError()) {
            if (shuttingDown_) {
                LOG(INFO) << "EtcdWatch was interrupted and shutting down.";
                rc = Status::OK();
            } else {
                LOG(ERROR) << "EtcdWatch failed, with rc: " << rc.ToString();
            }
            ShutdownEtcd();
            return rc;
        }
        std::this_thread::sleep_for(milliseconds(100));
    } while (!shuttingDown_);

    // Shutdown the stream and cq
    ShutdownEtcd();
    return Status::OK();
}

mvccpb::Event EtcdWatch::GenerateFakeEvent(const std::string &key, const std::string &val, int64_t modRevision,
                                           mvccpb::Event_EventType eventType, int64_t version)
{
    mvccpb::Event fakeEvent;
    auto fakeKV = fakeEvent.mutable_kv();
    fakeKV->set_key(key);
    fakeKV->set_value(val);
    fakeKV->set_version(version);
    fakeKV->set_mod_revision(modRevision);
    fakeEvent.set_type(eventType);
    return fakeEvent;
}

void EtcdWatch::CloseProducer()
{
    // Cancel the client context
    // This will unblock any Next() call
    (void)context_->TryCancel();

    // Exit producer thread. The Run() parent thread of the producer does a wait() followed by get() on its
    // std::future. Shutdown waits for the cleanup and shutdown of the producer. It determines this by waiting for
    // the future to become invalid when the Run() thread does std::future::get()
    {
        std::unique_lock<std::mutex> lck(producerMtx_);
        producerCond_.wait(lck, [this]() { return !producerStatus_.valid(); });
    }
}

void EtcdWatch::RetrieveEventPassively()
{
    INJECT_POINT("EtcdWatch.RetrieveEventPassively.AvoidEventCompensation", []() { return; });
    LOG(INFO) << "Start check and generate fake event.";
    Status rc;
    while (!shuttingDown_) {
        auto waitingTimeForThisCycleMs = rc.IsOk() ? EVENT_COMPENSATION_INTERVAL_UNDER_NORMAL_CONDITIONS_MS
                                                   : EVENT_COMPENSATION_INTERVAL_UNDER_ABNORMAL_CONDITIONS_MS;
        INJECT_POINT("EtcdWatch.RetrieveEventPassively.RetrieveEventQuickly",
                     [&](uint32_t timeMs) { waitingTimeForThisCycleMs = timeMs; });
        retrieveEventWaitPost_.WaitFor(waitingTimeForThisCycleMs);
        if (shuttingDown_) {
            return;
        }
        auto rc = RetrieveEventPassivelyImpl();
        LOG_IF_ERROR(rc, "Executing event compensation thread failed");
    }
}

Status EtcdWatch::RetrieveEventPassivelyImpl()
{
    INJECT_POINT("EtcdWatch.RetrieveEventPassivelyImpl.preRetrieveEvent");
    RETURN_IF_NOT_OK(checkEtcdStateHandler_());

    std::shared_lock<std::shared_timed_mutex> lck(prefixMapMutex_);
    return GenerateFakeEventIfNeeded();
}

Status EtcdWatch::RetrieveEventActively()
{
    std::lock_guard<std::shared_timed_mutex> lck(prefixMapMutex_);
    return GenerateFakeEventIfNeeded(true);
}

Status EtcdWatch::UpdateKeyVersion(const std::string &key, int64_t modRevision, int64_t version,
                                   mvccpb::Event::EventType eventType)
{
    bool isDelete;
    if (eventType == mvccpb::Event::EventType::Event_EventType_PUT) {
        isDelete = false;
    } else if (eventType == mvccpb::Event::EventType::Event_EventType_DELETE) {
        isDelete = true;
    } else {
        RETURN_STATUS(K_RUNTIME_ERROR, "UNKNOWN TYPE: " + std::to_string(eventType));
    }

    std::lock_guard<std::shared_timed_mutex> lck(keyVersionMutex_);
    auto it = keyVersion_->find(key);
    if (it == keyVersion_->end()) {
        VersionInfo info;
        info.modRevision = modRevision;
        info.version = version;
        info.isDelete = isDelete;
        keyVersion_->insert({ key, std::move(info) });
        return Status::OK();
    }
    if (modRevision > it->second.modRevision) {
        it->second.version = version;
        it->second.modRevision = modRevision;
        it->second.isDelete = isDelete;
        return Status::OK();
    };
    RETURN_STATUS(
        K_RUNTIME_ERROR,
        FormatString(
            "An old event[key: %s, mod_revision: %lld,] has been processed, incredible. Local mod_revision: %lld", key,
            modRevision, it->second.modRevision));
}

Status EtcdWatch::GenerateFakePutEventIfNeeded(bool watchedFailed,
                                               std::unordered_map<std::string, VersionInfo> &copyKeyVersion,
                                               std::unordered_map<std::string, int64_t> &prefix2Revision)
{
    auto cmp = [](const mvccpb::Event &left, const mvccpb::Event &right) {
        return left.kv().mod_revision() > right.kv().mod_revision();
    };
    std::set<mvccpb::Event, decltype(cmp)> events(cmp);
    for (auto &it : *prefixMap_) {
        int64_t revision;
        EtcdRangeGetVector outKeyValues;
        RETURN_IF_NOT_OK(prefixSearchHandler_(it.first, outKeyValues, revision));
        if (watchedFailed) {
            it.second = revision;
        }
        prefix2Revision.emplace(it.first, revision);

        for (const auto &outKeyValue : outKeyValues) {
            auto iter = copyKeyVersion.find(outKeyValue.key);
            if (iter != copyKeyVersion.end() && iter->second.modRevision >= outKeyValue.modRevision) {
                (void)copyKeyVersion.erase(outKeyValue.key);
                continue;
            }
            auto localVersion = iter == copyKeyVersion.end() ? 0 : iter->second.version;
            if (localVersion + 1 != outKeyValue.version && !watchedFailed) {
                LOG(INFO) << FormatString(
                    "[Processed: %s; Retrieved: %s], wait for %llu ms insert it to etcdEventQue_.",
                    iter == copyKeyVersion.end() ? "null" : iter->second.ToString(), outKeyValue.ToString(),
                    delayGenerateFakePutEventTimeMs_);
                auto traceID = Trace::Instance().GetTraceID();
                TimerQueue::TimerImpl timer;
                auto weakThis = weak_from_this();
                auto func = [weakThis, outKeyValue, traceID]() {
                    auto watch = weakThis.lock();
                    if (watch == nullptr) {
                        return;
                    }
                    TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
                    watch->DelayGenerateFakePutEvent(outKeyValue);
                };
                INJECT_POINT("worker.GenerateFakePutEventIfNeeded.timeout", [this](int32_t newTimeoutMs) {
                    delayGenerateFakePutEventTimeMs_ = newTimeoutMs;
                    return Status::OK();
                });
                LOG_IF_ERROR(TimerQueue::GetInstance()->AddTimer(delayGenerateFakePutEventTimeMs_, func, timer),
                             "DelayGenerateFakePutEvent failed");
            } else {
                auto localModRevision = iter == copyKeyVersion.end() ? 0 : iter->second.modRevision;
                LOG(INFO) << FormatString(
                    "key[%s] has expired[local mode_revision: %lld], generate a fake event[PUT] to update it to %lld",
                    outKeyValue.key, localModRevision, outKeyValue.modRevision);
                events.emplace(GenerateFakeEvent(outKeyValue.key, outKeyValue.value, outKeyValue.modRevision,
                                                 mvccpb::Event_EventType::Event_EventType_PUT, outKeyValue.version));
            }
            (void)copyKeyVersion.erase(outKeyValue.key);
        }
    }

    if (!events.empty()) {
        etcdEventQue_.Push(std::move(events));
    }
    return Status::OK();
}

void EtcdWatch::DelayGenerateFakePutEvent(const RangeSearchResult &outKeyValue)
{
    int64_t localModRevision = 0;
    {
        std::shared_lock<std::shared_timed_mutex> lock(keyVersionMutex_);
        auto iter = keyVersion_->find(outKeyValue.key);
        localModRevision = iter == keyVersion_->end() ? 0 : iter->second.modRevision;
    }
    if (outKeyValue.modRevision > localModRevision) {
        LOG(INFO) << FormatString(
            "key[%s] has expired[local mode_revision: %lld], generate a fake event[PUT] to update it to %lld",
            outKeyValue.key, localModRevision, outKeyValue.modRevision);
        etcdEventQue_.Push(GenerateFakeEvent(outKeyValue.key, outKeyValue.value, outKeyValue.modRevision,
                                             mvccpb::Event_EventType::Event_EventType_PUT, outKeyValue.version));
    } else {
        LOG(INFO) << FormatString("local key %s mod_revision is %lld, no need to insert old mod_revision %lld event.",
                                  outKeyValue.key, localModRevision, outKeyValue.modRevision);
    }
}

void EtcdWatch::GenerateFakeDeleteEventIfNeeded(const std::unordered_map<std::string, VersionInfo> &copyKeyVersion,
                                                const std::unordered_map<std::string, int64_t> &prefix2Revision)
{
    // The remaining keys are the keys that were not obtained by this batch of get operations, and delete events are
    // generated based on them.
    for (const auto &pair : copyKeyVersion) {
        if (pair.second.isDelete) {
            continue;
        }
        int64_t curRevision = 0;
        for (const auto &pair2 : prefix2Revision) {
            if (pair.first.find(pair2.first) != std::string::npos) {
                curRevision = pair2.second;
                break;
            }
        }
        LOG(INFO) << FormatString("key[%s] has expired, generate a fake event[DELETE] to update it to %lld", pair.first,
                                  curRevision);
        etcdEventQue_.Push(
            GenerateFakeEvent(pair.first, "", curRevision, mvccpb::Event_EventType::Event_EventType_DELETE, 0));
    }
}

Status EtcdWatch::GenerateFakeEventIfNeeded(bool watchedFailed)
{
    std::unordered_map<std::string, int64_t> prefix2Revision;
    std::unordered_map<std::string, VersionInfo> copyKeyVersion;
    {
        std::shared_lock<std::shared_timed_mutex> lock(keyVersionMutex_);
        copyKeyVersion = *(keyVersion_.get());
    }

    RETURN_IF_NOT_OK(GenerateFakePutEventIfNeeded(watchedFailed, copyKeyVersion, prefix2Revision));
    GenerateFakeDeleteEventIfNeeded(copyKeyVersion, prefix2Revision);
    return Status::OK();
}
}  // namespace datasystem
