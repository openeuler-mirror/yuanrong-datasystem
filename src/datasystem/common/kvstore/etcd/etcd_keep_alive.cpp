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
 * Description: Etcd elector implementation.
 */
#include "datasystem/common/kvstore/etcd/etcd_keep_alive.h"

#include <cstdint>
#include <thread>

#include "datasystem/common/log/log.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/format.h"

DS_DEFINE_bool(enable_etcd_auth, false, "Enable etcd certification auth");
DS_DEFINE_string(etcd_target_name_override, "",
                 "The etcd target name override for SSL host name checking"
                 "The configuration value should be consistent with the DNS content of the Subject Alternate Names of "
                 "the TLS certificate.");
DS_DECLARE_uint32(node_timeout_s);
DS_DEFINE_int32(heartbeat_interval_ms, 1000,
                "The time interval of each heartbeat between worker and master. Timeout must larger than 0.");
DS_DEFINE_validator(heartbeat_interval_ms, &Validator::ValidateInt32);
DS_DEFINE_string(etcd_ca, "", "The path of encrypted root etcd certificate, default is none.");
DS_DEFINE_string(etcd_cert, "", "The path of encrypted client's etcd certificate chain, default is none.");
DS_DEFINE_string(etcd_key, "", "The path of encrypted client's etcd private key, default is none.");
DS_DEFINE_string(etcd_passphrase_path, "", "The path of client's private key passphrase, default is none.");

using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::steady_clock;

namespace datasystem {
template <typename T>
void FillEtcdHeader(const T &pb, EtcdResponse &rsp)
{
    rsp.header.clusterId = pb.header().cluster_id();
    rsp.header.memberId = pb.header().member_id();
    rsp.header.revision = pb.header().revision();
    rsp.header.raftTerm = pb.header().raft_term();
}

EtcdKeepAlive::EtcdKeepAlive(const std::string &address, int64_t leaseId)
    : asyncTask_(1), shuttingDown_(false), address_(address), leaseId_(leaseId)
{
}

EtcdKeepAlive::~EtcdKeepAlive()
{
    LOG_IF_ERROR(Shutdown(), "Destructor shutting down EtcdKeepAlive, but an error was given.");
}

Status EtcdKeepAlive::Shutdown()
{
    {
        WriteLock lock(&shutdownLock_);
        // no-op and quit if shutting down was already set to true
        RETURN_OK_IF_TRUE(shuttingDown_.exchange(true));
    }

    if (!stream_) {
        return Status::OK();
    }

    LOG(INFO) << "Etcd Lease KeepAlive is being shut down.";

    // Unblock the pending event
    context_.TryCancel();

    // Wait for the asyncRes future to become invalid (if it is not already)
    // There is no need to std::future::wait() because the Run() thread does that and then calls get() to move the
    // future back to invalid state again. Here, we just need to wait for the invalid state to indicate the thread is
    // done
    {
        std::unique_lock<std::mutex> lck(asyncResMtx_);
        asyncResCond_.wait(lck, [this]() { return !asyncRes_.valid(); });
    }

    void *tag = nullptr;
    bool ok = false;

    // Cancel grpc stream channel.
    // If call is cancelled using try_cancel, ok will be false
    stream_->WritesDone((void *)KEEPALIVE_DONE);
    if (!cq_.Next(&tag, &ok) || tag != (void *)KEEPALIVE_DONE) {
        LOG(ERROR) << "Keep Alive WritesDone was unsuccessful";
    }

    grpc::Status status;
    stream_->Finish(&status, (void *)this);
    if (!cq_.Next(&tag, &ok) || !ok || tag != (void *)this) {
        LOG(ERROR) << "Keep Alive Finish was unsuccessful";
    }

    // Injecting status code to test when status is not 0
    INJECT_POINT("CheckGrpcStatus", [&status](int type) {
        status = grpc::Status(grpc::StatusCode(type), "error in grpc status");
        return Status::OK();
    });

    if (!status.ok()) {
        // If call is cancelled using try_cancel, return status is CANCELLED
        if (status.error_code() == grpc::StatusCode::CANCELLED) {
            LOG(INFO) << "Finish completed with result: cancelled";
        } else {
            LOG(ERROR) << "Finish stream with error: " << status.error_message();
        }
    }
    cq_.Shutdown();
    while (cq_.Next(&tag, &ok)) {
        ;
    }

    stream_.reset();

    return Status::OK();
}

Status EtcdKeepAlive::SendKeepAliveMessage(int64_t &timeTakenMilliseconds)
{
    INJECT_POINT("EtcdKeepAlive.SendKeepAliveMessage");
    int64_t totalTimeTakenMilliseconds = 0;
    static const int64_t toSecond = 1000;
    {
        // Submitting a task can only be done while synchronized behind the shutdown lock
        ReadLock lock(&shutdownLock_);
        if (shuttingDown_) {
            // Do not launch any task if we are shutting down
            return Status::OK();
        }
        auto traceId = Trace::Instance().GetTraceID();
        asyncRes_ = asyncTask_.Submit([this, traceId]() {
            TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
            return KeepAlive();
        });
    }

    // In case of any failures repeat call until lease is expired
    while (totalTimeTakenMilliseconds < GetLeaseExpiredMs()) {
        // Make a timebound call in separate thread
        // Wait for reply only for renewIntervalMilliseconds
        // steady clock is monotonic.
        int64_t beginTime = duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
        auto waitStatus = asyncRes_.wait_for(std::chrono::milliseconds(GetLeaseRenewIntervalMs()));
        int64_t endTime = duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
        int64_t timeCost = endTime - beginTime;
        timeTakenMilliseconds = static_cast<uint64_t>(timeTakenMilliseconds + timeCost) > INT64_MAX
                                    ? INT64_MAX
                                    : (timeTakenMilliseconds + timeCost);

        // Check any errors in the response of keep alive message
        if (std::future_status::ready == waitStatus) {
            std::pair<Status, EtcdResponse> keepAliveResponse;
            // Calling asyncRes_.get() changes the std::future back to invalid state.
            // A shutdown thread might be waiting for this event so it needs to be protected with lock and notified
            {
                std::unique_lock<std::mutex> lck(asyncResMtx_);
                keepAliveResponse = asyncRes_.get();
            }
            asyncResCond_.notify_one();

            if (keepAliveResponse.first.IsOk() && keepAliveResponse.second.ttl == 0) {
                // This means lease has expired
                RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, "Failed to refresh lease: the new ttl is 0.");
            }
            return keepAliveResponse.first;
        }

        // Keep track of total time taken in retry loop
        totalTimeTakenMilliseconds += timeCost;
        LOG(ERROR) << FormatString("Retrying KeepAlive taken %ld seconds, and the lease expiration time is %ld seconds",
                                   totalTimeTakenMilliseconds / toSecond, GetLeaseExpiredMs() / toSecond);
    }
    // Interrupt the clild before parent exits
    context_.TryCancel();
    asyncRes_.wait();  // wait for the child thread to complete
    std::pair<Status, EtcdResponse> keepAliveResponse;
    {
        std::unique_lock<std::mutex> lck(asyncResMtx_);
        keepAliveResponse = asyncRes_.get();
    }
    asyncResCond_.notify_one();
    if (keepAliveResponse.first.IsError()) {
        // This means sending keepalive is not successful and lease has expired
        RETURN_STATUS_LOG_ERROR(K_RPC_UNAVAILABLE, "SendKeepAlive Timeout");
    }
    return keepAliveResponse.first;
}

Status EtcdKeepAlive::Init(const std::string &authToken)
{
    RETURN_IF_NOT_OK(GrpcSession<etcdserverpb::Lease>::CreateSession(address_, leaseSession_));
    if (!authToken.empty()) {
        context_.AddMetadata("token", authToken);
    }
    stream_ = leaseSession_->Stub()->AsyncLeaseKeepAlive(&context_, &cq_, (void *)KEEPALIVE_CREATE);
    void *tag = nullptr;
    bool ok = false;
    if (!cq_.Next(&tag, &ok) || !ok || tag != (void *)KEEPALIVE_CREATE) {
        RETURN_STATUS_LOG_ERROR(StatusCode::K_KVSTORE_ERROR, "Init stream grpc failed!");
    }
    return Status::OK();
}

Status EtcdKeepAlive::Run(Timer &keepAliveTimeoutTimer)
{
    LOG(INFO) << "Main keep alive loop of etcd is running.";
    int64_t nextWaitMilliseconds = 0;
    // Check shuttingDown_ every 100ms to avoid waiting too long when exiting.
    const int64_t checkExitMilliseconds = 100;
    Timer livenessProbeCheckTimer;
    Status lastStatus;
    while (!shuttingDown_) {
        INJECT_POINT("heartbeat.sleep");
        int64_t endTime = 0;
        int64_t beginTime = duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
        auto currCheckExitMilliseconds = std::min(checkExitMilliseconds, nextWaitMilliseconds);
        do {
            std::this_thread::sleep_for(std::chrono::milliseconds(currCheckExitMilliseconds));
            endTime = duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
        } while (endTime - beginTime < nextWaitMilliseconds && !shuttingDown_);

        if (shuttingDown_) {
            break;
        }
        // If Run method return error, it could be triggered by the following reasons:
        // -1. Lease keep alive has timeout, we didn't receive any response from ETCD server.
        // -2. Failed to create a lease keepalive connection when stream write/read.

        // Send the keep alive message and keep track of time taken
        int64_t timeTakenMilliseconds = 0;
        lastStatus = LivenessHealthCheckEvent::GetInstance().NotifyAll(livenessProbeCheckTimer, lastStatus);
        if (lastStatus.IsError()) {
            LOG(ERROR) << lastStatus.ToString();
            continue;
        }
        auto res = SendKeepAliveMessage(timeTakenMilliseconds);
        if (res.IsError()) {
            return res;
        } else {
            keepAliveTimeoutTimer.Reset();
        }

        // Calculate how much time to wait before sending another keep alive message
        int64_t lriMilliseconds = GetLeaseRenewIntervalMs();
        nextWaitMilliseconds = timeTakenMilliseconds >= lriMilliseconds ? 0 : lriMilliseconds - (timeTakenMilliseconds);
    }

    LOG(INFO) << "Main keep alive loop exit with status OK (shutdown)";
    return Status::OK();
}

void ParseKeepAliveRsp(const etcdserverpb::LeaseKeepAliveResponse &rsp, EtcdResponse &response)
{
    FillEtcdHeader(rsp, response);
    response.leaseId = rsp.id();
    response.ttl = rsp.ttl();
}

std::pair<Status, EtcdResponse> EtcdKeepAlive::KeepAlive()
{
    EtcdResponse response;
    INJECT_POINT("worker.KeepAlive.send", [&response](int delayMs) {
        std::this_thread::sleep_for(std::chrono::milliseconds(delayMs));
        return std::make_pair(Status(K_RPC_UNAVAILABLE, "inject error"), response);
    });

    if (shuttingDown_) {
        // parse the last
        ParseKeepAliveRsp(rspPb_, response);
        return { Status::OK(), response };
    }

    etcdserverpb::LeaseKeepAliveRequest req;
    req.set_id(leaseId_);

    void *tag = nullptr;
    bool ok = false;
    stream_->Write(req, (void *)KEEPALIVE_WRITE);
    if (!cq_.Next(&tag, &ok) || !ok || tag != (void *)KEEPALIVE_WRITE) {
        LOG(ERROR) << "Failed to create a lease connection " << ok << " Tag ok? " << (tag == (void *)KEEPALIVE_WRITE);
        return { Status(StatusCode::K_RPC_UNAVAILABLE, ("Failed to create a lease keepalive connection")), response };
    }
    stream_->Read(&rspPb_, (void *)KEEPALIVE_READ);
    if (!cq_.Next(&tag, &ok) || !ok || tag != (void *)KEEPALIVE_READ) {
        LOG(ERROR) << "Failed to read in lease connection " << ok << " Tag ok? " << (tag == (void *)KEEPALIVE_READ);
        return { Status(StatusCode::K_RPC_UNAVAILABLE, "Failed to read in lease connection"), response };
    }
    ParseKeepAliveRsp(rspPb_, response);
    return { Status::OK(), response };
}

int64_t EtcdKeepAlive::GetLeaseExpiredMs()
{
    INJECT_POINT("GetLeaseExpiredMs", [](int64_t time) { return time; });
    const int64_t earlyExpiredMs = 5000;  // 5s
    const double rate = 0.9;
    int64_t exporedMs = static_cast<int64_t>(FLAGS_node_timeout_s) * kMillisecsPerSecond_;
    return std::max<int64_t>(exporedMs * rate, exporedMs - earlyExpiredMs);
}

int64_t EtcdKeepAlive::GetLeaseRenewIntervalMs()
{
    INJECT_POINT("GetLeaseRenewIntervalMs", [](int64_t time) { return time; });
    const uint32_t maxIntervalS = 60;
    const uint32_t retryTimes = 4;
    int64_t leaseRenewInterval = std::max(1u, std::min(maxIntervalS, FLAGS_node_timeout_s / retryTimes));
    return leaseRenewInterval * kMillisecsPerSecond_;
}
}  // namespace datasystem
