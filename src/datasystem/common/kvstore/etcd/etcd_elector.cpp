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
#include "datasystem/common/kvstore/etcd/etcd_elector.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
template <typename T>
void FillEtcdHeader(const T &pb, EtcdResponse &rsp)
{
    rsp.header.clusterId = pb.header().cluster_id();
    rsp.header.memberId = pb.header().member_id();
    rsp.header.revision = pb.header().revision();
    rsp.header.raftTerm = pb.header().raft_term();
}

Status EtcdElector::Init()
{
    RETURN_IF_NOT_OK(GrpcSession<etcdserverpb::Lease>::CreateSession(address_, leaseSession_));
    RETURN_IF_NOT_OK(GrpcSession<v3electionpb::Election>::CreateSession(address_, electionSession_));
    return Status::OK();
}

void EtcdElector::Shutdown()
{
    leaseSession_->Shutdown();
    electionSession_->Shutdown();
}

Status EtcdElector::Campaign(const std::string &name, int64_t lease, const std::string &value, EtcdResponse &response)
{
    v3electionpb::CampaignRequest req;
    v3electionpb::CampaignResponse rsp;
    req.set_name(name);
    req.set_lease(lease);
    req.set_value(value);
    RETURN_IF_NOT_OK(
        electionSession_->AsyncSendRpc("Campaign", req, rsp, &v3electionpb::Election::Stub::AsyncCampaign, ""));
    FillEtcdHeader(rsp, response);
    response.name = rsp.leader().name();
    response.key = rsp.leader().key();
    response.createRevision = rsp.leader().rev();
    response.leaseId = rsp.leader().lease();
    return Status::OK();
}

void ParseLeaderRsp(const v3electionpb::LeaderResponse &rsp, EtcdResponse &response)
{
    FillEtcdHeader(rsp, response);
    response.key = rsp.kv().key();
    response.value = rsp.kv().value();
    response.createRevision = rsp.kv().create_revision();
    response.modRevision = rsp.kv().mod_revision();
    response.version = rsp.kv().version();
    response.leaseId = rsp.kv().lease();
}

Status EtcdElector::Leader(const std::string &name, EtcdResponse &response)
{
    v3electionpb::LeaderRequest req;
    v3electionpb::LeaderResponse rsp;
    req.set_name(name);
    RETURN_IF_NOT_OK(electionSession_->AsyncSendRpc("Leader", req, rsp, &v3electionpb::Election::Stub::AsyncLeader, ""));
    ParseLeaderRsp(rsp, response);
    return Status::OK();
}

Status EtcdElector::Resign(const std::string &name, const std::string &key, int64_t revision, int64_t lease,
                           EtcdResponse &response)
{
    v3electionpb::ResignRequest req;
    v3electionpb::ResignResponse rsp;
    auto leader = req.mutable_leader();
    leader->set_name(name);
    leader->set_key(key);
    leader->set_rev(revision);
    leader->set_lease(lease);
    RETURN_IF_NOT_OK(electionSession_->AsyncSendRpc("Resign", req, rsp, &v3electionpb::Election::Stub::AsyncResign, ""));
    FillEtcdHeader(rsp, response);
    return Status::OK();
}

std::unique_ptr<EtcdObserver> EtcdElector::Observe(const std::string &name,
                                                   const std::function<void(const EtcdResponse &)> &callback, bool once)
{
    auto observer = std::make_unique<EtcdObserver>(address_, name, callback, once);
    if (observer->Init().IsError()) {
        return nullptr;
    }
    observer->Start();
    return observer;
}

std::unique_ptr<EtcdKeepAlive> EtcdElector::LeaseKeepAlive(int ttl)
{
    etcdserverpb::LeaseGrantRequest req;
    etcdserverpb::LeaseGrantResponse rsp;
    req.set_ttl(ttl);
    req.set_id(0);
    if (leaseSession_->AsyncSendRpc("LeaseGrant", req, rsp, &etcdserverpb::Lease::Stub::AsyncLeaseGrant, "").IsError()) {
        LOG(ERROR) << "LeaseGrant error: " << rsp.error();
        return nullptr;
    }
    auto keepAlive = std::make_unique<EtcdKeepAlive>(address_, rsp.id());
    if (keepAlive->Init().IsError()) {
        return nullptr;
    }
    return keepAlive;
}

EtcdObserver::~EtcdObserver()
{
    if (!running_) {
        return;
    }
    Shutdown();
    thread_.join();
}

void EtcdObserver::Shutdown()
{
    if (running_.exchange(false)) {
        context_->TryCancel();
    }
}

Status EtcdObserver::Init()
{
    RETURN_IF_NOT_OK(GrpcSession<v3electionpb::Election>::CreateSession(address_, electionSession_));
    v3electionpb::LeaderRequest req;
    req.set_name(name_);
    context_ = std::make_unique<grpc::ClientContext>();
    streamReader_ = electionSession_->Stub()->Observe(&(*context_), req);
    if (streamReader_ == nullptr) {
        RETURN_STATUS_LOG_ERROR(StatusCode::K_KVSTORE_ERROR, "Failed to create a observe connection");
    }
    running_ = true;
    return Status::OK();
}

void EtcdObserver::Start()
{
    thread_ = Thread([this]() { Observe(); });
}

void EtcdObserver::Observe()
{
    v3electionpb::LeaderResponse rsp;
    EtcdResponse response;
    do {
        bool ok = streamReader_->Read(&rsp);
        if (!ok && running_) {
            LOG(ERROR) << "Observer meets error, need to reconnect to ETCD server...";
            std::this_thread::sleep_for(std::chrono::seconds(1));
            LOG_IF_ERROR(Init(), "EtcdObserver init failed");
            continue;
        }
        if (!ok || !running_) {
            LOG(INFO) << "Exit observer background thread.";
            break;
        }
        ParseLeaderRsp(rsp, response);
        callback_(response);
        if (!running_ || once_) {
            break;
        }
    } while (true);
}
}  // namespace datasystem
