/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
 * Description: Lease gRPC service implementation for metastore service.
 */
#include "datasystem/common/kvstore/metastore/service/lease_service_impl.h"

#include "datasystem/common/log/log.h"

namespace datasystem {

LeaseServiceImpl::LeaseServiceImpl(LeaseManager *leaseManager, KVManager *kvManager)
    : leaseManager_(leaseManager), kvManager_(kvManager)
{
}

void LeaseServiceImpl::FillResponseHeader(::etcdserverpb::ResponseHeader *header)
{
    header->set_revision(kvManager_->CurrentRevision());
}

::grpc::Status LeaseServiceImpl::LeaseGrant(::grpc::ServerContext *context,
                                            const ::etcdserverpb::LeaseGrantRequest *request,
                                            ::etcdserverpb::LeaseGrantResponse *response)
{
    (void)context;
    LOG(INFO) << "LeaseGrant RPC: ttl=" << request->ttl();

    int64_t leaseId = 0;
    int64_t actualTtl = 0;
    Status status = leaseManager_->Grant(request->ttl(), &leaseId, &actualTtl);
    if (status.IsError()) {
        return ::grpc::Status(::grpc::StatusCode::INTERNAL, status.GetMsg());
    }

    FillResponseHeader(response->mutable_header());
    response->set_id(leaseId);
    response->set_ttl(actualTtl);

    return ::grpc::Status::OK;
}

::grpc::Status LeaseServiceImpl::LeaseKeepAlive(
    ::grpc::ServerContext *context,
    ::grpc::ServerReaderWriter<::etcdserverpb::LeaseKeepAliveResponse, ::etcdserverpb::LeaseKeepAliveRequest> *stream)
{
    (void)context;
    LOG(INFO) << "LeaseKeepAlive RPC stream started";

    ::etcdserverpb::LeaseKeepAliveRequest req;
    while (stream->Read(&req)) {
        LOG(INFO) << "LeaseKeepAlive request: lease_id=" << req.id();
        ::etcdserverpb::LeaseKeepAliveResponse resp;
        int64_t ttl = 0;
        Status status = leaseManager_->KeepAlive(req.id(), &ttl);
        if (status.IsError()) {
            return ::grpc::Status(::grpc::StatusCode::INTERNAL, status.GetMsg());
        }

        FillResponseHeader(resp.mutable_header());
        resp.set_id(req.id());
        resp.set_ttl(ttl);

        if (!stream->Write(resp)) {
            LOG(INFO) << "LeaseKeepAlive stream write failed, client disconnected";
            break;
        }
    }

    LOG(INFO) << "LeaseKeepAlive RPC stream ended";
    return ::grpc::Status::OK;
}

}  // namespace datasystem
