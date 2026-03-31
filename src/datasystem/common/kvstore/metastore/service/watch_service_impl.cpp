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
 * Description: Watch gRPC service implementation for metastore service.
 */
#include "datasystem/common/kvstore/metastore/service/watch_service_impl.h"

#include "datasystem/common/log/log.h"

namespace datasystem {

WatchServiceImpl::WatchServiceImpl(WatchManager *watchManager, KVManager *kvManager)
    : watchManager_(watchManager), kvManager_(kvManager)
{
}

WatchServiceImpl::~WatchServiceImpl()
{
}

void WatchServiceImpl::FillResponseHeader(::etcdserverpb::ResponseHeader *header)
{
    header->set_revision(kvManager_->CurrentRevision());
}

::grpc::Status WatchServiceImpl::Watch(
    ::grpc::ServerContext *context,
    ::grpc::ServerReaderWriter<::etcdserverpb::WatchResponse, ::etcdserverpb::WatchRequest> *stream)
{
    (void)context;
    LOG(INFO) << "Watch RPC stream started";

    // Handle stream requests until client disconnects
    ::etcdserverpb::WatchRequest req;
    while (stream->Read(&req)) {
        if (req.has_create_request()) {
            int64_t watchId = 0;
            LOG(INFO) << "Watch create request: key=" << req.create_request().key()
                      << ", start_revision=" << req.create_request().start_revision();
            Status status = watchManager_->RegisterWatcher(req.create_request(), stream, &watchId, kvManager_);
            if (status.IsError()) {
                LOG(ERROR) << "Failed to register watcher: " << status.GetMsg();
                // Send error response
                ::etcdserverpb::WatchResponse resp;
                auto *header = resp.mutable_header();
                FillResponseHeader(header);
                resp.set_watch_id(watchId);
                resp.set_canceled(true);
                stream->Write(resp);
                return ::grpc::Status(::grpc::StatusCode::INTERNAL, status.GetMsg());
            }
        } else if (req.has_cancel_request()) {
            LOG(INFO) << "Watch cancel request: watch_id=" << req.cancel_request().watch_id();
            watchManager_->CancelWatcher(req.cancel_request().watch_id());
        }
    }

    // Client disconnected, clean up all watchers for this stream
    LOG(INFO) << "Watch RPC stream ended, cleaning up all watchers";
    watchManager_->CancelWatchersByStream(stream);

    return ::grpc::Status::OK;
}

}  // namespace datasystem
