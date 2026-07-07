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

#include "datasystem/coordinator/watch_dispatcher_impl.h"

#include <algorithm>

#include "datasystem/common/rpc/rpc_constants.h"
#include "datasystem/common/rpc/rpc_options.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/coordinator.stub.rpc.pb.h"
#include "datasystem/protos/coordinator.brpc.stub.pb.h"

namespace datasystem {
namespace coordinator {
namespace {
constexpr int32_t WATCH_NOTIFY_RPC_TIMEOUT_MS = 3000;

int32_t GetWatchNotifyRpcTimeoutMs()
{
    return std::min<int32_t>(WATCH_NOTIFY_RPC_TIMEOUT_MS, static_cast<int32_t>(FLAGS_node_timeout_s) * TO_MILLISECOND);
}

Status ConvertEventType(WatchEvent::Type type, EventPb::EventType &pbType)
{
    switch (type) {
        case WatchEvent::Type::PUT:
            pbType = EventPb::PUT;
            return Status::OK();
        case WatchEvent::Type::DELETE:
            pbType = EventPb::DELETE;
            return Status::OK();
        case WatchEvent::Type::REWATCH:
            RETURN_STATUS(StatusCode::K_TRY_AGAIN, "watch channel requires rewatch");
        default:
            RETURN_STATUS(StatusCode::K_INVALID, "unknown watch event type");
    }
}

void FillEventKv(const KeyValueEntry &entry, KeyValue *kv)
{
    kv->set_key(entry.key);
    kv->set_value(entry.value);
    kv->set_version(entry.version);
    kv->set_mod_revision(entry.modRevision);
}
}  // namespace

Status WatchDispatcherImpl::DoNotify(int64_t watchId, const std::string &watcherAddr,
                                     std::vector<std::shared_ptr<WatchEvent>> &events)
{
    HostPort watcherHostPort;
    RETURN_IF_NOT_OK(watcherHostPort.ParseString(watcherAddr));

    std::shared_ptr<RpcStubBase> rpcStub;
    RETURN_IF_NOT_OK(RpcStubCacheMgr::Instance().GetStub(watcherHostPort, StubType::COORDINATOR_WORKER_SVC, rpcStub));

    EventReqPb req;
    req.set_watch_id(watchId);
    for (const auto &event : events) {
        CHECK_FAIL_RETURN_STATUS(event != nullptr, StatusCode::K_INVALID, "watch event is null");
        EventPb::EventType pbType = EventPb::EVENT_TYPE_UNSPECIFIED;
        RETURN_IF_NOT_OK(ConvertEventType(event->type, pbType));
        auto *pbEvent = req.add_events();
        pbEvent->set_type(pbType);
        FillEventKv(event->entry, pbEvent->mutable_kv());
    }
    RpcOptions opts;
    opts.SetTimeout(GetWatchNotifyRpcTimeoutMs());
    EventRspPb rsp;
    if (FLAGS_use_brpc) {
        auto brpcStub = std::dynamic_pointer_cast<CoordinatorWatchService_BrpcGenericStub>(rpcStub);
        RETURN_RUNTIME_ERROR_IF_NULL(brpcStub);
        return brpcStub->HandleEvent(opts, req, rsp);
    }
    auto stub = std::dynamic_pointer_cast<CoordinatorWatchService_Stub>(rpcStub);
    RETURN_RUNTIME_ERROR_IF_NULL(stub);
    return stub->HandleEvent(opts, req, rsp);
}
}  // namespace coordinator
}  // namespace datasystem
