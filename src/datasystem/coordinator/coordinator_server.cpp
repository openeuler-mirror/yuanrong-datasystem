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

#include "datasystem/coordinator/coordinator_server.h"

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/logging.h"
#include "datasystem/common/rpc/rpc_auth_key_manager.h"
#include "datasystem/common/rpc/rpc_channel.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/util/gflag/flags.h"

DS_DECLARE_string(coordinator_address);
DS_DEFINE_uint64(coordinator_rpc_stub_cache_size, 2048, "Maximum coordinator RPC stub cache size.");

namespace datasystem {
namespace coordinator {
Status CoordinatorServer::Init()
{
    CHECK_FAIL_RETURN_STATUS(!FLAGS_coordinator_address.empty(), K_INVALID,
                             "The coordinator_address must be specified when starting coordinator.");
    RETURN_IF_NOT_OK(coordinatorAddr_.ParseString(FLAGS_coordinator_address));

    Logging::GetInstance()->Start("datasystem_coordinator");

    RpcCredential cred;
    RETURN_IF_NOT_OK(RpcAuthKeyManager::ServerLoadKeys(WORKER_SERVER_NAME, cred));
    builder_.SetCredential(cred);
    RETURN_IF_NOT_OK(RpcStubCacheMgr::Instance().Init(FLAGS_coordinator_rpc_stub_cache_size, coordinatorAddr_));

    memStore_ = std::make_shared<MemoryKvStore>();
    watchRegistry_ = std::make_shared<WatchRegistry>();
    watchDispatcher_ = std::make_shared<WatchDispatcherImpl>(watchRegistry_.get());
    clock_ = std::make_shared<SteadyClockReal>();
    ttlManager_ = std::make_shared<TtlManager>(clock_);
    store_ = std::make_shared<CoordinatorStore>(memStore_, watchRegistry_, watchDispatcher_, ttlManager_);
    coordinatorService_ = std::make_unique<CoordinatorServiceImpl>(coordinatorAddr_, store_);

    RpcServiceCfg cfg;
    cfg.numRegularSockets_ = FLAGS_rpc_thread_num;
    cfg.numStreamSockets_ = 0;
    cfg.hwm_ = RPC_LIGHT_SERVICE_HWM;
    cfg.udsEnabled_ = false;

    builder_.AddEndPoint(RpcChannel::TcpipEndPoint(coordinatorAddr_));
    builder_.AddService(coordinatorService_.get(), cfg);
    return Status::OK();
}

Status CoordinatorServer::Start()
{
    RETURN_IF_NOT_OK(builder_.Init(rpcServer_));
    RETURN_IF_NOT_OK(builder_.BuildAndStart(rpcServer_));
    LOG(INFO) << "datasystem coordinator started at " << coordinatorAddr_.ToString();
    return Status::OK();
}

Status CoordinatorServer::Shutdown()
{
    LOG(INFO) << "Coordinator process executing a shutdown.";
    if (rpcServer_ != nullptr) {
        rpcServer_->Shutdown();
        rpcServer_.reset();
    }
    coordinatorService_.reset();
    store_.reset();
    ttlManager_.reset();
    clock_.reset();
    watchDispatcher_.reset();
    watchRegistry_.reset();
    memStore_.reset();
    LOG(INFO) << "Coordinator shutdown success.";
    return Status::OK();
}
}  // namespace coordinator
}  // namespace datasystem
