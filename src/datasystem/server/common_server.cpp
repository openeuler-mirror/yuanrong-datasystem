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
 * Description: CommonServer is base rpc server of worker/master/gcs.
 */
#include "datasystem/server/common_server.h"

#include "datasystem/common/log/log.h"
#include "datasystem/common/eventloop/timer_queue.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/uri.h"
#include "datasystem/common/util/validator.h"

DS_DEFINE_int32(rpc_thread_num, 16, "Thread number of OC HandleRequest, must great than or equal to 0.");
DS_DEFINE_validator(rpc_thread_num, &Validator::ValidateRpcThreadNum);
DS_DECLARE_string(unix_domain_socket_dir);

namespace datasystem {
CommonServer::CommonServer(HostPort hostPort, HostPort bindHostPort)
    : hostPort_(std::move(hostPort)), bindHostPort_(std::move(bindHostPort))
{
}

Status CommonServer::CreateGenericService()
{
    const int32_t smallSvc = 1;
    RpcServiceCfg cfg;
    cfg.numStreamSockets_ = 0;
    cfg.numRegularSockets_ = smallSvc;
    cfg.hwm_ = RPC_LIGHT_SERVICE_HWM;
    cfg.udsEnabled_ = false;
#ifdef WITH_TESTS
    genericSvc_ = std::make_unique<GenericServiceImpl>(bindHostPort_);
    RETURN_IF_NOT_OK(genericSvc_->Init());
    builder_.AddService(genericSvc_.get(), cfg);
#endif
    return Status::OK();
}

Status CommonServer::Init()
{
    eventLoop_ = std::make_shared<SockEventLoop>();
    RETURN_IF_NOT_OK(eventLoop_->Init());
    builder_.AddEndPoint(RpcChannel::TcpipEndPoint(bindHostPort_));
    RETURN_IF_NOT_OK(CreateGenericService());
    CHECK_FAIL_RETURN_STATUS(TimerQueue::GetInstance()->Initialize(), K_RUNTIME_ERROR, "TimerQueue init failed!");
    RETURN_IF_NOT_OK(builder_.Init(rpcServer_));
    return Status::OK();
}

Status CommonServer::Start()
{
    return builder_.BuildAndStart(rpcServer_);
}

Status CommonServer::Shutdown()
{
    if (rpcServer_)
        rpcServer_->Shutdown();
    return Status::OK();
}

ThreadPool::ThreadPoolUsage CommonServer::GetRpcServicesUsage(const std::string &serviceName)
{
    return rpcServer_->GetRpcServicesUsage(serviceName);
}
}  // namespace datasystem
