/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: Check etcd health
 */

#include "datasystem/common/kvstore/etcd/etcd_health.h"

#include <grpcpp/grpcpp.h>

#include "datasystem/common/kvstore/etcd/grpc_session.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
Status CheckEtcdHealth(const std::string &address)
{
    std::unique_ptr<GrpcSession<etcdserverpb::Maintenance>> maintenanceSession;
    RETURN_IF_NOT_OK(GrpcSession<etcdserverpb::Maintenance>::CreateSession(address, maintenanceSession));

    etcdserverpb::StatusRequest req;
    etcdserverpb::StatusResponse rsp;
    const int defaultTimeout = 10;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(defaultTimeout));

    auto rc = maintenanceSession->Stub()->Status(&context, req, &rsp);
    CHECK_FAIL_RETURN_STATUS(rc.ok(), K_RUNTIME_ERROR, "Connect to etcd failed, as: " + rc.error_message());
    maintenanceSession->Shutdown();
    return Status::OK();
}
}  // namaespace datasystem