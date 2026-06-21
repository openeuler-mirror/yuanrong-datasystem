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

#include "datasystem/coordinator/coordinator_service_impl.h"

namespace datasystem {
namespace coordinator {
namespace {
Status CoordinatorStoreNotBound()
{
    return Status(StatusCode::K_NOT_READY, "coordinator store is not bound");
}
}  // namespace

Status CoordinatorServiceImpl::Put(const PutReqPb &req, PutRspPb &rsp)
{
    (void)req;
    (void)rsp;
    return CoordinatorStoreNotBound();
}

Status CoordinatorServiceImpl::Range(const RangeReqPb &req, RangeRspPb &rsp)
{
    (void)req;
    (void)rsp;
    return CoordinatorStoreNotBound();
}

Status CoordinatorServiceImpl::DeleteRange(const DeleteRangeReqPb &req, DeleteRangeRspPb &rsp)
{
    (void)req;
    (void)rsp;
    return CoordinatorStoreNotBound();
}

Status CoordinatorServiceImpl::WatchRange(const WatchRangeReqPb &req, WatchRangeRspPb &rsp)
{
    (void)req;
    (void)rsp;
    return CoordinatorStoreNotBound();
}

Status CoordinatorServiceImpl::KeepAlive(const KeepAliveReqPb &req, KeepAliveRspPb &rsp)
{
    (void)req;
    (void)rsp;
    return CoordinatorStoreNotBound();
}
}  // namespace coordinator
}  // namespace datasystem
