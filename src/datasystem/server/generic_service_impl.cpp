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
 * Description: GenericServiceImpl is used to implement common service of rpc server(worker/master/gcs).
 */
#include "datasystem/server/generic_service_impl.h"

#include <string>

#include "datasystem/common/log/log.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/protos/generic_service.pb.h"

#ifdef BUILD_COVERAGE
extern "C" void __gcov_flush();
#endif

namespace datasystem {
Status GenericServiceImpl::GcovFlush(const GcovFlushReqPb &req, GcovFlushRspPb &resp)
{
    (void)req;
    (void)resp;
    LOG(INFO) << "Received GcovFlush request from " << GetLocalAddr().ToString();
#ifdef BUILD_COVERAGE
    LOG(INFO) << "Start to process GcovFlush request.";
    __gcov_flush();
    LOG(INFO) << "Finished to process GcovFlush request.";
#else
    LOG(WARNING) << "Build code with no coverage build parameters";
#endif
    return Status::OK();
}
Status GenericServiceImpl::SetInjectAction(const datasystem::SetInjectActionReqPb &req,
                                           datasystem::SetInjectActionRspPb &rsp)
{
    (void)rsp;
    LOG(INFO) << "Received set inject action request from " << GetLocalAddr().ToString()
              << ", req:" << req.ShortDebugString();
#ifdef WITH_TESTS
    RETURN_IF_NOT_OK(inject::Set(req.name(), req.action()));
#else
    (void)req;
    LOG(WARNING) << "Build code with no tests";
#endif
    return Status::OK();
}

Status GenericServiceImpl::ClearInjectAction(const datasystem::ClearInjectActionReqPb &req,
                                             datasystem::ClearInjectActionRspPb &rsp)
{
    (void)rsp;
    LOG(INFO) << "Received clear inject action request from " << GetLocalAddr().ToString()
              << ", req:" << req.ShortDebugString();
#ifdef WITH_TESTS
    RETURN_IF_NOT_OK(inject::Clear(req.name()));
#else
    (void)req;
    LOG(WARNING) << "Build code with no tests";
#endif
    return Status::OK();
}

Status GenericServiceImpl::GetInjectActionExecuteCount(const datasystem::GetInjectActionExecuteCountReqPb &req,
                                                       datasystem::GetInjectActionExecuteCountRspPb &rsp)
{
#ifdef WITH_TESTS
    rsp.set_execute_count(inject::GetExecuteCount(req.name()));
#else
    (void)req;
    (void)rsp;
    LOG(WARNING) << "Build code with no tests";
#endif
    return Status::OK();
}

}  // namespace datasystem
