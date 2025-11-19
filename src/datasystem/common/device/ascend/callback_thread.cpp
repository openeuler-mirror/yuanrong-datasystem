/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: Defines the ascend callback thread.
 */

#include "datasystem/common/device/ascend/callback_thread.h"

#include "datasystem/common/log/trace.h"

namespace datasystem {
namespace acl {
CallbackThread::CallbackThread()
{
    aclDeviceManager_ = acl::AclDeviceManager::Instance();
    auto traceId = Trace::Instance().GetTraceID();
    thread_ = std::make_unique<std::thread>([this, traceId] {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
        constexpr uint32_t CALLBACK_TIMEOUT_MS = 100;
        wp_.Wait();
        while (!exitFlag_) {
            // Wait to execute callback tasks submitted via aclrtLaunchCallback interface
            // This processes pending asynchronous callback operations with a specified timeout
            (void)aclDeviceManager_->AclrtProcessReport(CALLBACK_TIMEOUT_MS);
        }
        LOG(INFO) << "CallbackThread exit.";
    });
    tid_ = thread_->native_handle();
}

CallbackThread::~CallbackThread()
{
    wp_.Set();
    exitFlag_ = true;
    if (thread_ != nullptr) {
        thread_->join();
    }
}

Status CallbackThread::SubscribeStream(aclrtStream stream)
{
    Status rc = aclDeviceManager_->AclrtSubscribeReport(tid_, stream);
    wp_.Set();
    return rc;
}

Status CallbackThread::UnSubscribeStream(aclrtStream stream)
{
    return aclDeviceManager_->AclrtUnSubscribeReport(tid_, stream);
}
}  // namespace acl
}  // namespace datasystem
