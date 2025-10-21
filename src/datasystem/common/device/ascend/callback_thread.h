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

#ifndef DATASYSTEM_COMMON_DEVICE_ACL_CALLBACK_THREAD_H
#define DATASYSTEM_COMMON_DEVICE_ACL_CALLBACK_THREAD_H

#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/device/ascend/acl_device_manager.h"

namespace datasystem {
namespace acl {
class CallbackThread {
public:
    CallbackThread();
    ~CallbackThread();

    Status SubscribeStream(aclrtStream stream);
    Status UnSubscribeStream(aclrtStream stream);

private:
    acl::AclDeviceManager *aclDeviceManager_;
    std::atomic<bool> exitFlag_{ false };
    std::unique_ptr<std::thread> thread_;
    uint64_t tid_;
    WaitPost wp_;
};
}  // namespace acl
}  // namespace datasystem

#endif
