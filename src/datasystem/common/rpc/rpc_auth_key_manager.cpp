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
 * Description: Helper functions to load curve public/private key pair.
 */
#include "datasystem/common/rpc/rpc_auth_key_manager.h"
#include "datasystem/common/rpc/rpc_constants.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/log/log.h"

namespace datasystem {
Status RpcAuthKeyManager::CopyCurveAuthKey(const char *src, std::unique_ptr<char[]> &dest)
{
    CHECK_FAIL_RETURN_STATUS(src != nullptr, K_INVALID, "Source pointer should not be null");
    size_t len = std::strlen(src) + 1;
    try {
        dest = std::make_unique<char[]>(len);
    } catch (const std::bad_alloc &e) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, e.what());
    }
    int ret = strcpy_s(dest.get(), len, src);
    CHECK_FAIL_RETURN_STATUS(ret == EOK, StatusCode::K_RUNTIME_ERROR,
                             FormatString("Copy key failed, the strcpy_s return: %d", ret));
    return Status::OK();
}
}  // namespace datasystem
