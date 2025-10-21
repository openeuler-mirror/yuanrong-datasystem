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
 * Description: Context util.
 */

#include "datasystem/context/context.h"

#include "datasystem/common/log/log.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/util/validator.h"

namespace datasystem {
Status Context::SetTraceId(const std::string &traceId)
{
    CHECK_FAIL_RETURN_STATUS(Validator::IsTraceIdFormat(traceId), K_INVALID,
                             "The trace id contains illegal char(s). traceId not match regex "
                             "\"^[a-zA-Z0-9\\~\\.\\-\\/_!@#%\\^\\&\\*\\(\\)\\+\\=\\:;]*$\"");
    CHECK_FAIL_RETURN_STATUS(traceId.length() <= Trace::TRACEID_PREFIX_SIZE, K_INVALID,
                             FormatString("The length of trace id should less than %d", Trace::TRACEID_PREFIX_SIZE));
    Trace::Instance().SetPrefix(traceId);
    VLOG(1) << "set trace id:" << traceId;
    return Status::OK();
}

void Context::SetTenantId(const std::string &tenantId)
{
    g_ContextTenantId = tenantId;
    LOG(INFO) << "set tenant id : " << g_ContextTenantId;
}
}  // namespace datasystem