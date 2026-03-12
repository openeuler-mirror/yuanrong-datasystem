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
 * Description: Disk mmap instance.
 */
#include "datasystem/common/shared_memory/mmap/flexible_mmap.h"

#include <sys/mman.h>
#include <unistd.h>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace memory {

FlexibleMmap::FlexibleMmap(AllocatorFuncRegister funcRegister)
{
    createFunc_ = std::move(funcRegister.createFunc);
    destroyFunc_ = std::move(funcRegister.destroyFunc);
}

Status FlexibleMmap::Initialize(uint64_t size, bool populate, bool hugepage)
{
    (void)populate;
    (void)hugepage;
    auto rc = createFunc_(&pointer_, size);
    if (pointer_ != nullptr) {
        mmapSize_ = size;
        curr_ = reinterpret_cast<uintptr_t>(pointer_);
        tail_ = curr_ + static_cast<uintptr_t>(mmapSize_);
    }
    return rc;
}

void FlexibleMmap::Destroy()
{
    LOG_IF_ERROR(destroyFunc_(pointer_, mmapSize_), "Faied in destroy function");
}

FlexibleMmap::~FlexibleMmap()
{
    if (pointer_ == nullptr) {
        return;
    }
    LOG_IF_ERROR(destroyFunc_(pointer_, mmapSize_), "Faied in destroy function");
}

}  // namespace memory
}  // namespace datasystem