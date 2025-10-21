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
 * Description: A writer preference lock implementation.
 */
#include "datasystem/common/util/locks.h"
#include "datasystem/common/log/log.h"

namespace datasystem {

#ifdef WITH_TESTS
void WriterPrefRWLock::CheckReader()
{
    std::shared_lock<std::shared_timed_mutex> locker(mutex_);
    if (readerThreadIds_.count(std::this_thread::get_id()) > 0) {
        LOG(ERROR) << "Not allowed to acquire multiple read locks from the same thread.";
    }
}
#endif

}  // namespace datasystem
