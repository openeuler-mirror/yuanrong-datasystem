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

/**
 * Description: Client mmap table management.
 */
#include "datasystem/client/mmap/shm_mmap_table.h"

#include <atomic>
#include <cstddef>
#include <exception>

#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"

namespace datasystem {
namespace client {
ShmMmapTable::~ShmMmapTable() = default;

Status ShmMmapTable::MmapAndStoreFd(const int &clientFd, const int &workerFd, const uint64_t &mmapSize,
                                    const std::string &tenantId, const std::string &clientId)
{
    bthread::RWLockWrGuard l(mutex_);
    auto entry = mmapTable_.find(workerFd);
    if (entry == mmapTable_.end()) {
        // Check the workerFd and clientFd whether is valid.
        if (workerFd > 0 && clientFd > 0) {
            LOG(INFO) << FormatString("Client id: %s, worker fd: %d, mmap the client fd %d, mmap size is %llu",
                                      clientId, workerFd, clientFd, mmapSize);
            auto newEntry = std::make_shared<ShmMmapTableEntry>(clientFd, mmapSize, clientId);
            RETURN_IF_NOT_OK(newEntry->Init(enableHugeTlb_, tenantId));
            SubmitHostMemoryPin(newEntry);
            mmapTable_[workerFd] = std::move(newEntry);
        }
    } else {
        LOG(INFO) << FormatString("The client fd %d exists, no need to mmap again", clientFd);
    }
    return Status::OK();
}

void ShmMmapTable::SubmitHostMemoryPin(const std::shared_ptr<ShmMmapTableEntry> &entry)
{
    try {
        if (pinThread_ == nullptr) {
            pinThread_ = std::make_unique<ThreadPool>(1, 1, "cuda_host_pin");
        }
        pinThread_->Execute([entry] { entry->PinHostMemory(); });
    } catch (const std::exception &e) {
        entry->SkipHostMemoryPin();
        LOG(WARNING) << "Submit CUDA host memory pin task failed: " << e.what();
    } catch (...) {
        entry->SkipHostMemoryPin();
        LOG(WARNING) << "Submit CUDA host memory pin task failed with an unknown exception";
    }
}
}  // namespace client
}  // namespace datasystem
