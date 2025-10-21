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
#include "datasystem/common/shared_memory/mmap/disk_mmap.h"

#include <sys/mman.h>
#include <unistd.h>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/utils/status.h"

DS_DEFINE_string(
    shared_disk_directory, "",
    "Disk cache data placement directory, default value is empty, indicating that disk cache is not enabled.");
DS_DEFINE_validator(shared_disk_directory, &Validator::ValidatePathString);

namespace datasystem {
namespace memory {

Status DiskMmap::Initialize(uint64_t size, bool populate, bool hugepage)
{
    (void)hugepage;
    errno = 0;
    // Create tmp file.
    const int permission = 0600;
    fd_ = open(FLAGS_shared_disk_directory.c_str(), O_TMPFILE | O_RDWR, permission);
    INJECT_POINT("DiskMmap.OpenTmpFail", []() {
        errno = EOPNOTSUPP;
        return Status::OK();
    });
    if (errno == EOPNOTSUPP) {
        LOG(WARNING) << "The filesystem not support O_TMPFILE, falling back to mktemp.";
        std::string tmpFile = FLAGS_shared_disk_directory + "/datasystem-XXXXXX";
        char templateName[tmpFile.size() + 1];
        auto ret = strcpy_s(templateName, tmpFile.size() + 1, tmpFile.c_str());
        CHECK_FAIL_RETURN_STATUS(ret == EOK, StatusCode::K_RUNTIME_ERROR,
                                 FormatString("strcpy_s tmpFile failed: %d", ret));
        fd_ = mkstemp(templateName);
        if (fd_ >= 0) {
            if (unlink(templateName) == -1) {
                close(fd_);
                return Status(StatusCode::K_RUNTIME_ERROR,
                              "unlink " + std::string(templateName) + " failed: " + StrErr(errno));
            }
        }
    }
    CHECK_FAIL_RETURN_STATUS(fd_ >= 0, StatusCode::K_RUNTIME_ERROR, "Create tmp file failed: " + StrErr(errno));

    unsigned int flags = MAP_SHARED;
    if (populate) {
        flags |= MAP_POPULATE;
    }
    type_ = "disk";
    return SetupFileMapping(size, flags, false);
}

}  // namespace memory
}  // namespace datasystem