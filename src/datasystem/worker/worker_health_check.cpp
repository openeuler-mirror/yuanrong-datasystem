/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "datasystem/worker/worker_health_check.h"

#include <cerrno>
#include <unistd.h>

#include "datasystem/common/log/log.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/uri.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/utils/status.h"

DS_DEFINE_string(health_check_path, "",
                 "File will create after the worker successfully "
                 "sends a heartbeat message to the master for the first time. "
                 "It is used to detect whether the container is ready in the k8s scenario."
                 "The path length must less than 4095 characters.");
DS_DEFINE_validator(health_check_path, &Validator::ValidatePathString);

namespace datasystem {
std::atomic<bool> g_health{ false };
std::atomic<bool> g_topologyServingAdmission{ true };

Status ResetHealthProbe()
{
    g_health.store(false, std::memory_order_release);
    if (FLAGS_health_check_path.empty()) {
        return Status::OK();
    }
    LOG(INFO) << "Worker is starting up, health probe reset.";
    RETURN_IF_NOT_OK(Uri::NormalizePathWithUserHomeDir(FLAGS_health_check_path, "~/.datasystem/probe/healthy", ""));
    std::string fileDir = FLAGS_health_check_path.substr(0, FLAGS_health_check_path.find_last_of('/'));
    if (!FileExist(fileDir)) {
        // Change the permission of "~/.datasystem/probe" to 0700.
        const mode_t per = 0700;
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(CreateDir(fileDir, true, per), "Create healthy check file failed!");
    }
    return RevokeHealthProbe();
}

Status SetHealthProbe()
{
    if (FLAGS_health_check_path.empty()) {
        g_health.store(true, std::memory_order_release);
        return Status::OK();
    }
    LOG(INFO) << "Worker is healthy, health probe set.";
    auto rc = AtomicWriteTextFile(FLAGS_health_check_path, "health check success\n");
    if (rc.IsError()) {
        auto rollbackRc = RevokeHealthProbe();
        if (rollbackRc.IsError()) {
            return Status(rollbackRc.GetCode(),
                          rollbackRc.GetMsg() + "; original health publication error: " + rc.ToString());
        }
        return rc;
    }
    g_health.store(true, std::memory_order_release);
    return Status::OK();
}

Status RevokeHealthProbe()
{
    g_health.store(false, std::memory_order_release);
    if (FLAGS_health_check_path.empty()) {
        return Status::OK();
    }
    if (unlink(FLAGS_health_check_path.c_str()) == 0 || errno == ENOENT) {
        return Status::OK();
    }
    const int error = errno;
    RETURN_STATUS(K_IO_ERROR, FormatString("Delete health marker %s failed with errno: %d, errmsg: %s",
                                           FLAGS_health_check_path, error, StrErr(error)));
}

bool IsHealthy()
{
    return g_health.load() && g_topologyServingAdmission.load();
}

void SetTopologyServingAdmission(bool allowed)
{
    const bool previous = g_topologyServingAdmission.exchange(allowed);
    if (previous != allowed) {
        LOG(INFO) << "Cluster topology business admission is now " << (allowed ? "open" : "closed");
    }
}

void SetUnhealthy()
{
    LOG(INFO) << "Worker is unhealthy now!";
    g_health.store(false, std::memory_order_release);
}
}  // namespace datasystem
