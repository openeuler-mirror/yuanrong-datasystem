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
                 "The Worker creates this file after startup and renews its mtime after each successful liveness "
                 "check. An empty path disables the external file probe. The path length must be less than 4095 "
                 "characters.");
DS_DEFINE_validator(health_check_path, &Validator::ValidatePathString);

namespace datasystem {
std::atomic<bool> g_health{ false };
std::atomic<bool> g_topologyServingAdmission{ true };

Status ResetWorkerProbe()
{
    if (FLAGS_health_check_path.empty()) {
        return Status::OK();
    }
    RETURN_IF_NOT_OK(Uri::NormalizePathWithUserHomeDir(FLAGS_health_check_path, "~/.datasystem/probe/worker", ""));
    const std::string parentDir = Uri(FLAGS_health_check_path).GetParent();
    const std::string fileDir = parentDir.empty() ? "." : parentDir;
    if (!FileExist(fileDir)) {
        const mode_t per = 0700;
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(CreateDir(fileDir, true, per), "Create worker probe directory failed");
    }
    LOG(INFO) << "Worker is starting up, remove stale worker probe.";
    return RemoveWorkerProbe();
}

Status PublishWorkerProbe()
{
    if (FLAGS_health_check_path.empty()) {
        return Status::OK();
    }
    RETURN_IF_NOT_OK(AtomicWriteTextFile(FLAGS_health_check_path, ""));
    LOG(INFO) << "Worker probe published.";
    return Status::OK();
}

Status SetHealthy()
{
    g_health.store(true, std::memory_order_release);
    return Status::OK();
}

Status RemoveWorkerProbe()
{
    if (FLAGS_health_check_path.empty()) {
        return Status::OK();
    }
    if (unlink(FLAGS_health_check_path.c_str()) == 0 || errno == ENOENT) {
        return Status::OK();
    }
    const int error = errno;
    RETURN_STATUS(K_IO_ERROR, FormatString("Delete worker probe %s failed with errno: %d, errmsg: %s",
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
