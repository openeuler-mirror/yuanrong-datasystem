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
 * Description: Monitor logger, flush current node's res information and operation time cost
 * information, there is two Bounded-buffer to support async write log messages.
 */
#include "datasystem/common/metrics/hard_disk_exporter/hard_disk_exporter.h"

#include "datasystem/common/log/pod_identifier.h"
#include "datasystem/common/log/log_time.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/uri.h"

DS_DECLARE_string(cluster_name);

namespace datasystem {

Status HardDiskExporter::Init(const std::string &filePath)
{
    RETURN_IF_NOT_OK(CreateFileByPath(filePath));
    podName_ = GetPodIdentifier();
    MetricsExporter::Init();
    return Status::OK();
}

void HardDiskExporter::Send(const std::string &message, Uri &uri, int line)
{
    std::ostringstream constructStr;
    LogTime logTime;
    ConstructLogPrefix(constructStr, logTime.getTm(), logTime.getUsec(), uri.GetFileName().c_str(), line,
                       podName_.c_str(),
                       'I', FLAGS_cluster_name);
    constructStr << std::string(message);
    WriteMessage(constructStr.str());
}

HardDiskExporter::~HardDiskExporter()
{
    Stop();
    if (fd_ != K_INVALID_FD) {
        RETRY_ON_EINTR(close(fd_));
        fd_ = K_INVALID_FD;
    }
}

void HardDiskExporter::FlushThread()
{
    FlushBufferedMessages();
}
}  // namespace datasystem
