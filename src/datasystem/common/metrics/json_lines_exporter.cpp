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
 * Description: JSON-Lines exporter implementation. Each line is one complete JSON object,
 *              no text prefix. Rotation/pruning reuse the base class machinery.
 */
#include "datasystem/common/metrics/json_lines_exporter.h"

#include <sstream>
#include <string>

#include <nlohmann/json.hpp>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/log_time.h"
#include "datasystem/common/log/pod_identifier.h"
#include "datasystem/common/util/file_util.h"

DS_DECLARE_string(cluster_name);

namespace datasystem {
namespace {
// Return a complete JSON string literal for a label value. Pod/cluster identifiers are usually
// IPs or hostnames, but use the JSON library so all control characters are escaped correctly.
std::string JsonStringLiteral(const std::string &value)
{
    return nlohmann::json(value).dump();
}
}  // namespace

Status JsonLinesExporter::Init(const std::string &filePath)
{
    RETURN_IF_NOT_OK(CreateFileByPath(filePath));
    podName_ = GetPodIdentifier();
    clusterName_ = FLAGS_cluster_name;
    MetricsExporter::Init();
    return Status::OK();
}

JsonLinesExporter::~JsonLinesExporter()
{
    Stop();
    if (fd_ != K_INVALID_FD) {
        RETRY_ON_EINTR(close(fd_));
        fd_ = K_INVALID_FD;
    }
}

void JsonLinesExporter::FlushThread()
{
    FlushBufferedMessages();
}

std::string WrapJsonWithPodCluster(const std::string &body, const std::string &podName,
                                   const std::string &clusterName, const std::string &time)
{
    if (body.empty() || body.front() != '{') {
        return body;
    }
    std::string timeStr = time;
    if (timeStr.empty()) {
        LogTime logTime;
        timeStr = FormatLogTimestamp(logTime.getTm(), logTime.getUsec());
    }
    std::ostringstream out;
    out << "{\"time\":" << JsonStringLiteral(timeStr) << ",\"pod_name\":" << JsonStringLiteral(podName) <<
        ",\"cluster_name\":" << JsonStringLiteral(clusterName) << ',' << body.substr(1);
    return out.str();
}
}  // namespace datasystem
