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
 * Description: JSON-Lines exporter, writes one complete JSON object per line without text prefix.
 *              Shares the base class double-buffer/flush/file-rotation machinery with HardDiskExporter.
 */

#ifndef DATASYSTEM_COMMON_METRICS_JSON_LINES_EXPORTER_H
#define DATASYSTEM_COMMON_METRICS_JSON_LINES_EXPORTER_H

#include <string>

#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/metrics/metrics_exporter.h"

namespace datasystem {
class JsonLinesExporter : public MetricsExporter {
public:
    /**
     * @brief  Initialize sink and start flush thread and create log file by absolute path.
     * @param[in] filePath Absolute path to the log file.
     * @return Status of the call.
     */
    Status Init(const std::string &filePath);

    ~JsonLinesExporter() override;

    /**
     * @brief Write a raw JSON line to the active buffer (no text prefix), then submit.
     * @param[in] json One complete JSON object, written as a single line.
     */
    void WriteJsonLine(const std::string &json)
    {
        WriteMessage(json);
        SubmitWriteMessage();
    }

    /**
     * @brief Write a raw JSON line without prefix and submit it to the flush queue.
     * @note uri/line are ignored; they only satisfy the base exporter interface shared with
     *       HardDiskExporter. The flush semantics are identical to WriteJsonLine.
     */
    void Send(const std::string &message, Uri &uri, int line) override
    {
        (void)uri;
        (void)line;
        WriteJsonLine(message);
    }

    /**
     * @brief Get the cached pod identifier (for JSON top-level pod_name field).
     * @return Const reference to the cached pod name.
     */
    [[nodiscard]] const std::string &PodName() const noexcept
    {
        return podName_;
    }

    /**
     * @brief Get the cached cluster name (for JSON top-level cluster_name field).
     * @return Const reference to the cached cluster name.
     */
    [[nodiscard]] const std::string &ClusterName() const noexcept
    {
        return clusterName_;
    }

private:
    /**
     * @brief Write buffered JSON lines to file and rotate when size exceeds the limit.
     */
    void FlushThread() override;

    std::string clusterName_;
};

/**
 * @brief Prepend time/pod_name/cluster_name as the first top-level fields of a JSON object body.
 *
 * time uses the same ISO8601 format as the standard log prefix (see FormatLogTimestamp). When time
 * is empty, the current wall-clock time is captured at call time. Both labels are JSON-escaped. The
 * body is not re-serialized: a plain string splice inserts the escaped fields right after the
 * leading '{'. Used by both kv_resource.log and kv_metrics.log so the two outputs share one
 * escaping + prefixing path (no shotgun edit when the label source changes).
 * @param[in] body A complete JSON object starting with '{'.
 * @param[in] podName Pod identifier for the top-level pod_name field.
 * @param[in] clusterName Cluster name for the top-level cluster_name field.
 * @param[in] time Optional ISO8601 timestamp; empty means capture now.
 * @return The wrapped JSON string, or the input unchanged if it is not a JSON object.
 */
[[nodiscard]] std::string WrapJsonWithPodCluster(const std::string &body, const std::string &podName,
                                                 const std::string &clusterName,
                                                 const std::string &time = "");
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_METRICS_JSON_LINES_EXPORTER_H
