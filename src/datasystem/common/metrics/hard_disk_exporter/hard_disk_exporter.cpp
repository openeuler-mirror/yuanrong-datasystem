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

#include <fcntl.h>
#include <iterator>
#include <map>

#include "datasystem/common/log/pod_identifier.h"
#include "datasystem/common/log/log_time.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/uri.h"

DS_DECLARE_string(cluster_name);
DS_DECLARE_string(log_dir);
DS_DECLARE_int32(logfile_mode);
DS_DECLARE_uint32(log_size);
DS_DECLARE_uint32(max_log_size);
DS_DECLARE_uint32(max_log_file_num);

namespace datasystem {

void HardDiskExporter::CollectRotatedLogFiles(const std::string &filePath, std::vector<std::string> &files)
{
    std::string dir;
    std::string basename;
    size_t pos = filePath.find_last_of('/');
    if (pos == std::string::npos) {
        LOG(WARNING) << "Invalid log file path: " << filePath;
        return;
    }
    dir = filePath.substr(0, pos);
    basename = filePath.substr(pos + 1);

    std::string prefix;
    size_t idx = basename.find(".log");
    if (idx == std::string::npos) {
        LOG(WARNING) << "Invalid log file path: " << filePath;
        return;
    }
    prefix = basename.substr(0, idx + 1);  // include dot

    std::string pattern = dir + "/" + prefix + "*[0-9]\\.log";
    LOG_IF_ERROR(Glob(pattern, files), "Collect rotated log files failed");
}

void HardDiskExporter::PruneOldLogFiles(const std::vector<std::string> &files)
{
    if (FLAGS_max_log_file_num == 0 || files.size() <= FLAGS_max_log_file_num) {
        return;
    }

    std::map<uint64_t, std::string> fileMap;
    for (const auto &file : files) {
        uint64_t timestamp = 0;
        if (!GetFileLastModified(file, &timestamp).IsOk()) {
            continue;
        }
        while (fileMap.find(timestamp) != fileMap.end()) {
            ++timestamp;
        }
        fileMap.emplace(timestamp, file);
    }

    if (fileMap.size() <= FLAGS_max_log_file_num) {
        return;
    }

    size_t redundant = fileMap.size() - FLAGS_max_log_file_num;
    for (auto it = fileMap.begin(); it != fileMap.end() && redundant > 0; it++, redundant--) {
        LOG_IF_ERROR(DeleteFile(it->second), "Delete old log file failed");
    }
}

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

Status HardDiskExporter::CreateFileByPath(const std::string &filePath)
{
    if (!FileExist(FLAGS_log_dir, W_OK | R_OK | X_OK)) {
        const int permission = 0700;  // Minimum permission for log dir.
        RETURN_IF_NOT_OK(CreateDir(FLAGS_log_dir, true, permission));
    }
    filePath_ = filePath;
    // if the FLAGS_enable_aiops_log is true, the group members can read log files as well.
    const mode_t permission = FLAGS_logfile_mode;
    return OpenFile(filePath, O_CREAT | O_APPEND | O_RDWR, permission, &fd_);
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
    std::unique_lock<std::mutex> l(mtx_);

    // wait active buffer write to pool
    notEmpty_.wait(l, [&] { return !bufferPool_.empty() || exitFlag_; });

    std::unique_ptr<std::vector<std::string>> flushBuffer{ nullptr };
    GetFlushBufferFromQueue(flushBuffer);
    if (flushBuffer == nullptr) {
        return;
    }
    off_t endPos = lseek(fd_, 0, SEEK_END);
    std::stringstream ss;
    std::copy(flushBuffer->begin(), flushBuffer->end(), std::ostream_iterator<std::string>(ss, "\n"));
    std::string message = ss.str();
    // do not support log message in sink
    (void)WriteFileNoErrorLog(fd_, message.c_str(), message.size(), endPos);
    if (fileSize_ > UINT64_MAX - message.size()) {
        fileSize_ = UINT64_MAX;
    } else {
        fileSize_ += message.size();
    }
    ChangeLogFile();
}

void HardDiskExporter::GetLogFilePath(uint64_t &timestamp, std::string &filePath)
{
    time_t date;
    auto stringTimestamp = std::to_string(timestamp);
    (void)StringToDateTime(stringTimestamp, date);
    (void)GetFormatDate(date, stringTimestamp);
    size_t index = filePath_.find(".log");
    if (index != std::string::npos) {
        auto prefixPath = filePath_.substr(0, index + 1);
        filePath = prefixPath + stringTimestamp + ".log";
    }
}

void HardDiskExporter::ChangeLogFile()
{
    // FLAGS_max_log_size describes data at the MB
    size_t maxSize = static_cast<size_t>(FLAGS_max_log_size) * 1024 * 1024;
    if (fileSize_ < maxSize) {
        return;
    }

    auto lastSize = fileSize_;
    fileSize_ = 0;
    RETRY_ON_EINTR(close(fd_));
    fd_ = K_INVALID_FD;

    uint64_t timestamp;
    (void)GetFileLastModified(filePath_, &timestamp);
    std::string newFile;
    GetLogFilePath(timestamp, newFile);

    // Avoid overwriting existing files.
    while (FileExist(newFile)) {
        timestamp += 1;
        GetLogFilePath(timestamp, newFile);
    }

    int ret = rename(filePath_.c_str(), newFile.c_str());
    if (ret != 0) {
        // Rename failed, old file still in use, retry in next time.
        fileSize_ = lastSize;
    }
    (void)CreateFileByPath(filePath_);

    if (ret == 0) {
        std::vector<std::string> files;
        CollectRotatedLogFiles(filePath_, files);
        PruneOldLogFiles(files);
    }
}
}  // namespace datasystem
