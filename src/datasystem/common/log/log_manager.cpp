/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2025. All rights reserved.
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
 * Description: LogManager will run threads and periodically do log rolling, compress and flush_.
 */
#include "datasystem/common/log/log_manager.h"

#include <chrono>
#include <map>

#include <spdlog/details/file_helper.h>
#include <unistd.h>

#include "datasystem/common/constants.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/logging.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/log/spdlog/log_severity.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/utils/status.h"

DS_DECLARE_string(log_filename);
DS_DECLARE_uint32(max_log_file_num);
DS_DECLARE_bool(log_compress);
DS_DECLARE_uint32(log_retention_day);
DS_DECLARE_bool(log_monitor);
DS_DECLARE_string(log_dir);

namespace datasystem {

using namespace std::chrono;

const int PER_OPERATION_NUM = 3;

LogManager::~LogManager()
{
    (void)Stop();
}

Status LogManager::Start()
{
    LOG(INFO) << "Start Log Manager thread.";
    CHECK_FAIL_RETURN_STATUS(state_ == INITED, StatusCode::K_RUNTIME_ERROR, "State is not INITED");
    state_ = RUNNING;

    backgroundThread_ = Thread(&LogManager::RunTimerTask, this, DoLogBackgroundTask, logProcessInterval_);
    backgroundThread_.set_name("LogBackgroundTask");
    return Status::OK();
}

Status LogManager::Stop()
{
    {
        CHECK_FAIL_RETURN_STATUS(state_ == RUNNING, StatusCode::K_RUNTIME_ERROR, "State is not RUNNING");
        LOG(INFO) << "Stop Log Manager thread begin.";
        state_ = STOPPED;
    }

    backgroundThread_.join();
    LOG(INFO) << "Stop Log Manager thread complete.";
    return Status::OK();
}

void LogManager::RunTimerTask(const std::function<Status(void)> &func, int64_t waitSeconds)
{
    const int intervalMs = 10;
    const int secToMs = 1000;
    LOG(INFO) << "RunTimerTask thread start with duration:" << waitSeconds << "s";
    bool isCompressed = false;
    Timer timer;
    while (state_ == RUNNING) {
        if (!isCompressed) {
            std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs));
            if (static_cast<int64_t>(timer.ElapsedMilliSecond()) < waitSeconds * secToMs) {
                continue;
            }
        }

        Status status = func();
        isCompressed = status.GetCode() == K_TRY_AGAIN;
        if (status.IsError() && status.GetCode() != K_TRY_AGAIN) {
            LOG(WARNING) << "Do Log File Manager failed:" << status.ToString();
        }

        timer.Reset();
    }

    LOG(INFO) << "RunTimerTask thread ready to exit.";
}

Status LogManager::DoLogBackgroundTask()
{
    bool isCompressed = false;
    RETURN_IF_NOT_OK(DoLogFileCompress(isCompressed));
    RETURN_IF_NOT_OK(DoLogFileRolling());
    RETURN_IF_NOT_OK(DoLogMonitorWrite());
    CHECK_FAIL_RETURN_STATUS(isCompressed == false, K_TRY_AGAIN,
                             "Execute success, execute the next round of loop immediately.");
    return Status::OK();
}

Status LogManager::DoLogMonitorWrite()
{
    if (!FLAGS_log_monitor) {
        return Status::OK();
    }

    auto instance = Logging::AccessRecorderManagerInstance();
    return instance->SubmitWriteMessage();
}

Status LogManager::FetchLogWithPattern(std::vector<std::string> &files, bool isRolling)
{
    std::string pattern;
    std::string suffix = "";
    if (FLAGS_log_compress && isRolling) {
        suffix = "\\.gz";
    }

    std::stringstream accessRecorderFile;
    accessRecorderFile << FLAGS_log_dir.c_str() << "/" << ACCESS_LOG_NAME << "\\." << "*[0-9]\\.log" << suffix;
    pattern = accessRecorderFile.str();
    RETURN_IF_NOT_OK(Glob(pattern, files));

    std::stringstream requestOutFile;
    requestOutFile << FLAGS_log_dir.c_str() << "/" << REQUEST_OUT_LOG_NAME << "\\." << "*[0-9]\\.log" << suffix;
    pattern = requestOutFile.str();
    RETURN_IF_NOT_OK(Glob(pattern, files));

    std::stringstream dsClientAccessFile;
    dsClientAccessFile << FLAGS_log_dir.c_str() << "/" << CLIENT_ACCESS_LOG_NAME << "_[0-9]*" << "\\." << "*[0-9]\\.log"
                       << suffix;
    pattern = dsClientAccessFile.str();
    RETURN_IF_NOT_OK(Glob(pattern, files));

    std::stringstream resourceFile;
    resourceFile << FLAGS_log_dir.c_str() << "/" << RESOURCE_LOG_NAME << "\\." << "*[0-9]\\.log" << suffix;
    pattern = resourceFile.str();
    RETURN_IF_NOT_OK(Glob(pattern, files));

    return Status::OK();
}

Status LogManager::DoLogFileRolling()
{
    if (!FLAGS_log_compress) {
        return Status::OK();
    }

    for (int i = 0; i < NUM_SEVERITIES; ++i) {
        // 1st: get log files based on regular expressions.
        std::vector<std::string> files;
        // log gzip filename format: <program name>.<severity level>.<date>.<time>.log.gz
        std::stringstream ss;
        ss << FLAGS_log_dir.c_str() << "/" << FLAGS_log_filename.c_str() << "\\." << GetLogSeverityName(LogSeverity(i))
           << "\\." << "*[0-9]\\.log" << "\\.gz";
        std::string pattern = ss.str();
        RETURN_IF_NOT_OK(Glob(pattern, files));

        if (i == NUM_SEVERITIES - 1) {
            RETURN_IF_NOT_OK(FetchLogWithPattern(files, true));
        }

        // 2nd: calculate the total size of the log files and get their timestamp.
        std::map<int64_t, FileUnit> fileMap;
        for (auto &file : files) {
            auto size = FileSize(file);
            CHECK_FAIL_RETURN_STATUS(size >= 0, K_RUNTIME_ERROR, "Get file size failed");
            int64_t timestamp;
            RETURN_IF_NOT_OK(GetFileModifiedTime(file, timestamp));
            fileMap.emplace(timestamp, FileUnit(file, size));
        }

        if ((FLAGS_max_log_file_num == 0 || fileMap.size() <= FLAGS_max_log_file_num) && FLAGS_log_retention_day == 0) {
            continue;
        }

        // 3rd: delete the oldest files.
        size_t redundantNum = (FLAGS_max_log_file_num == 0 || fileMap.size() <= FLAGS_max_log_file_num)
                                  ? 0
                                  : fileMap.size() - FLAGS_max_log_file_num;
        for (const auto &file : fileMap) {
            auto curTime = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
            bool needDelByTime = curTime - file.first > FLAGS_log_retention_day * 24 * 60 * 60 * 1000;
            bool needDelByNum = redundantNum > 0;
            if (!needDelByTime && !needDelByNum) {
                break;
            }

            RETURN_IF_NOT_OK(DeleteFile(file.second.fileName_));
            redundantNum--;
        }
    }

    return Status::OK();
}

Status LogManager::DoLogFileCompress(bool &isCompressed)
{
    if (!FLAGS_log_compress) {
        return Status::OK();
    }

    for (int i = 0; i < NUM_SEVERITIES; ++i) {
        // 1st: get log files based on regular expressions.
        std::vector<std::string> files;
        // log filename format: <program name>.<severity level>.<date>.<time>.log
        std::stringstream ss;
        ss << FLAGS_log_dir.c_str() << "/" << FLAGS_log_filename.c_str() << "\\." << GetLogSeverityName(LogSeverity(i))
           << "\\." << "*[0-9]\\.log";
        std::string pattern = ss.str();
        RETURN_IF_NOT_OK(Glob(pattern, files));

        if (i == NUM_SEVERITIES - 1) {
            RETURN_IF_NOT_OK(FetchLogWithPattern(files));
        }

        if (!files.empty()) {
            // Avoid filebeat lost log. Since the filebeat polling cycle is 1s, the interval here is 1s.
            auto interval = std::chrono::milliseconds(1000);
            std::this_thread::sleep_for(interval);
        }

        // 2nd: compress these file in '.gz' format
        int num = 0;
        for (const auto &file : files) {
            std::string targetFile = file;
            if (i < NUM_SEVERITIES - 1) {
                // e.g: datasystem_worker.INFO.1.log -> datasystem_worker.INFO.{TIME}.log
                int64_t timestamp;
                GetFileModifiedTime(file, timestamp);

                std::string basename, ext, idx;
                std::tie(basename, ext) = spdlog::details::file_helper::split_by_extension(file);
                std::tie(basename, idx) = spdlog::details::file_helper::split_by_extension(basename);

                targetFile = basename + "." + FormatTimestampToString(timestamp) + ext;
                RETURN_IF_NOT_OK(RenameFile(file, targetFile));
            }

            // e.g: datasystem_worker.INFO.{TIME}.log -> datasystem_worker.INFO.{TIME}.log.gz
            std::string gzFile = targetFile + ".gz";
            if (FileExist(gzFile)) {
                continue;
            }

            // Compress the file and delete the origin file, we just need the compress files!
            isCompressed = true;
            RETURN_IF_NOT_OK(CompressFile(targetFile, gzFile));
            RETURN_IF_NOT_OK(DeleteFile(targetFile));
            if (++num == PER_OPERATION_NUM) {
                break;
            }
        }
    }

    return Status::OK();
}

}  // namespace datasystem
