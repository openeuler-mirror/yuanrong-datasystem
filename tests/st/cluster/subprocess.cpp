/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: Subprocess, it's api just like thread.
 */
#include "cluster/subprocess.h"

#include <chrono>
#include <cstdlib>
#include <sstream>
#include <sched.h>
#include <sys/prctl.h>
#include <sys/wait.h>
#include <thread>

#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/log/log.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace st {
Subprocess::Subprocess(std::string cmd)
    : state_(ProcessState::K_INITIALIZE), cmd_(std::move(cmd)), pid_(-1), exitCode_(0), injectAbort_(false)
{
}

Status Subprocess::ParseCommand()
{
    if (cmd_.empty()) {
        RETURN_STATUS(StatusCode::K_INVALID, "exec command is empty.");
    }
    std::istringstream iss(cmd_);
    std::string item;
    while (std::getline(iss, item, ' ')) {
        if (item.empty()) {
            continue;
        }
        args_.emplace_back(item);
    }
    if (args_.empty()) {
        RETURN_STATUS(StatusCode::K_INVALID, "exec command is not executable: " + cmd_);
    }

    exe_ = args_.front();
    return Status::OK();
}

Status Subprocess::Start()
{
    if (state_ != ProcessState::K_INITIALIZE && state_ != ProcessState::K_TERMINATE) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Invalid state: " + std::to_string(state_));
    }
    RETURN_IF_NOT_OK(ParseCommand());

    pid_ = fork();
    if (pid_ == -1) {
        int err = errno;
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Unable to fork: " + StrErr(err));
    } else if (pid_ == 0) {
        // We are the child, let's call the 'execvp' to execute the command. The process must run in
        // foreground mode to prevent exit. Otherwise we will loss their states and cannot manage it.
        // prctl(PR_SET_PDEATHSIG) can prevent orphans when parent is killed.
        prctl(PR_SET_PDEATHSIG, SIGKILL);

        std::vector<char *> argv;
        for (const auto &arg : args_) {
            argv.emplace_back(const_cast<char *>(arg.c_str()));
        }
        argv.emplace_back(nullptr);

        // Set the environment variables for subprocess, if it has
        // been exist in current context, overwrite it directly.
        for (const auto &pair : env_) {
            setenv(pair.first.c_str(), pair.second.c_str(), 1);
        }
        execvp(exe_.c_str(), &argv[0]);
        int err = errno;
        exit(err);
    } else {
        state_ = ProcessState::K_RUNNING;
    }
    return Status::OK();
}

Status Subprocess::SetRunningMode()
{
    state_ = ProcessState::K_RUNNING;
    return Status::OK();
}

Status Subprocess::Shutdown()
{
    if (state_ != ProcessState::K_RUNNING) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Invalid state: " + std::to_string(state_));
    }
    Status s = KillAndWait(SIGTERM);
    Join();
    state_ = ProcessState::K_TERMINATE;
    DeleteHealthCheckFile();
    return s;
}

Status Subprocess::ShutdownByKill()
{
    if (state_ != ProcessState::K_RUNNING) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Invalid state: " + std::to_string(state_));
    }
    Status s = Kill(SIGKILL);
    Join();
    state_ = ProcessState::K_TERMINATE;
    DeleteHealthCheckFile();
    return s;
}

void Subprocess::Join() const
{
    int status;
    waitpid(pid_, &status, 0);
}

void Subprocess::SetEnv(std::vector<std::pair<std::string, std::string>> env)
{
    env_ = std::move(env);
}

Status Subprocess::Kill(int signum) const
{
    pid_t pid = Pid();
    if (pid == -1) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "subprocess inactive.");
    }
    int ret = kill(pid, signum);
    if (ret != 0) {
        int err = errno;
        std::stringstream ss;
        ss << "subprocess: kill -" << signum << " " << pid << " failed, errno: " << err;
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, ss.str());
    }
    return Status::OK();
}

Status Subprocess::KillAndWait(int signum, int timeoutSecs)
{
    RETURN_IF_NOT_OK(Kill(signum));
    if (signum == SIGKILL) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Failed to wait for SIGKILL!");
    }
    Timer watchdog;
    const int SLEEP_TIME_MS = 10;  // sleep time interval is 10ms.
    do {
        if (!IsProcessAlive()) {
            if (!IsProcessExitNormally()) {
                RETURN_STATUS(K_RUNTIME_ERROR,
                              FormatString("process: %d, exit abnormally with signal %d.", pid_, WTERMSIG(exitCode_)));
            }
            double elapsedTime = watchdog.ElapsedSecond();
            if (elapsedTime - timeoutSecs > 0) {
                VLOG(1) << FormatString("process: %d, terminated spent %fs, exceed expect time: %ds.", pid_,
                                        elapsedTime, timeoutSecs);
            }
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
    } while (true);
    return Status::OK();
}

bool Subprocess::IsProcessAlive()
{
    int status;
    auto ret = waitpid(pid_, &status, WNOHANG);

    bool alive;
    if (ret == -1) {
        alive = false;
    } else if (ret == 0) {
        alive = true;
    } else {
        if (!WIFEXITED(status)) {
            exitCode_ = status;
        }
        alive = false;
    }

    if (!alive) {
        state_ = ProcessState::K_TERMINATE;
    }
    return alive;
}

bool Subprocess::IsProcessExitNormally()
{
    if (exitCode_ == 0) {
        return true;
    }
    // inject point may send SIGABORT while abort(), should check manually
    auto signal = WTERMSIG(exitCode_);
    if (signal == SIGTERM || (signal == SIGABRT && injectAbort_)) {
        return true;
    }
    return false;
}

pid_t Subprocess::Pid() const
{
    return pid_;
}

int Subprocess::ExitCode() const
{
    return exitCode_;
}

void Subprocess::AppendCmdParams(const std::string &params)
{
    cmd_ += " " + params;
}

void Subprocess::DeleteHealthCheckFile()
{
    std::string healthPrefix = "check_path=";
    std::string::size_type startPos = cmd_.find(healthPrefix);
    if (startPos != std::string::npos) {
        startPos += healthPrefix.size();
        std::string::size_type endpos = cmd_.find(" ", startPos);
        std::string healthFile;
        if (endpos != std::string::npos) {
            healthFile = cmd_.substr(startPos, endpos - startPos);
        } else {
            healthFile = cmd_.substr(startPos);
        }
        std::remove(healthFile.c_str());
    }
}

void Subprocess::SetInjectAbort()
{
    injectAbort_ = true;
}

}  // namespace st
}  // namespace datasystem
