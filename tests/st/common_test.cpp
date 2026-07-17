// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "common_test.h"

#include <array>
#include <chrono>
#include <cstdio>
#include <thread>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/status_helper.h"

DS_DECLARE_bool(alsologtostderr);
DS_DECLARE_string(log_dir);

namespace datasystem {
namespace st {
namespace {
constexpr int kRemoveRetryDelayMs = 500;
constexpr size_t kCommandOutputBufferSize = 1024;
}  // namespace

Status ExecuteCmd(const std::string &cmd, std::string &result, int *exitCode)
{
    FILE *ptr = popen(cmd.c_str(), "r");
    CHECK_FAIL_RETURN_STATUS(ptr != nullptr, StatusCode::K_RUNTIME_ERROR, "Execute cmd:" + cmd + " error.");
    std::array<char, kCommandOutputBufferSize> buffer{};
    while (fgets(buffer.data(), buffer.size(), ptr) != nullptr) {
        result.append(buffer.data());
    }
    if (exitCode != nullptr) {
        *exitCode = pclose(ptr);
    } else {
        pclose(ptr);
    }
    return Status::OK();
}

Status ExecuteCmd(const std::string &cmd)
{
    int exitCode = 0;
    std::string result;
    Status status = ExecuteCmd(cmd + " 2>&1", result, &exitCode);
    if (status.IsOk() && exitCode != 0) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, result);
    }
    return status;
}

namespace {
void ClearTestCaseDir(const std::string &path)
{
    auto status = RemoveAll(path);
    if (status.IsError()) {
        const std::string cmd = "mount | grep " + path + " | awk '{print $3}' | xargs -I {} fusermount3 -u {}";
        status = ExecuteCmd(cmd);
        ASSERT_TRUE(status.IsOk()) << status.ToString();
        std::this_thread::sleep_for(std::chrono::milliseconds(kRemoveRetryDelayMs));
        status = RemoveAll(path);
        ASSERT_TRUE(status.IsOk()) << status.ToString();
    }
}
}  // namespace

CommonTest::CommonTest() : CommonTest("")
{
}

CommonTest::CommonTest(const std::string &pathSuffix)
{
    FLAGS_alsologtostderr = true;
    const auto *testInfo = testing::UnitTest::GetInstance()->current_test_info();
    const std::string caseName = testInfo == nullptr ? "unknown_test_suite" : testInfo->test_case_name();
    const std::string testName = testInfo == nullptr ? "unknown_test" : testInfo->name();
    testCasePath_ = std::string(LLT_BIN_PATH) + "/ds/" + caseName + "." + testName;
    if (!pathSuffix.empty()) {
        testCasePath_ += "." + pathSuffix;
    }
    FLAGS_log_dir = testCasePath_ + "/client";
    ClearTestCaseDir(testCasePath_);
    (void)CreateDir(FLAGS_log_dir, true);
}

void CommonTest::SetUp()
{
}
}  // namespace st
}  // namespace datasystem
