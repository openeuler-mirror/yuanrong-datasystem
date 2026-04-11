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
 * Description: Test absl failure handler.
 */
#include "datasystem/common/log/failure_handler.h"

#include <chrono>
#include <string>

#include "ut/common.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/util/file_util.h"

DS_DECLARE_string(log_dir);

namespace datasystem {
namespace ut {
class FailureHandlerTest : public CommonTest {};

TEST_F(FailureHandlerTest, FailureWriter)
{
    FailureWriter("xxx\nxxx\nxxx\nxxx\nxxx\nxxx\nxxx\nxxx\nxxx\nxxx\nxxx\nxxx\nxxx\nxxx\nxxx\nxxx\nxxx\nxxx\n");

    struct stat buffer;
    std::string backtraceFilename = FLAGS_log_dir + "/container.log";
    EXPECT_TRUE(stat(backtraceFilename.c_str(), &buffer) == 0);
    DS_EXPECT_OK(DeleteFile(backtraceFilename));
}

using FailureHandlerDeathTest = FailureHandlerTest;

TEST_F(FailureHandlerDeathTest, FailureSignalHandler)
{
    InstallFailureSignalHandler("datasystem");

    EXPECT_DEATH({
        int* ptr = nullptr;
        *ptr = 42;
    }, ".*SIGSEGV.*");

    auto interval = std::chrono::milliseconds(1000);
    std::this_thread::sleep_for(interval);
    
    struct stat buffer;
    std::string backtraceFilename = FLAGS_log_dir + "/container.log";
    EXPECT_TRUE(stat(backtraceFilename.c_str(), &buffer) == 0);
}

}  // namespace ut
}  // namespace datasystem
