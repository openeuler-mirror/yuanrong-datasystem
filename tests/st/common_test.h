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

#ifndef DATASYSTEM_TEST_ST_COMMON_TEST_H
#define DATASYSTEM_TEST_ST_COMMON_TEST_H

#include <string>

#include <gtest/gtest.h>

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace st {
Status ExecuteCmd(const std::string &cmd, std::string &result, int *exitCode = nullptr);

Status ExecuteCmd(const std::string &cmd);

class CommonTest : public testing::Test {
public:
    CommonTest();

    ~CommonTest() override = default;

    void SetUp() override;

    void TearDown() override
    {
    }

protected:
    explicit CommonTest(const std::string &pathSuffix);

    std::string testCasePath_;
};
}  // namespace st
}  // namespace datasystem

#endif  // DATASYSTEM_TEST_ST_COMMON_TEST_H
