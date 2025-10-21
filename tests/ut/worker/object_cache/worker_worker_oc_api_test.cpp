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
 * Description: Test WorkerWorkerOcApi
 */

#include "datasystem/worker/object_cache/worker_worker_oc_api.h"
#include <gtest/gtest.h>

#include "common.h"
#include "datasystem/utils/status.h"

using namespace datasystem::object_cache;

namespace datasystem {
namespace ut {
class WorkerWorkerOcApiTest : public CommonTest {
};

TEST_F(WorkerWorkerOcApiTest, TestExecRemoteGetLocally)
{
    auto localApi = std::make_unique<object_cache::WorkerLocalWorkerOCApi>(nullptr, nullptr);
    ASSERT_EQ(localApi->GetObjectRemote(nullptr).GetCode(), K_RUNTIME_ERROR);
}
}  // namespace ut
}  // namespace datasystem