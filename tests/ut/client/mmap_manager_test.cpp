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
 * Description: Mmap table class test.
 */
#include "datasystem/client/mmap_manager.h"

#include "datasystem/common/util/status_helper.h"
#include "datasystem/client/client_worker_common_api.h"

#include "common.h"

using namespace datasystem::client;

namespace datasystem {
namespace ut {
class MmapManagerTest : public CommonTest {};

TEST_F(MmapManagerTest, TestCleanRef)
{
    // todo: The mmapManager and mmapTable should be combined.
    // This testcase is just for increased coverage.
    HostPort hostPort("127.0.0.1", 8080);
    auto clientApi = std::make_shared<ClientWorkerRemoteCommonApi>(hostPort);
    MmapManager mmapManager(clientApi);

    mmapManager.CleanInvalidMmapTable();
    mmapManager.GetMmapEntryByFd(-1);
}
}  // namespace ut
}  // namespace datasystem