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

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <unistd.h>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "common.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/wait_post.h"
#include "datasystem/object_client.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/utils/status.h"
#include "oc_client_common.h"
#include "datasystem/common/metrics/res_metric_collector.h"

namespace datasystem {
namespace st {
namespace {
}  // namespace
class ClientSkipAuthTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        auto workerNum = 2;
        opts.numWorkers = workerNum;
        opts.numEtcd = 1;
        opts.workerGflagParams =
            FormatString("-skip_authenticate=true -shared_memory_size_mb=50 -v=2 -log_monitor=true");
    }

protected:
};

TEST_F(ClientSkipAuthTest, NoAuthPutGet)
{
    FLAGS_v = 1;
    std::shared_ptr<ObjectClient> cliLocal, clientRemote;
    InitTestClient(0, cliLocal);
    InitTestClient(1, clientRemote);
    std::string val = "is a test ";
    std::string objKey = "key";
    CreateParam param;
    DS_ASSERT_OK(cliLocal->Put(objKey, (uint8_t *)val.c_str(), val.size(), param));
    std::vector<Optional<Buffer>> buffers;
    DS_EXPECT_OK(clientRemote->Get({ objKey }, 0, buffers));
    ASSERT_EQ(buffers.size(), size_t(1));
    ASSERT_TRUE(buffers[0]);
    char buff[buffers[0]->GetSize() + 1];
    buff[buffers[0]->GetSize()] = '\0';
    memcpy_s(buff, buffers[0]->GetSize(), buffers[0]->ImmutableData(), buffers[0]->GetSize());
    std::string getVal(buff);
    EXPECT_EQ(getVal, val);
}

}  // namespace st
}  // namespace datasystem