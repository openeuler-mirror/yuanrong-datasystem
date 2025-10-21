
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
 * Description: eviction test.
 */
#include <fcntl.h>
#include <memory>
#include <vector>

#include "client/object_cache/oc_client_common.h"
#include "common.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/object_cache/lock.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/immutable_string/immutable_string.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/common/util/queue/queue.h"
#include "datasystem/common/object_cache/safe_table.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/master/object_cache/store/object_meta_store.h"
#include "datasystem/object_cache/buffer.h"
#include "datasystem/worker/cluster_manager/etcd_cluster_manager.h"
#include "datasystem/worker/object_cache/async_send_manager.h"
#include "datasystem/worker/object_cache/obj_cache_shm_unit.h"
#include "datasystem/worker/object_cache/worker_master_oc_api.h"
#include "datasystem/worker/object_cache/worker_oc_eviction_manager.h"
#include "datasystem/worker/object_cache/worker_oc_service_impl.h"
#include "datasystem/worker/object_cache/service/worker_oc_service_crud_common_api.h"
#include "securec.h"

using namespace datasystem::object_cache;
using namespace datasystem::worker;
using namespace datasystem::master;
namespace datasystem {
namespace st {
class EvictMemTest : public OCClientCommon {
public:
    std::vector<std::string> workerAddress_;

    void SetClusterSetupOptions(ExternalClusterOptions &opts)
    {
        opts.numWorkers = 1;
        opts.numEtcd = 1;
        std::string hostIp = "127.0.0.1";
        opts.enableSpill = true;
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        for (auto addr : opts.workerConfigs) {
            workerAddress_.emplace_back(addr.ToString());
        }
        opts.workerGflagParams =
            "-shared_memory_size_mb=560  -eviction_reserve_mem_threshold_mb=100 -v=1 "
            "--inject_actions=evictAction.setDelete:call()";
        opts.numOBS = 1;
    }
};

TEST_F(EvictMemTest, EvictThresHoldTest)
{
    std::shared_ptr<DsClient> client0;
    InitTestDsClient(0, client0);
    std::string key400 = "key_400", key10 = "key_10", key20 = "key_20";
    auto mb400 = 400, mb20 = 20, mb10 = 10;
    auto put = [&client0](uint64_t mb) {
        uint64_t dataSize =  mb*MB_TO_BYTES;
        // Put, spill will be triggered
        CreateParam param;
        param.writeMode = WriteMode::NONE_L2_CACHE_EVICT;
        std::string objectKey = FormatString("key_%lu", mb);
        std::string data(dataSize, 'x');
        std::shared_ptr<Buffer> buffer;
        DS_ASSERT_OK(client0->Object()->Create(objectKey, dataSize, param, buffer));
        DS_ASSERT_OK(buffer->MemoryCopy(data.data(), dataSize));
        DS_ASSERT_OK(buffer->Publish());
    };
    inject::Set("Exist.QueryLocalMem", "call()");
    auto exist = [&client0](std::string objectKey) -> bool {
        std::vector<bool> exists;
        auto res = client0->KV()->Exist({ objectKey }, exists);
        if (res.IsError()) {
            return false;
        }
        return exists[0];
    };
    // The real size of 400_MB keys is 448MB
    // mem reserve is min(560*0.2, 100) 100MB ;
    // case 1  (560-468) <100 ,evict ;
    put(mb400);
    put(mb20);
    DS_ASSERT_TRUE((exist(key20)), true);
    std::vector<std::string> failKeys;
    DS_ASSERT_TRUE((exist(key400)), false);
    DS_ASSERT_OK(client0->Hetero()->Delete({ key20 }, failKeys));
    DS_ASSERT_TRUE((failKeys.empty()), true);
    // case 2  (560-458) >100 , no evict
    put(mb400);
    put(mb10);
    DS_ASSERT_TRUE((exist(key400)), true);
}
}  // namespace st
}  // namespace datasystem