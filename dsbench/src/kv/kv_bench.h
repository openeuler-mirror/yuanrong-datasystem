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

#ifndef KV_BENCH_H
#define KV_BENCH_H

#include <vector>

#include "bench_base.h"
#include "datasystem/common/util/wait_post.h"
#include "datasystem/kv_client.h"
#include "datasystem/utils/status.h"
#include "kv_args.h"

namespace datasystem {

namespace bench {
class KVBench final : public BenchBase {
public:
    KVBench(KVArgs &args) : BenchBase(args), args_(args)
    {
    }
    virtual ~KVBench() = default;

protected:
    Status WarmUp() override;
    Status Prepare() override;
    Status Run(uint64_t threadIndex, Barrier &barrier) override;
    Status PrintBenchmarkInfo() override;

    std::string GetBenchCost();
    Status FetchOwnerId(const std::string &ownerWorkerAddr, const std::string &accessKey, const std::string &secretKey,
                        std::string &ownerId);

    Status Set(std::unique_ptr<KVClient> &client, uint64_t threadIndex);
    Status Get(std::unique_ptr<KVClient> &client, uint64_t threadIndex);
    Status Del(std::unique_ptr<KVClient> &client, uint64_t threadIndex);

private:
    void GenerateSetKeys(std::vector<std::string> &keys);
    void GenerateGetOrDelKeys(std::vector<std::string> &keys);
    std::vector<std::vector<std::string>> perThreadKeys_;
    KVArgs &args_;
};
}  // namespace bench
}  // namespace datasystem
#endif
