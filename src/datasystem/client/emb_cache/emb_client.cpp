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
 * Description: Data system State Cache Client management.
 */

#include "datasystem/emb_client.h"

#include <climits>
#include <cstddef>
#include <cstdlib>
#include <string>
#include <utility>
#include <vector>

#include "datasystem/kv_client.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/kv/read_only_buffer.h"
#include "datasystem/utils/status.h"

#include "datasystem/client/emb_cache/emb_client_impl.h"

namespace datasystem {
InitParams::InitParams(const emb_cache::TableMetaInfo& info)
: tableKey(info.tableKey),
  tableIndex(info.tableIndex),
  tableName(info.tableName),
  dimSize(info.dimSize),
  tableCapacity(info.tableCapacity),
  bucketNum(info.bucketNum),
  hashFunction(info.hashFunction),
  bucketCapacity(info.bucketCapacity) {}

EmbClient::EmbClient(const ConnectOptions &connectOptions)
{
    kvClientImpl_ = std::make_shared<KVClient>(connectOptions);
    embClientImpl_ = std::make_unique<emb_cache::EmbClientImpl>(connectOptions);
}

EmbClient::~EmbClient()
{
    if (kvClientImpl_) {
        kvClientImpl_.reset();
    }
    if (embClientImpl_) {
        embClientImpl_.reset();
    }
}

Status EmbClient::ShutDown()
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    
    if (kvClientImpl_) {
        auto implRc = kvClientImpl_->ShutDown();
    }
    
    return Status::OK();
}

Status EmbClient::Init(const InitParams &params, const std::string &etcdserver, const std::string &localPath)
{
    // 初始化kvClientImpl_、embClientImpl_、tableIndex_
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    bool needRollbackState;
    
    auto rc = kvClientImpl_->Init();
    RETURN_IF_NOT_OK(rc);
    
    // 这里的Init本质上是根据params来创建一个tableMeta，这部分创建meta的逻辑写在了embClientImpl_->Init()，所以需要先创建一个embClientImpl_
    // embClientImpl_ = datasystem::emb_cache::EmbClientImpl::GetInstance();
    std::shared_ptr<EmbClient> self = shared_from_this(); //指向client本身
    // @todo:这里还要检查是否创建embClientImpl_成功
    static std::once_flag initFlag;
    embClientImpl_->Init(params, self, etcdserver, kvClientImpl_, localPath);

    tableKey_ = params.tableKey;

    return Status::OK();
}

Status EmbClient::Insert(const std::vector<uint64_t> &keys, const std::vector<StringView> &values)
{
    Status rc = embClientImpl_->Insert(keys, values);
    return rc;
}

Status EmbClient::Find(const std::vector<uint64_t> &keys, std::vector<StringView> &buffer)
{
    Status rc = embClientImpl_->Find(keys, buffer);
    return rc;
}

Status EmbClient::BuildIndex()
{
    Status rc = embClientImpl_->BuildIndex();
    return rc;
}

Status EmbClient::Load(const std::vector<std::string> embKeyFilesPath, const std::vector<std::string> embValueFilesPath, const std::string fileFormat)
{
    Status rc = embClientImpl_->Load(tableKey_, embKeyFilesPath, embValueFilesPath, fileFormat);
    return rc;
}

}  // namespace datasystem