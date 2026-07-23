/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Coordination-backed object metadata reader.
 */
#include "datasystem/worker/object_cache/object_metadata_coordination_reader.h"

#include <memory>

#include "datasystem/cluster/coordination_backend/coordination_backend.h"
#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/object_posix.pb.h"

namespace datasystem {
namespace object_cache {
namespace {
constexpr int DEBUG_LOG_LEVEL = 2;

std::string BuildMetadataTableName()
{
    return std::string(ETCD_META_TABLE_PREFIX) + ETCD_HASH_SUFFIX;
}

std::string BuildMetadataKey(const std::string &objectKey)
{
    return FormatString("%010u/%s", MurmurHash3_32(objectKey), objectKey);
}
}  // namespace

ObjectMetadataCoordinationReader::ObjectMetadataCoordinationReader(cluster::ICoordinationBackend *backend)
    : backend_(backend)
{
}

Status ObjectMetadataCoordinationReader::QueryObjectMetadata(const std::string &objectKey, int32_t timeoutMs,
                                                             master::QueryMetaInfoPb &queryMeta)
{
    CHECK_FAIL_RETURN_STATUS(backend_ != nullptr, K_NOT_READY, "coordination backend is not initialized");
    CHECK_FAIL_RETURN_STATUS(!backend_->IsKeepAliveTimeout(), K_RPC_UNAVAILABLE, "coordination backend is unavailable");

    RangeSearchResult res;
    RETURN_IF_NOT_OK(backend_->Get(BuildMetadataTableName(), BuildMetadataKey(objectKey), res, timeoutMs));

    auto metaPb = std::make_unique<ObjectMetaPb>();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(metaPb->ParseFromString(res.value), StatusCode::K_RUNTIME_ERROR,
                                         FormatString("Parse string to ObjectMetaPb failed. String is: %s", res.value));
    VLOG(DEBUG_LOG_LEVEL) << "Success to get ObjectKey " << objectKey << ", metadata primary addr "
                          << metaPb->primary_address() << " from coordination store";
    queryMeta.set_address(metaPb->primary_address());
    queryMeta.set_allocated_meta(metaPb.release());
    return Status::OK();
}

}  // namespace object_cache
}  // namespace datasystem
