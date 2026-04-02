/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
 * Description: pipeline h2d interface implement for worker
 */

#ifndef OS_XPRT_PIPLN_WORKER_API
#define OS_XPRT_PIPLN_WORKER_API
#include "datasystem/common/os_transport_pipeline/os_transport_pipeline_common_api.h"
#include "datasystem/common/os_transport_pipeline/os_transport_pipeline_types.h"
#include "datasystem/common/shared_memory/shm_unit.h"
#include "datasystem/protos/worker_object.pb.h"

using namespace datasystem;
namespace OsXprtPipln {

DEFINE_HOOK(Status ParsePiplnH2DRequest(const GetReqPb &req, H2DChunkManager &mgr, const std::string &objectKey,
                                        int infoIdx));

DEFINE_HOOK(Status ConstructPipelineRH2DResponse(GetRspPb &resp, H2DChunkManager &mgr,
                                                 std::vector<std::string> rawObjectKeys));

DEFINE_HOOK(Status TriggerLocalPipelineRH2D(H2DChunkManager &mgr, const std::string &objectKey, void *pointer,
                                            size_t metaSize, size_t dataSize));

DEFINE_HOOK(Status TriggerRemotePipelineRH2D(H2DChunkManager &mgr, const std::string &key, uint64_t offset,
                                             uint64_t size, std::shared_ptr<ShmUnit> shmUnit,
                                             const std::string &remoteAddress, GetObjectRemoteReqPb &subReq));

DEFINE_HOOK(Status InitOsPiplnH2DEnv(void *ctx, void *jfc, void *jfce));

DEFINE_HOOK(Status StartPipelineSender(PiplnSndArgs &args));

DEFINE_HOOK_WITH_RETTYPE(bool, IsPiplnH2DRequest(const GetReqPb &req));

DEFINE_HOOK_WITH_RETTYPE(bool, IsPiplnH2DRequest(const BatchGetObjectRemoteReqPb &req));

DEFINE_HOOK_WITH_RETTYPE(bool, IsPiplnH2DRequest(const UrmaRemoteAddrPb &urmaInfo));

DEFINE_HOOK_WITH_RETTYPE(bool, IsPiplnH2DRequest(const H2DChunkManager &mgr));

DEFINE_HOOK_WITH_RETTYPE(int, PiplnH2DRecvEventHook(void *));

DEFINE_HOOK_WITH_RETTYPE(void, UnInitOsPiplnH2DEnv());

DEFINE_HOOK(Status MaybeTriggerLocalPipelineRH2D(H2DChunkManager &mgr, const std::string &key, uint64_t offset,
                                                 uint64_t size, std::shared_ptr<ShmUnit> shmUnit));

}  // namespace OsXprtPipln

#endif