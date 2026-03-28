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
 * Description: Defines the ascend device manager.
 */

#ifndef DATASYSTEM_COMMON_DEVICE_ACL_PIPELINE_P2P_TASK_H
#define DATASYSTEM_COMMON_DEVICE_ACL_PIPELINE_P2P_TASK_H

#include "datasystem/common/device/ascend/acl_pipeline_task.h"

#include <map>

#include "datasystem/common/device/device_pointer_wrapper.h"
#include "datasystem/common/device/ascend/acl_resource_manager.h"
#include "datasystem/hetero/device_common.h"
#include "datasystem/utils/status.h"

namespace datasystem {
class CommWrapperBase;
namespace acl {
struct P2PSendTask {
    std::vector<Blob> srcBuffers;
    size_t totalSize;
    std::shared_ptr<CommWrapperBase> comm;
    std::shared_ptr<DeviceRtEventWrapper> event;
    Blob transBuffer;
    uint64_t seq{ 0 };
};

using PipeLineP2PResource = TwoPhaseAclPipeLineResource;

class PipeLineP2PBase {
public:
    PipeLineP2PBase(AclResourceManager *aclResourceMgr) : aclResourceMgr_(aclResourceMgr)
    {
    }
    virtual ~PipeLineP2PBase();

    static void NotifyCallback(void *userData);

protected:
    Status AllocTransferBuffer(size_t objectSize, Blob &transBuffer, uint64_t &seq);

    AclResourceManager *aclResourceMgr_;

    struct CallbackData {
        PipeLineP2PBase *self;
        uint64_t ackSeq;
    };
    std::atomic<uint64_t> seq_{ 1 };
    std::atomic<uint64_t> ackSeq_{ 0 };

    std::mutex mutex_;
    std::map<uint64_t, std::vector<ShmUnit>> transferUnitPools_;
};

class PipeLineP2PSend : public TwoPhaseAclPipeLineBase<PipeLineP2PSend, P2PSendTask>, public PipeLineP2PBase {
public:
    PipeLineP2PSend(AclResourceManager *aclResourceMgr) : PipeLineP2PBase(aclResourceMgr)
    {
    }
    ~PipeLineP2PSend() = default;

    Status Submit(P2PSendTask &&task);

    Status WaitFinish();

protected:
    friend class TwoPhaseAclPipeLineBase<PipeLineP2PSend, P2PSendTask>;
    Status PreProcessImpl()
    {
        return Status::OK();
    }

    Status PostProcessImpl()
    {
        return Status::OK();
    }

    Status RunTaskPhaseOneImpl(size_t pipelineIndex, const P2PSendTask &task, aclrtStream stream);

    Status RunTaskPhaseTwoImpl(size_t pipelineIndex, const P2PSendTask &task, aclrtStream stream);

    Status PostTaskProcessImpl(const P2PSendTask &task);

private:
};

struct P2PRecvTask {
    std::vector<Blob> destBuffers;
    size_t totalSize;
    std::shared_ptr<CommWrapperBase> comm;
    std::shared_ptr<DeviceRtEventWrapper> event;
    Blob transBuffer;
    uint64_t seq{ 0 };
};

class PipeLineP2PRecv : public TwoPhaseAclPipeLineBase<PipeLineP2PRecv, P2PRecvTask>, public PipeLineP2PBase {
public:
    PipeLineP2PRecv(AclResourceManager *aclResourceMgr) : PipeLineP2PBase(aclResourceMgr)
    {
    }
    ~PipeLineP2PRecv() = default;

    Status Submit(P2PRecvTask &&task);
    Status WaitFinish();

protected:
    friend class TwoPhaseAclPipeLineBase<PipeLineP2PRecv, P2PRecvTask>;
    Status PreProcessImpl()
    {
        return Status::OK();
    }

    Status PostProcessImpl()
    {
        return Status::OK();
    }

    Status RunTaskPhaseOneImpl(size_t pipelineIndex, const P2PRecvTask &task, aclrtStream stream);

    Status RunTaskPhaseTwoImpl(size_t pipelineIndex, const P2PRecvTask &task, aclrtStream stream);

    Status PostTaskProcessImpl(const P2PRecvTask &task);

private:
};
}  // namespace acl
}  // namespace datasystem
#endif
