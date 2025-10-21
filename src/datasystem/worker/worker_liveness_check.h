/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: The interface of worker liveiness check.
 */
#ifndef DATASYSTEM_WORKER_WORKER_LIVENESS_CHECK_H
#define DATASYSTEM_WORKER_WORKER_LIVENESS_CHECK_H

#include <cstdint>
#include <functional>
#include <memory>
#include <thread>
#include <atomic>

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/protos/master_object.stub.rpc.pb.h"
#include "datasystem/protos/object_posix.stub.rpc.pb.h"
#include "datasystem/protos/share_memory.stub.rpc.pb.h"
#include "datasystem/protos/worker_object.stub.rpc.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace worker {
class WorkerOCServer;
class WorkerLivenessCheck {
public:
    WorkerLivenessCheck(WorkerOCServer *workerOcServer, std::string probeFileName, uint32_t probeTimeoutMs,
                        HostPort hostPort, std::string workerUuid, std::shared_ptr<AkSkManager> akSkManager);
    ~WorkerLivenessCheck();
    WorkerLivenessCheck(const WorkerLivenessCheck &) = delete;
    WorkerLivenessCheck &operator=(const WorkerLivenessCheck &) = delete;

    /**
     * @brief Init WorkerLivenessCheck instance.
     * @return Status of this call.
     */
    Status Init();

    /**
     * @brief Stop liveness check.
     */
    void Stop();

protected:
    /**
     * @brief Set liveness probe.
     * @param[in] success Mark as success liveness probe or not.
     * @return Status of this call
     */
    Status SetLivenessProbe(bool success);

    /**
     * @brief Reset liveness probe, if liveness probe exist, delete it
     * @return Status of this call
     */
    Status ResetLivenessProbe();

    /**
     * @brief Check liveness for all services.
     * @return Status of this call.
     */
    Status CheckWorkerServices();

    /**
     * @brief Check liveness for rocksdb service.
     * @return Status of this call.
     */
    Status CheckRocksDbService();

    /**
     * @brief Check current node is master or not.
     * @return true Current node is master.
     */
    bool IsMasterNode();

    /**
     * @brief Get liveness status.
     * @param[in/out] timer check timer.
     * @param[in] lastStatus last check status.
     * return Status of the call.
     */
    Status CheckLivenessProbeFile(Timer &timer, const Status &lastStatus);

private:
    void Run();
    Status DoLivenessCheck();

    std::unique_ptr<Thread> checkThread_;
    std::atomic<bool> exitFlag_{ false };
    WorkerOCServer *workerOcServer_;
    std::string probeFileName_;
    uint32_t probeTimeoutMs_;
    struct Policy {
        std::string name;
        std::function<Status()> func;
    };
    std::vector<Policy> policies_;
    HostPort hostPort_;
    std::time_t lastSuccessTimeUs_ = 0;
    const uint32_t TIME_UNIT_CONVERSION = 1000;

    // object if for send rpc.
    std::string livenessKey_;
    std::string workerUuid_;
    std::shared_ptr<AkSkManager> akSkManager_;
    std::vector<std::string> servicesNames_;
    std::shared_mutex lock_;
};
}  // namespace worker
}  // namespace datasystem

#endif
