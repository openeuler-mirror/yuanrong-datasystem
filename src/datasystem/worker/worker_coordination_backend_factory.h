/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Worker coordination backend factory.
 */
#ifndef DATASYSTEM_WORKER_WORKER_COORDINATION_BACKEND_FACTORY_H
#define DATASYSTEM_WORKER_WORKER_COORDINATION_BACKEND_FACTORY_H

#include <memory>
#include <string>

#include "datasystem/cluster/coordination_backend/coordination_backend.h"

namespace datasystem {
class EtcdStore;
class ICoordinatorServiceProxy;

namespace worker {

std::unique_ptr<cluster::ICoordinationBackend> CreateWorkerEtcdCoordinationBackend(EtcdStore *etcdStore);

std::unique_ptr<cluster::ICoordinationBackend> CreateWorkerDsCoordinationBackend(ICoordinatorServiceProxy *proxy,
                                                                                 std::string watcherAddr);

}  // namespace worker
}  // namespace datasystem

#endif  // DATASYSTEM_WORKER_WORKER_COORDINATION_BACKEND_FACTORY_H
