/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
#ifndef DATASYSTEM_TEST_TOPOLOGY_EXECUTOR_ACCESS_H
#define DATASYSTEM_TEST_TOPOLOGY_EXECUTOR_ACCESS_H

#include <unordered_map>
#include <unordered_set>

#include "datasystem/cluster/executor/topology_phase_callbacks.h"
#include "datasystem/cluster/repository/topology_repository.h"
#include "datasystem/common/util/thread_pool.h"

#define private public
#include "datasystem/cluster/executor/topology_task_executor.h"
#undef private

#endif
