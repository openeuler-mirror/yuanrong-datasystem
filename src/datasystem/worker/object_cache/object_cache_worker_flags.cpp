/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
 * Description: Worker object-cache flag definitions shared by object-cache components.
 */

#include "datasystem/common/flags/flags.h"

DS_DEFINE_int32(oc_worker_worker_parallel_nums, 16, "worker worker batch rsp control nums, default 0 means unlimited");
DS_DEFINE_int32(oc_worker_worker_parallel_min, 100,
                "Min data count for parallel worker worker batch rsp, default is 100");
DS_DEFINE_uint64(oc_worker_aggregate_single_max, 65536,
                 "Max single item size for batching worker worker batch rsp, default is 64KB");
DS_DEFINE_uint64(oc_worker_aggregate_merge_size, 2097152,
                 "Target batch size for worker worker responses, default is 2MB");
