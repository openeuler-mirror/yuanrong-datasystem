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
 * Description: Worker flag definitions shared by worker-side components.
 */

#include "datasystem/common/util/gflag/flags.h"
#include "datasystem/common/util/validator.h"

DS_DEFINE_string(
    worker_address, "127.0.0.1:31501",
    "Address of ds worker to accepting connections and the value cannot be empty and must under this format "
    "<ip>:<port>");
DS_DEFINE_string(bind_address, "",
                 "Address of ds worker to bind socket and must under this format <ip>:<port>, the same with "
                 "worker_address if not provide");
DS_DEFINE_validator(worker_address, &Validator::ValidateHostPortStringWithLog);
DS_DEFINE_validator(bind_address, &Validator::ValidateHostPortStringWithLog);
