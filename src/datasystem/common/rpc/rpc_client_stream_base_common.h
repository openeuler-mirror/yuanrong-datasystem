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
 * Description: RPC client streaming common code.
 * ClientWriter/ClientReader/ClientWriterReader methods are now defined
 * inline in rpc_stub.h using direct brpc impl dispatch.
 * This header is kept for backward include compatibility.
 */
#ifndef DATASYSTEM_COMMON_RPC_RPC_CLIENT_STREAM_BASE_COMMON_H
#define DATASYSTEM_COMMON_RPC_RPC_CLIENT_STREAM_BASE_COMMON_H

#include "datasystem/common/rpc/rpc_stub.h"

namespace datasystem {
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_RPC_CLIENT_STREAM_BASE_COMMON_H
