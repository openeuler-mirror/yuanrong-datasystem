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
 * Description: Service config.
 */
#include "datasystem/common/rpc/rpc_service_cfg.h"

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/rpc/rpc_constants.h"
#include "datasystem/common/rpc/unix_sock_fd.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/protos/rpc_option.pb.h"
#include "datasystem/common/log/log.h"

DS_DECLARE_string(unix_domain_socket_dir);

namespace datasystem {
RpcServiceCfg::RpcServiceCfg()
    : numRegularSockets_(RPC_NUM_BACKEND),
      numStreamSockets_(RPC_NUM_BACKEND),
      hwm_(RPC_HWM),
      udsEnabled_(false),
      udsDirectory_(FormatString("%s/%s", FLAGS_unix_domain_socket_dir, GetStringUuid().substr(0, RPC_EIGHT))),
      tcpDirect_("0")
{
    // Default to have at a socket with file name client.sock.
    sockList_.emplace_back(GetServiceSockName(ServiceSocketNames::DEFAULT_SOCK), RPC_SOCK_MODE);
}
}  // namespace datasystem
