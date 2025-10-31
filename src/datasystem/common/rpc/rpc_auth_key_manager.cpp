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
 * Description: Helper functions to load curve public/private key pair.
 */
#include "datasystem/common/rpc/rpc_auth_key_manager.h"

#include <nlohmann/json.hpp>
#include <securec.h>

DS_DEFINE_bool(enable_curve_zmq, false,
            "Whether to enable Curve ZMQ for authentication and authorization between components.");

namespace datasystem {
static Status CreateCredentialsHelper(const RpcAuthKeys &keys, const std::string &serverName, RpcCredential &cred)
{
    const char *serverPublicKey = nullptr;
    RETURN_IF_NOT_OK(keys.GetServerKey(serverName, serverPublicKey));
    CHECK_FAIL_RETURN_STATUS(keys.GetClientPublicKey() != nullptr, K_RUNTIME_ERROR,
                             "Client public key should not be null");
    CHECK_FAIL_RETURN_STATUS(keys.GetClientPrivateKey() != nullptr, K_RUNTIME_ERROR,
                             "Client private key should not be null");
    CHECK_FAIL_RETURN_STATUS(serverPublicKey != nullptr, K_RUNTIME_ERROR, "Server key should not be null");
    cred.SetAuthCurveClient(keys.GetClientPublicKey(), keys.GetClientPrivateKey(), serverPublicKey);
    return Status::OK();
}

Status RpcAuthKeyManager::CreateCredentials(const std::string &serverName, RpcCredential &cred)
{
    if (!FLAGS_enable_curve_zmq) {
        return Status::OK();
    }
    const RpcAuthKeys &authKeys = RpcAuthKeyManager::Instance().GetKeys();
    RETURN_IF_NOT_OK(CreateCredentialsHelper(authKeys, serverName, cred));
    return Status::OK();
}

Status RpcAuthKeyManager::CreateClientCredentials(const RpcAuthKeys &authKeys, const std::string &serverName,
                                                  RpcCredential &cred)
{
    if (authKeys.GetClientPublicKey() && authKeys.GetClientPrivateKey()) {
        VLOG(RPC_LOG_LEVEL) << "ZMQ CURVE authentication identity is provided";
        RETURN_IF_NOT_OK(CreateCredentialsHelper(authKeys, serverName, cred));
    }
    return Status::OK();
}

Status RpcAuthKeyManager::CopyCurveAuthKey(const char *src, std::unique_ptr<char[]> &dest)
{
    CHECK_FAIL_RETURN_STATUS(src != nullptr, K_INVALID, "Source pointer should not be null");
    size_t len = std::strlen(src) + 1;
    try {
        dest = std::make_unique<char[]>(len);
    } catch (const std::bad_alloc &e) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, e.what());
    }
    int ret = strcpy_s(dest.get(), len, src);
    CHECK_FAIL_RETURN_STATUS(ret == EOK, StatusCode::K_RUNTIME_ERROR,
                             FormatString("Copy key failed, the strcpy_s return: %d", ret));
    return Status::OK();
}
}  // namespace datasystem
