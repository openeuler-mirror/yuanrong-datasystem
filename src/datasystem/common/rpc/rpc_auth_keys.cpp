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
 * Description: Struct for curve authentication key settings.
 */
#include "datasystem/common/rpc/rpc_auth_keys.h"

#include <securec.h>

#include "datasystem/common/log/log.h"
#include "datasystem/common/rpc/rpc_auth_key_manager.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/utils/sensitive_value.h"

namespace datasystem {
Status RpcAuthKeys::SetRpcAuthKeys(const std::string &clientPublicKey, const std::string &clientPrivateKey,
                                   const std::unordered_map<std::string, std::string> &rpcServerKeys)
{
    RETURN_IF_NOT_OK(SetClientPublicKey(clientPublicKey));
    RETURN_IF_NOT_OK(SetClientPrivateKey(clientPrivateKey));
    for (auto &kv : rpcServerKeys) {
        RETURN_IF_NOT_OK(SetServerKey(kv.first, kv.second));
    }
    return Status::OK();
}

static Status CopyKeyHelper(const std::string &src, std::unique_ptr<char[]> &dest)
{
    RETURN_OK_IF_TRUE(src.empty());
    RETURN_IF_NOT_OK(RpcAuthKeyManager::CopyCurveAuthKey(src.c_str(), dest));
    return Status::OK();
}

const char *RpcAuthKeys::GetClientPublicKey() const
{
    return clientPublicKey_.get();
}

Status RpcAuthKeys::SetClientPublicKey(std::unique_ptr<char[]> &clientPublicKey)
{
    clientPublicKey_ = std::move(clientPublicKey);
    return Status::OK();
}

Status RpcAuthKeys::SetClientPublicKey(const std::string &clientPublicKey)
{
    return CopyKeyHelper(clientPublicKey, clientPublicKey_);
}

const char *RpcAuthKeys::GetClientPrivateKey() const
{
    return clientPrivateKey_.get();
}

Status RpcAuthKeys::SetClientPrivateKey(std::unique_ptr<char[]> &clientPrivateKey)
{
    clientPrivateKey_ = std::move(clientPrivateKey);
    return Status::OK();
}

Status RpcAuthKeys::SetClientPrivateKey(SensitiveValue clientPrivateKey)
{
    RETURN_OK_IF_TRUE(clientPrivateKey.Empty());
    size_t outSize;
    CHECK_FAIL_RETURN_STATUS(clientPrivateKey.MoveTo(clientPrivateKey_, outSize), K_INVALID, "value is empty.");
    return Status::OK();
}

static Status CheckServerValidity(const std::string &serverName)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(SERVER_TYPES.find(serverName) != SERVER_TYPES.end(), K_RUNTIME_ERROR,
                                         "Invalid server component name");
    return Status::OK();
}

Status RpcAuthKeys::GetServerKey(const std::string &serverName, const char *&keyContent) const
{
    RETURN_IF_NOT_OK(CheckServerValidity(serverName));
    auto keyIter = serverPublicKeys_.find(serverName);
    CHECK_FAIL_RETURN_STATUS(keyIter != serverPublicKeys_.end(), K_RUNTIME_ERROR, "Key not exist");
    keyContent = keyIter->second.get();
    return Status::OK();
}

Status RpcAuthKeys::SetServerKey(const std::string &serverName, std::unique_ptr<char[]> &keyContent)
{
    RETURN_IF_NOT_OK(CheckServerValidity(serverName));
    serverPublicKeys_[serverName] = std::move(keyContent);
    return Status::OK();
}

Status RpcAuthKeys::SetServerKey(const std::string &serverName, const std::string &keyContent)
{
    RETURN_IF_NOT_OK(CheckServerValidity(serverName));
    std::unique_ptr<char[]> key;
    RETURN_IF_NOT_OK(CopyKeyHelper(keyContent, key));
    serverPublicKeys_[serverName] = std::move(key);
    return Status::OK();
}
}  // namespace datasystem