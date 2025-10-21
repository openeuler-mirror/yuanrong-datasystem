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
 * Description: Datasystem unit test base class for authentication related tests.
 */
#ifndef DATASYSTEM_TEST_ST_ZMQ_CURVE_TEST_COMMON_H
#define DATASYSTEM_TEST_ST_ZMQ_CURVE_TEST_COMMON_H

#include <string>

#include "datasystem/common/encrypt/secret_manager.h"
#include "datasystem/common/rpc/rpc_auth_key_manager.h"
#include "datasystem/common/util/file_util.h"

DS_DECLARE_string(encrypt_kit);

namespace datasystem {
namespace st {
class ZmqCurveTestCommon {
public:
    virtual const std::string &GetCurveKeyDir() = 0;

    // Prepare the keys for client by loading them from files
    Status ClientPreLoadKey(const std::string &zmqClientName, RpcAuthKeys &authKeys)
    {
        std::string clientFilePathPrefix = GetCurveKeyDir() + "/" + zmqClientName;
        std::string clientPublicKey;
        std::string clientPrivateKey;
        RETURN_IF_NOT_OK(LoadKey(clientFilePathPrefix + PUBLIC_KEY_EXT, clientPublicKey));
        RETURN_IF_NOT_OK(LoadKey(clientFilePathPrefix + PRIVATE_KEY_EXT, clientPrivateKey));
        RETURN_IF_NOT_OK(authKeys.SetClientPublicKey(clientPublicKey));
        RETURN_IF_NOT_OK(authKeys.SetClientPrivateKey(clientPrivateKey));
        for (const auto &server : SERVER_TYPES) {
            std::string serverPublicKey;
            std::string filePathPrefix = GetCurveKeyDir() + "/" + server;
            Status rc = LoadKey(filePathPrefix + PUBLIC_KEY_EXT, serverPublicKey);
            if (rc.IsOk()) {
                RETURN_IF_NOT_OK(authKeys.SetServerKey(server, serverPublicKey));
            }
        }
        return Status::OK();
    }

protected:
    Status LoadKey(const std::string &filePath, std::string &keyContent)
    {
        std::ifstream ifs(filePath);
        if (!ifs.is_open()) {
            RETURN_STATUS(StatusCode::K_IO_ERROR, "Cannot open " + filePath);
        }
        std::stringstream buffer;
        buffer << ifs.rdbuf();
        keyContent = buffer.str();
        ifs.close();
        std::unique_ptr<char[]> text;
        // Need to load the key component and generate the root key before calling here.
        int keyContentLen;
        SecretManager::Instance()->Decrypt(keyContent, text, keyContentLen);
        keyContent.assign(text.get(), keyContentLen);
        return Status::OK();
    }

    const std::string curveKeyDir = "data/client_zmq_curve_test/encrypt_dir";
    const std::string validClientName = "client";
    const std::string invalidClientName = "invalid_client";
    RpcAuthKeys authKeys_ = {};
};
}  // namespace st
}  // namespace datasystem
#endif  // DATASYSTEM_TEST_ST_ZMQ_CURVE_TEST_COMMON_H