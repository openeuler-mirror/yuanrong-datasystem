/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: The state cache example.
 */

#include <iostream>

#include "datasystem/utils/aes.h"
#include "datasystem/utils/status.h"

using datasystem::Aes;
using datasystem::Status;
static constexpr int SUCCESS = 0;
static constexpr int FAILED = -1;

int main(int argc, char *argv[])
{
    std::string keyStr(32, 'a');
    datasystem::SensitiveValue key(keyStr);
    Aes aes(key, Aes::Algorithm::AES_256_GCM);
    datasystem::SensitiveValue plainText("test encrypt");
    std::string cipherText;
    Status rc = aes.Encrypt(plainText, cipherText);
    if (rc.IsError()) {
        std::cout << "Encrypt failed: " << rc.ToString() << std::endl;
        return FAILED;
    }
    datasystem::SensitiveValue plainText2;
    rc = aes.Decrypt(cipherText, plainText2);
    if (rc.IsError()) {
        std::cout << "Decrypt failed: " << rc.ToString() << std::endl;
        return FAILED;
    }
    if (plainText == plainText2) {
        std::cout << "Encrypt and decrypt success. " << rc.ToString() << std::endl;
        return SUCCESS;
    };
    std::cout << "Decrypt is not getting value encrypt by Encrypt()." << std::endl;
    return FAILED;
}
