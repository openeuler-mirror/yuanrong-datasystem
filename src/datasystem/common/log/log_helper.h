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
 * Description: skip sensitive info in protobuf req/resp for log.
 */

#ifndef DATASYSTEM_COMMON_UTIL_LOG_HELPER_H
#define DATASYSTEM_COMMON_UTIL_LOG_HELPER_H

#include <iostream>

namespace datasystem {
class LogHelper {
public:
    LogHelper() = default;
    ~LogHelper() = default;

    template <typename T>
    struct HasSensitive {
        // token() is one of the sensitive info
        template <typename U>
        static auto HasToken(int) -> decltype(std::declval<U>().token(), std::true_type());
        template <typename U>
        static std::false_type HasToken(...);

        // signature() is one of the sensitive info
        template <typename U>
        static auto HasSignature(int) -> decltype(std::declval<U>().signature(), std::true_type());
        template <typename U>
        static std::false_type HasSignature(...);

        // has any sensitive info
        static constexpr bool value = decltype(HasToken<T>(0))::value || decltype(HasSignature<T>(0))::value;
    };

    template <typename ReqType>
    static typename std::enable_if<HasSensitive<ReqType>::value, std::string>::type IgnoreSensitive(const ReqType &req)
    {
        ReqType copy = req;
        auto tokenField = copy.descriptor()->FindFieldByName("token");
        if (tokenField != nullptr) {
            copy.GetReflection()->SetString(&copy, tokenField, "***");
        }

        auto signatureField = copy.descriptor()->FindFieldByName("signature");
        if (signatureField != nullptr) {
            copy.GetReflection()->SetString(&copy, signatureField, "***");
        }

        auto accessKeyField = copy.descriptor()->FindFieldByName("access_key");
        if (accessKeyField != nullptr) {
            copy.GetReflection()->SetString(&copy, accessKeyField, "***");
        }

        return copy.ShortDebugString();
    }

    template <typename ReqType>
    static typename std::enable_if<!HasSensitive<ReqType>::value, std::string>::type IgnoreSensitive(const ReqType &req)
    {
        return req.ShortDebugString();
    }
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_UTIL_LOG_HELPER_H
