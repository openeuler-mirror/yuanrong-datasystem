/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: Some tools for protobuf.
 */
#ifndef DATASYSTEM_COMMON_UTIL_PROTOBUF_UTIL_H
#define DATASYSTEM_COMMON_UTIL_PROTOBUF_UTIL_H

#include <google/protobuf/util/message_differencer.h>
#include <google/protobuf/descriptor.h>

#include "datasystem/common/log/log.h"

using google::protobuf::util::MessageDifferencer;
using google::protobuf::FieldDescriptor;
using google::protobuf::Descriptor;

namespace datasystem {
/**
* @brief Compare map fields in protobuf.
* @param[in] msg1 The Pb need to be compaired.
* @param[in] msg2 The Pb need to be compaired.
* @param[in] mapFieldName The map field name.
* @return T/F
*/
template <typename T>
bool CompareMapFields(const T &msg1, const T &msg2, const std::string &mapFieldName)
{
    MessageDifferencer differencer;
    const Descriptor *descriptor = T::descriptor();

    const FieldDescriptor *mapField = descriptor->FindFieldByName(mapFieldName);
    if (!mapField) {
        LOG(ERROR) << "find field by name failed: " << mapFieldName;
        return false;
    }
    const Descriptor *msgType = mapField->message_type();
    if (!msgType) {
        LOG(ERROR) << "The type of field may be wrong";
        return false;
    }
    const FieldDescriptor *keyField = msgType->FindFieldByName("key");
    if (!keyField) {
        LOG(ERROR) << "find key field in map failed";
        return false;
    }

    differencer.TreatAsMap(mapField, keyField);
    return differencer.Compare(msg1, msg2);
}
}  // namespace datasystem
#endif
