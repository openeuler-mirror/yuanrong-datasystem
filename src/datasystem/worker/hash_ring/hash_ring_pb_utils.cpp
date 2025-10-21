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
#include "datasystem/worker/hash_ring/hash_ring_pb_utils.h"

#include <fstream>

#include <google/protobuf/json/json.h>

#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/uri.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace worker {
using namespace google::protobuf::json;
Status HashRingPbToJson(const HashRingPb &hashRing, std::string &jsonStr, bool format)
{
    PrintOptions options;
    options.add_whitespace = format;
    auto status = MessageToJsonString(hashRing, &jsonStr, options);
    if (!status.ok()) {
        RETURN_STATUS(K_RUNTIME_ERROR, status.ToString());
    }
    return Status::OK();
}

Status HashRingPbFromJson(const std::string &jsonStr, HashRingPb &hashRing)
{
    ParseOptions options;
    auto status = JsonStringToMessage(jsonStr, &hashRing, options);
    if (!status.ok()) {
        RETURN_STATUS(K_RUNTIME_ERROR, status.ToString());
    }
    return Status::OK();
}

Status WriteToJsonFile(const std::string &filename, const HashRingPb &hashRing)
{
    std::string jsonStr;
    RETURN_IF_NOT_OK(HashRingPbToJson(hashRing, jsonStr, true));
    std::string realPath;
    RETURN_IF_NOT_OK(Uri::GetRealfile(filename, realPath));
    std::ofstream outFile(realPath);
    if (!outFile.is_open()) {
        RETURN_STATUS(K_RUNTIME_ERROR, FormatString("Cannot open file %s to write", filename));
    }
    outFile << jsonStr;
    outFile.close();

    return Status::OK();
}

Status ReadFromJsonFile(const std::string &file, HashRingPb &hashRing)
{
    std::string jsonStr;
    RETURN_IF_NOT_OK(ReadFileToString(file, jsonStr));
    return HashRingPbFromJson(jsonStr, hashRing);
}
}  // namespace worker
}  // namespace datasystem
