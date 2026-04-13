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
 * Description: OBS XML construction and parsing utility implementation.
 */

#include "datasystem/common/l2cache/obs_client/obs_xml_util.h"

#include <sstream>

namespace datasystem {

std::string ObsXmlUtil::BuildBatchDeleteXml(const std::vector<std::string> &keys)
{
    std::ostringstream oss;
    oss << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Delete>\n<Quiet>true</Quiet>\n";
    for (const auto &key : keys) {
        oss << "  <Object><Key>" << key << "</Key></Object>\n";
    }
    oss << "</Delete>";
    return oss.str();
}

std::string ObsXmlUtil::BuildCompleteMultipartXml(const std::vector<std::pair<int, std::string>> &parts)
{
    std::ostringstream oss;
    oss << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CompleteMultipartUpload>\n";
    for (const auto &part : parts) {
        oss << "  <Part><PartNumber>" << part.first << "</PartNumber><ETag>" << part.second
            << "</ETag></Part>\n";
    }
    oss << "</CompleteMultipartUpload>";
    return oss.str();
}

std::string ObsXmlUtil::GetXmlElement(const std::string &xml, const std::string &tag)
{
    std::string openTag = "<" + tag + ">";
    std::string closeTag = "</" + tag + ">";
    auto start = xml.find(openTag);
    if (start == std::string::npos) {
        return "";
    }
    start += openTag.size();
    auto end = xml.find(closeTag, start);
    if (end == std::string::npos) {
        return "";
    }
    return xml.substr(start, end - start);
}

std::vector<std::string> ObsXmlUtil::GetXmlElements(const std::string &xml, const std::string &tag)
{
    std::vector<std::string> results;
    std::string openTag = "<" + tag + ">";
    std::string closeTag = "</" + tag + ">";
    size_t pos = 0;
    while (pos < xml.size()) {
        auto start = xml.find(openTag, pos);
        if (start == std::string::npos) {
            break;
        }
        start += openTag.size();
        auto end = xml.find(closeTag, start);
        if (end == std::string::npos) {
            break;
        }
        results.push_back(xml.substr(start, end - start));
        pos = end + closeTag.size();
    }
    return results;
}

std::string ObsXmlUtil::ParseInitiateMultipartResponse(const std::string &xml)
{
    return GetXmlElement(xml, "UploadId");
}

Status ObsXmlUtil::ParseListObjectsResponse(const std::string &xml, std::vector<std::string> &keys,
                                            std::vector<uint64_t> &sizes, bool &isTruncated,
                                            std::string &nextMarker)
{
    keys.clear();
    sizes.clear();
    isTruncated = false;
    nextMarker.clear();

    std::string truncated = GetXmlElement(xml, "IsTruncated");
    isTruncated = (truncated == "true");
    nextMarker = GetXmlElement(xml, "NextMarker");

    std::string openTag = "<Contents>";
    std::string closeTag = "</Contents>";
    size_t pos = 0;
    while (pos < xml.size()) {
        auto start = xml.find(openTag, pos);
        if (start == std::string::npos) {
            break;
        }
        auto end = xml.find(closeTag, start);
        if (end == std::string::npos) {
            break;
        }
        std::string content = xml.substr(start, end + closeTag.size() - start);
        std::string key = GetXmlElement(content, "Key");
        std::string sizeStr = GetXmlElement(content, "Size");
        if (!key.empty()) {
            keys.push_back(key);
            sizes.push_back(sizeStr.empty() ? 0 : std::stoull(sizeStr));
        }
        pos = end + closeTag.size();
    }
    // Fallback: when IsTruncated=true but NextMarker is absent, use the last key as marker.
    if (isTruncated && nextMarker.empty() && !keys.empty()) {
        nextMarker = keys.back();
    }
    return Status::OK();
}

void ObsXmlUtil::ParseErrorResponse(const std::string &xml, std::string &code, std::string &message)
{
    code = GetXmlElement(xml, "Code");
    message = GetXmlElement(xml, "Message");
}

}  // namespace datasystem
