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
 * Description: OBS XML construction and parsing utility (no third-party XML library).
 */

#ifndef DATASYSTEM_COMMON_L2CACHE_OBS_CLIENT_OBS_XML_UTIL_H
#define DATASYSTEM_COMMON_L2CACHE_OBS_CLIENT_OBS_XML_UTIL_H

#include <string>
#include <vector>
#include <utility>

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
class ObsXmlUtil {
public:
    ~ObsXmlUtil() = default;
    // === XML Construction ===

    /**
     * @brief Build Batch Delete request XML body.
     * @param[in] keys Object keys to delete.
     * @return XML string.
     */
    static std::string BuildBatchDeleteXml(const std::vector<std::string> &keys);

    /**
     * @brief Build Complete Multipart Upload request XML body.
     * @param[in] parts Vector of (partNumber, eTag) pairs.
     * @return XML string.
     */
    static std::string BuildCompleteMultipartXml(const std::vector<std::pair<int, std::string>> &parts);

    // === XML Parsing ===

    /**
     * @brief Extract the text content of the first occurrence of a tag.
     * @param[in] xml XML string.
     * @param[in] tag Tag name.
     * @return Tag content, or empty string if not found.
     */
    static std::string GetXmlElement(const std::string &xml, const std::string &tag);

    /**
     * @brief Extract text content of all occurrences of a tag.
     * @param[in] xml XML string.
     * @param[in] tag Tag name.
     * @return Vector of tag contents.
     */
    static std::vector<std::string> GetXmlElements(const std::string &xml, const std::string &tag);

    /**
     * @brief Parse Initiate Multipart Upload response to extract UploadId.
     * @param[in] xml Response XML.
     * @return UploadId, or empty string on failure.
     */
    static std::string ParseInitiateMultipartResponse(const std::string &xml);

    /**
     * @brief Parse List Objects response XML.
     * @param[in] xml Response XML.
     * @param[out] keys Object keys found.
     * @param[out] sizes Object sizes (parallel to keys).
     * @param[out] isTruncated Whether there are more results.
     * @param[out] nextMarker Marker for next page.
     * @return Status of the call.
     */
    static Status ParseListObjectsResponse(const std::string &xml, std::vector<std::string> &keys,
                                           std::vector<uint64_t> &sizes, bool &isTruncated,
                                           std::string &nextMarker);

    /**
     * @brief Parse OBS error response XML.
     * @param[in] xml Response XML.
     * @param[out] code Error code (e.g., "NoSuchKey").
     * @param[out] message Error message.
     */
    static void ParseErrorResponse(const std::string &xml, std::string &code, std::string &message);
};
}  // namespace datasystem
#endif
