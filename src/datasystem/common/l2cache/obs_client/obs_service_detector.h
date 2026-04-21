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
 * Description: OBS/S3 service type detector.
 */

#ifndef DATASYSTEM_COMMON_L2CACHE_OBS_CLIENT_OBS_SERVICE_DETECTOR_H
#define DATASYSTEM_COMMON_L2CACHE_OBS_CLIENT_OBS_SERVICE_DETECTOR_H

#include <string>
#include <map>
#include <memory>

#include "datasystem/common/l2cache/obs_client/obs_signature_provider.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {

class CurlHttpClient;

class ObsServiceDetector {
public:
    /**
     * @brief Detect service type by sending a HEAD request.
     * @param[in] httpClient HTTP client to use.
     * @param[in] endpoint Service endpoint.
     * @param[in] bucket Bucket name.
     * @param[in] httpsEnabled Whether HTTPS is enabled.
     * @return Detected signature type (defaults to OBS_V2 on failure).
     */
    static SignatureType Detect(std::shared_ptr<CurlHttpClient> &httpClient,
                                const std::string &endpoint,
                                const std::string &bucket,
                                bool httpsEnabled = false);

    /**
     * @brief Parse region from endpoint hostname.
     * @param[in] endpoint Endpoint string (may include port).
     * @return Region string, defaults to "us-east-1".
     */
    static std::string ParseRegionFromEndpoint(const std::string &endpoint);

private:
    /**
     * @brief Check response headers to determine service type.
     * @param[in] headers Response headers.
     * @return Detected signature type.
     */
    static SignatureType CheckServiceType(const std::map<std::string, std::string> &headers);
};

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_L2CACHE_OBS_CLIENT_OBS_SERVICE_DETECTOR_H
