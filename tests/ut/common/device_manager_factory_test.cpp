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
 * Description: Device manager factory test.
 */
#include <fnmatch.h>

#include <string>
#include <vector>

#include "datasystem/common/device/device_manager_factory.h"
#include "ut/common.h"

namespace datasystem {
namespace ut {

TEST(DeviceManagerFactoryTest, NpuDeviceGlobMatchesNumericDeviceNodes)
{
    const std::vector<std::string> devicePaths = {
        "/dev/davinci0", "/dev/davinci2",  "/dev/davinci5",  "/dev/davinci7",
        "/dev/davinci9", "/dev/davinci10", "/dev/davinci15", "/dev/davinci16"
    };
    for (const auto &devicePath : devicePaths) {
        EXPECT_EQ(fnmatch("/dev/davinci[0-9]*", devicePath.c_str(), 0), 0) << devicePath;
    }
}

TEST(DeviceManagerFactoryTest, NpuDeviceGlobRejectsNonNumericDeviceNodes)
{
    const std::vector<std::string> nonDevicePaths = { "/dev/davinci_manager", "/dev/davinci", "/dev/nvidia0" };
    for (const auto &path : nonDevicePaths) {
        EXPECT_EQ(fnmatch("/dev/davinci[0-9]*", path.c_str(), 0), FNM_NOMATCH) << path;
    }
}

}  // namespace ut
}  // namespace datasystem
