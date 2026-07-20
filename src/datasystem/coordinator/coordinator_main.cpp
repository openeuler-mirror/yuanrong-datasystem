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
 * Description: Coordinator server entrypoint.
 */
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/coordinator_server.h"

int main(int argc, char **argv)
{
    datasystem::TraceGuard traceGuard =
        datasystem::Trace::Instance().SetTraceNewID("CoordMain;" + datasystem::GetStringUuid());
    datasystem::ParseCommandLineFlags(argc, argv);

    auto rc = datasystem::CoordinatorServer::GetInstance()->InitAndRun();
    if (rc.IsError()) {
        LOG(ERROR) << "Coordinator runtime error: " << rc.ToString();
        return -1;
    }
    return 0;
}
