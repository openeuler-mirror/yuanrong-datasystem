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
 * Description: Datasystem all client management.
 */

#include "datasystem/datasystem.h"

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
DsClient::DsClient(const ConnectOptions &connectOptions)
{
    kvClient_ = std::make_shared<KVClient>(connectOptions);
    heteroClient_ = std::make_shared<HeteroClient>(connectOptions);
    objectClient_ = std::make_shared<ObjectClient>(connectOptions);
}

Status DsClient::Init()
{
    RETURN_IF_NOT_OK(kvClient_->Init());
    RETURN_IF_NOT_OK(heteroClient_->Init());
    return objectClient_->Init();
}

Status DsClient::ShutDown()
{
    RETURN_IF_NOT_OK(objectClient_->ShutDown());
    RETURN_IF_NOT_OK(heteroClient_->ShutDown());
    return kvClient_->ShutDown();
}

std::shared_ptr<KVClient> DsClient::KV()
{
    return kvClient_;
}

std::shared_ptr<HeteroClient> DsClient::Hetero()
{
    return heteroClient_;
}

std::shared_ptr<ObjectClient> DsClient::Object()
{
    return objectClient_;
}
}  // namespace datasystem