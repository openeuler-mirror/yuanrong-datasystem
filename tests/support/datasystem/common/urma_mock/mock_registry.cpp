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

#include "datasystem/common/urma_mock/mock_registry.h"

#include "datasystem/common/urma_mock/urma_mock_backend.h"

namespace datasystem {
namespace urma_mock {

SideTables &Tables()
{
    static SideTables *s = new SideTables();
    return *s;
}

MockTablesLock::MockTablesLock() : backendLock_(MockUrmaBackend::Mu()), tables_(Tables()), tablesLock_(tables_.mu)
{
}

MockTablesLock::~MockTablesLock() = default;

SideTables &MockTablesLock::GetTables()
{
    return tables_;
}

}  // namespace urma_mock
}  // namespace datasystem
