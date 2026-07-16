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

#ifndef DATASYSTEM_COMMON_URMA_MOCK_MOCK_REGISTRY_H
#define DATASYSTEM_COMMON_URMA_MOCK_MOCK_REGISTRY_H

#include <memory>
#include <mutex>
#include <unordered_map>

#include "datasystem/common/urma_mock/abi/urma_abi_compat.h"
#include "datasystem/common/urma_mock/objects/mock_context.h"
#include "datasystem/common/urma_mock/objects/mock_device.h"
#include "datasystem/common/urma_mock/objects/mock_jfc.h"
#include "datasystem/common/urma_mock/objects/mock_jetty.h"
#include "datasystem/common/urma_mock/objects/mock_seg.h"
#include "datasystem/common/urma_mock/objects/mock_tjetty.h"

namespace datasystem {
namespace urma_mock {
/**
 * @brief Process-wide side tables from URMA shim pointers to mock objects.
 * C ABI entry points populate these maps when returning shim pointers and erase entries in the matching delete or free
 * call. shared_ptr values keep in-flight send/import work valid even after a registry entry is removed.
 *
 * @note Code that needs both backend state and side tables must take MockUrmaBackend::mu_ before SideTables::mu. Do
 * not call ds_urma_mock_* entry points while holding either lock.
 */
struct SideTables {
    std::mutex mu;
    std::unordered_map<urma_context_t *, std::shared_ptr<MockContext>> ctx;
    std::unordered_map<urma_jfce_t *, std::shared_ptr<MockContext>> jfce;
    std::unordered_map<urma_jfc_t *, std::shared_ptr<MockJfc>> jfc;
    std::unordered_map<urma_jfr_t *, std::shared_ptr<MockContext>> jfr;  ///< JFR tracked at context level.
    std::unordered_map<urma_jetty_t *, std::shared_ptr<MockJetty>> jetty;
    std::unordered_map<urma_target_jetty_t *, std::shared_ptr<MockTjetty>> tjetty;
    std::unordered_map<urma_target_seg_t *, std::shared_ptr<MockSeg>> tseg;
    std::unordered_map<urma_seg_t *, std::shared_ptr<MockSeg>> seg;  ///< import_seg return handle.
    std::unordered_map<urma_device_t *, std::shared_ptr<MockDevice>> dev;
};

/**
 * @brief Get the process-wide URMA shim pointer side tables.
 * @return Mutable side-table registry. Callers must hold SideTables::mu when reading or writing maps.
 */
SideTables &Tables();

/**
 * @brief Lookup a mock object from a side table while retaining its lifetime.
 * @param[in] map Side-table map guarded by SideTables::mu.
 * @param[in] key Raw ABI handle.
 * @return Shared mock object, or nullptr when key is absent.
 */
template <typename Key, typename Value>
std::shared_ptr<Value> FindMockObject(const std::unordered_map<Key, std::shared_ptr<Value>> &map, Key key)
{
    auto it = map.find(key);
    return it == map.end() ? nullptr : it->second;
}

/**
 * @brief Lock helper for code that needs backend state and URMA pointer side tables together.
 * The constructor follows the mock lock order: backend mutex first, side-table mutex second.
 */
class MockTablesLock {
public:
    /**
     * @brief Acquire backend and side-table locks.
     */
    MockTablesLock();

    /**
     * @brief Release side-table and backend locks.
     */
    ~MockTablesLock();

    /**
     * @brief Get the locked process-wide side tables.
     * @return Side table registry guarded by this object.
     */
    SideTables &GetTables();

private:
    std::lock_guard<std::mutex> backendLock_;
    SideTables &tables_;
    std::lock_guard<std::mutex> tablesLock_;
};

}  // namespace urma_mock
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_URMA_MOCK_MOCK_REGISTRY_H
