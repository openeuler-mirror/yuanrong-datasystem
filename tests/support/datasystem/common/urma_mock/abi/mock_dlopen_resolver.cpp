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

#include "datasystem/common/urma_mock/abi/mock_dlopen_resolver.h"

#include <cstddef>

#include "securec.h"

#include "datasystem/common/log/log.h"
#include "datasystem/common/urma_mock/abi/mock_abi.h"
#include "datasystem/common/urma_mock/transport/uds_endpoint_service.h"
#include "datasystem/common/urma_mock/urma_mock_backend.h"

namespace datasystem {
namespace urma_mock {

struct UrmaMockDlopenOps {
    void *(*handle)();
    bool (*init)();
    void (*cleanup)();
    void *(*symbol)(const char *name);
};

namespace {
char g_mockUrmaDlopenHandle;
void *const K_URMA_MOCK_HANDLE = &g_mockUrmaDlopenHandle;
constexpr size_t K_MOCK_SYMBOL_COUNT = 51;

template <typename Fn>
void *CastMockFunction(Fn fn)
{
    void *symbol = nullptr;
    constexpr auto symbolSize = sizeof(void *);
    constexpr auto functionSize = sizeof(Fn);
    static_assert(functionSize == symbolSize, "URMA mock function pointer size mismatch");
    int ret = memcpy_s(reinterpret_cast<void *>(&symbol), symbolSize, static_cast<const void *>(&fn), functionSize);
    if (ret != 0) {
        LOG(ERROR) << "[UrmaDlopen] memcpy_s failed while casting mock symbol: " << ret;
        return nullptr;
    }
    return symbol;
}

template <typename Fn>
void *LoadMockFunction(Fn fn)
{
    return CastMockFunction(fn);
}

struct MockSymbolEntry {
    const char *sdkName;
    void *(*load)();
};

template <auto Fn>
void *LoadMockSymbol()
{
    return LoadMockFunction(Fn);
}

template <auto Fn>
constexpr MockSymbolEntry MakeMockSymbol(const char *sdkName)
{
    return { sdkName, &LoadMockSymbol<Fn> };
}

constexpr MockSymbolEntry MOCK_SYMBOLS[] = {
    MakeMockSymbol<ds_urma_mock_init>("urma_init"),
    MakeMockSymbol<ds_urma_mock_uninit>("urma_uninit"),
    MakeMockSymbol<ds_urma_mock_register_log_func>("urma_register_log_func"),
    MakeMockSymbol<ds_urma_mock_unregister_log_func>("urma_unregister_log_func"),
    MakeMockSymbol<ds_urma_mock_get_device_list>("urma_get_device_list"),
    MakeMockSymbol<ds_urma_mock_get_device_by_name>("urma_get_device_by_name"),
    MakeMockSymbol<ds_urma_mock_query_device>("urma_query_device"),
    MakeMockSymbol<ds_urma_mock_get_eid_list>("urma_get_eid_list"),
    MakeMockSymbol<ds_urma_mock_free_eid_list>("urma_free_eid_list"),
    MakeMockSymbol<ds_urma_mock_create_context>("urma_create_context"),
    MakeMockSymbol<ds_urma_mock_delete_context>("urma_delete_context"),
    MakeMockSymbol<ds_urma_mock_set_context_opt>("urma_set_context_opt"),
    MakeMockSymbol<ds_urma_mock_user_ctl>("urma_user_ctl"),
    MakeMockSymbol<ds_urma_mock_create_jfce>("urma_create_jfce"),
    MakeMockSymbol<ds_urma_mock_delete_jfce>("urma_delete_jfce"),
    MakeMockSymbol<ds_urma_mock_create_jfc>("urma_create_jfc"),
    MakeMockSymbol<ds_urma_mock_delete_jfc>("urma_delete_jfc"),
    MakeMockSymbol<ds_urma_mock_rearm_jfc>("urma_rearm_jfc"),
    MakeMockSymbol<ds_urma_mock_create_jfr>("urma_create_jfr"),
    MakeMockSymbol<ds_urma_mock_delete_jfr>("urma_delete_jfr"),
    MakeMockSymbol<ds_urma_mock_create_jfs>("urma_create_jfs"),
    MakeMockSymbol<ds_urma_mock_delete_jfs>("urma_delete_jfs"),
    MakeMockSymbol<ds_urma_mock_modify_jfs>("urma_modify_jfs"),
    MakeMockSymbol<ds_urma_mock_create_jetty>("urma_create_jetty"),
    MakeMockSymbol<ds_urma_mock_delete_jetty>("urma_delete_jetty"),
    MakeMockSymbol<ds_urma_mock_modify_jetty>("urma_modify_jetty"),
    MakeMockSymbol<ds_urma_mock_get_rjetty>("urma_get_rjetty"),
    MakeMockSymbol<ds_urma_mock_put_rjetty>("urma_put_rjetty"),
    MakeMockSymbol<ds_urma_mock_register_seg>("urma_register_seg"),
    MakeMockSymbol<ds_urma_mock_wait_jfc>("urma_wait_jfc"),
    MakeMockSymbol<ds_urma_mock_poll_jfc>("urma_poll_jfc"),
    MakeMockSymbol<ds_urma_mock_ack_jfc>("urma_ack_jfc"),
    MakeMockSymbol<ds_urma_mock_import_jfr>("urma_import_jfr"),
    MakeMockSymbol<ds_urma_mock_advise_jfr>("urma_advise_jfr"),
    MakeMockSymbol<ds_urma_mock_unimport_jfr>("urma_unimport_jfr"),
    MakeMockSymbol<ds_urma_mock_import_jetty>("urma_import_jetty"),
    MakeMockSymbol<ds_urma_mock_unimport_jetty>("urma_unimport_jetty"),
    MakeMockSymbol<ds_urma_mock_post_jetty_send_wr>("urma_post_jetty_send_wr"),
    MakeMockSymbol<ds_urma_mock_write>("urma_write"),
    MakeMockSymbol<ds_urma_mock_read>("urma_read"),
    MakeMockSymbol<ds_urma_mock_post_jfs_wr>("urma_post_jfs_wr"),
    MakeMockSymbol<ds_urma_mock_import_seg>("urma_import_seg"),
    MakeMockSymbol<ds_urma_mock_get_seg_ctx>("urma_get_seg_ctx"),
    MakeMockSymbol<ds_urma_mock_put_seg_ctx>("urma_put_seg_ctx"),
    MakeMockSymbol<ds_urma_mock_unregister_seg>("urma_unregister_seg"),
    MakeMockSymbol<ds_urma_mock_unimport_seg>("urma_unimport_seg"),
    MakeMockSymbol<ds_urma_mock_get_async_event>("urma_get_async_event"),
    MakeMockSymbol<ds_urma_mock_ack_async_event>("urma_ack_async_event"),
    MakeMockSymbol<ds_urma_mock_start_perf>("urma_start_perf"),
    MakeMockSymbol<ds_urma_mock_stop_perf>("urma_stop_perf"),
    MakeMockSymbol<ds_urma_mock_get_perf_info>("urma_get_perf_info"),
};

static_assert(sizeof(MOCK_SYMBOLS) / sizeof(MOCK_SYMBOLS[0]) == K_MOCK_SYMBOL_COUNT,
              "Update URMA mock dispatch table when adding or removing mock ABI symbols.");
}  // namespace

void *MockUrmaDlopenHandle()
{
    return K_URMA_MOCK_HANDLE;
}

bool IsMockUrmaDlopenHandle(void *handle)
{
    return handle == K_URMA_MOCK_HANDLE;
}

bool InitMockUrmaDlopen()
{
    return MockUrmaBackend::Instance().Init();
}

void CleanupMockUrmaDlopen()
{
    MockUrmaBackend::Instance().Cleanup();
}

void *LoadMockUrmaSymbol(const std::string &name)
{
    for (const auto &entry : MOCK_SYMBOLS) {
        if (name == entry.sdkName) {
            return entry.load();
        }
    }
    return nullptr;
}

}  // namespace urma_mock
}  // namespace datasystem

namespace {
void *DsUrmaMockDlopenHandle()
{
    return datasystem::urma_mock::MockUrmaDlopenHandle();
}

bool DsUrmaMockDlopenInit()
{
    return datasystem::urma_mock::InitMockUrmaDlopen();
}

void DsUrmaMockDlopenCleanup()
{
    datasystem::urma_mock::CleanupMockUrmaDlopen();
}

void *DsUrmaMockDlopenSymbol(const char *name)
{
    if (name == nullptr) {
        return nullptr;
    }
    return datasystem::urma_mock::LoadMockUrmaSymbol(name);
}

const datasystem::urma_mock::UrmaMockDlopenOps G_URMA_MOCK_DLOPEN_OPS = {
    DsUrmaMockDlopenHandle,
    DsUrmaMockDlopenInit,
    DsUrmaMockDlopenCleanup,
    DsUrmaMockDlopenSymbol,
};
}  // namespace

extern "C" const datasystem::urma_mock::UrmaMockDlopenOps *ds_urma_mock_dlopen_ops()
{
    return &G_URMA_MOCK_DLOPEN_OPS;
}
