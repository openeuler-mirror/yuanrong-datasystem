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
#include "datasystem/utils/embedded_config.h"

namespace datasystem {

EmbeddedConfig &EmbeddedConfig::Address(const std::string &address)
{
    extraArgs["worker_address"] = address;
    return *this;
}

EmbeddedConfig &EmbeddedConfig::EtcdAddress(const std::string &address)
{
    extraArgs["etcd_address"] = address;
    return *this;
}

EmbeddedConfig &EmbeddedConfig::SharedMemorySizeMb(uint64_t size)
{
    extraArgs["shared_memory_size_mb"] = std::to_string(size);
    return *this;
}

EmbeddedConfig &EmbeddedConfig::UnixDomainSocketDir(const std::string &path)
{
    extraArgs["unix_domain_socket_dir"] = path;
    return *this;
}

EmbeddedConfig &EmbeddedConfig::EnableHugeTlb(bool enable)
{
    extraArgs["enable_huge_tlb"] = enable ? "true" : "false";
    return *this;
}

EmbeddedConfig &EmbeddedConfig::EnableFallocate(bool enable)
{
    extraArgs["enable_fallocate"] = enable ? "true" : "false";
    return *this;
}

EmbeddedConfig &EmbeddedConfig::SharedMemoryPopulate(bool populate)
{
    extraArgs["shared_memory_populate"] = populate ? "true" : "false";
    return *this;
}

EmbeddedConfig &EmbeddedConfig::EnableThp(bool enable)
{
    extraArgs["enable_thp"] = enable ? "true" : "false";
    return *this;
}

EmbeddedConfig &EmbeddedConfig::ArenaPerTenant(int num)
{
    extraArgs["arena_per_tenant"] = std::to_string(num);
    return *this;
}

EmbeddedConfig &EmbeddedConfig::EnableUrma(bool enable)
{
    extraArgs["enable_urma"] = enable ? "true" : "false";
    return *this;
}

EmbeddedConfig &EmbeddedConfig::UrmaPollSize(int size)
{
    extraArgs["urma_poll_size"] = std::to_string(size);
    return *this;
}

EmbeddedConfig &EmbeddedConfig::UrmaRegisterWholeArena(bool whole)
{
    extraArgs["urma_register_whole_arena"] = whole ? "true" : "false";
    return *this;
}

EmbeddedConfig &EmbeddedConfig::UrmaEventMode(bool enable)
{
    extraArgs["urma_event_mode"] = enable ? "true" : "false";
    return *this;
}

EmbeddedConfig &EmbeddedConfig::LogDir(const std::string &path)
{
    extraArgs["log_dir"] = path;
    return *this;
}

EmbeddedConfig &EmbeddedConfig::MinLogLevel(int level)
{
    extraArgs["minloglevel"] = std::to_string(level);
    return *this;
}

EmbeddedConfig &EmbeddedConfig::MaxLogSize(int size)
{
    extraArgs["max_log_size"] = std::to_string(size);
    return *this;
}

EmbeddedConfig &EmbeddedConfig::VLogLevel(int level)
{
    extraArgs["v"] = std::to_string(level);
    return *this;
}

EmbeddedConfig &EmbeddedConfig::RocksdbStoreDir(const std::string &path)
{
    extraArgs["rocksdb_store_dir"] = path;
    return *this;
}

EmbeddedConfig &EmbeddedConfig::RocksdbMaxOpenFile(int num)
{
    extraArgs["rocksdb_max_open_file"] = std::to_string(num);
    return *this;
}

EmbeddedConfig &EmbeddedConfig::RocksdbBackgroundThreads(int num)
{
    extraArgs["rocksdb_background_threads"] = std::to_string(num);
    return *this;
}

EmbeddedConfig &EmbeddedConfig::RocksdbWriteMode(int mode)
{
    extraArgs["rocksdb_write_mode"] = std::to_string(mode);
    return *this;
}

EmbeddedConfig &EmbeddedConfig::EnableRedirect(bool enable)
{
    extraArgs["enable_redirect"] = enable ? "true" : "false";
    return *this;
}

EmbeddedConfig &EmbeddedConfig::EnableDataReplication(bool enable)
{
    extraArgs["enable_data_replication"] = enable ? "true" : "false";
    return *this;
}

EmbeddedConfig &EmbeddedConfig::SetArg(const std::string &name, const std::string &value)
{
    extraArgs[name] = value;
    return *this;
}

EmbeddedConfig &EmbeddedConfig::SetArgs(const std::unordered_map<std::string, std::string> &args)
{
    for (const auto &kv : args) {
        extraArgs[kv.first] = kv.second;
    }
    return *this;
}

const std::unordered_map<std::string, std::string> &EmbeddedConfig::GetArgs() const
{
    return extraArgs;
}
}  // namespace datasystem
