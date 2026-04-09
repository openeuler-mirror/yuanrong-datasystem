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
#ifndef DATASYSTEM_UTILS_CONFIG_H
#define DATASYSTEM_UTILS_CONFIG_H

#include <cstdint>
#include <string>
#include <unordered_map>

namespace datasystem {
class EmbeddedConfig {
public:
    EmbeddedConfig() = default;
    ~EmbeddedConfig() = default;

    /**
     * @brief Set server listening address.
     * @param[in] address Equivalent to flag worker_address.
     * @return Reference to self for chaining.
     */
    EmbeddedConfig &Address(const std::string &address);

    /**
     * @brief Set etcd endpoint list.
     * @param[in] address Equivalent to flag etcd_address.
     * @return Reference to self for chaining.
     */
    EmbeddedConfig &EtcdAddress(const std::string &address);

    /**
     * @brief Size of shared-memory arena in MiB.
     * @param[in] size Equivalent to flag shared_memory_size_mb.
     * @return Reference to self for chaining.
     */
    EmbeddedConfig &SharedMemorySizeMb(uint64_t size);

    /**
     * @brief Directory that holds Unix-domain sockets.
     * @param[in] path Equivalent to flag unix_domain_socket_dir.
     * @return Reference to self for chaining.
     */
    EmbeddedConfig &UnixDomainSocketDir(const std::string &path);

    /**
     * @brief Enable transparent huge_pages for shared memory.
     * @param[in] enable Equivalent to flag enable_huge_tlb.
     * @return Reference to self for chaining.
     */
    EmbeddedConfig &EnableHugeTlb(bool enable);

    /**
     * @brief Pre-allocate shared memory via fallocate.
     * @param[in] enable Equivalent to flag enable_fallocate.
     * @return Reference to self for chaining.
     */
    EmbeddedConfig &EnableFallocate(bool enable);

    /**
     * @brief Populate shared memory up-front to avoid page-faults.
     * @param[in] populate Equivalent to flag shared_memory_populate.
     * @return Reference to self for chaining.
     */
    EmbeddedConfig &SharedMemoryPopulate(bool populate);

    /**
     * @brief Enable transparent-huge-page inside arena.
     * @param[in] enable Equivalent to flag enable_huge_thp.
     * @return Reference to self for chaining.
     */
    EmbeddedConfig &EnableThp(bool enable);

    /**
     * @brief Number of arenas per tenant.
     * @param[in] num Equivalent to flag arena_per_tenant.
     * @return Reference to self for chaining.
     */
    EmbeddedConfig &ArenaPerTenant(int num);

    /**
     * @brief Enable URMA transport.
     * @param[in] enable Equivalent to flag enable_urma.
     * @return Reference to self for chaining.
     */
    EmbeddedConfig &EnableUrma(bool enable);

    /**
     * @brief URMA completion-queue poll size.
     * @param[in] size Equivalent to flag urma_poll_size.
     * @return Reference to self for chaining.
     */
    EmbeddedConfig &UrmaPollSize(int size);

    /**
     * @brief Register the whole arena to URMA in one chunk.
     * @param[in] whole Equivalent to flag urma_register_whole_arena.
     * @return Reference to self for chaining.
     */
    EmbeddedConfig &UrmaRegisterWholeArena(bool whole);

    /**
     * @brief Use URMA event-driven mode.
     * @param[in] enable Equivalent to flag urma_event_mode.
     * @return Reference to self for chaining.
     */
    EmbeddedConfig &UrmaEventMode(bool enable);

    /**
     * @brief Directory to stroe log files.
     * @param[in] path Equivalent to flag log_dir.
     * @return Reference to self for chaining.
     */
    EmbeddedConfig &LogDir(const std::string &path);

    /**
     * @brief Minimum severity level to write to log file.
     * @param[in] level Equivalent to flag min_log_level.
     * @return Reference to self for chaining.
     */
    EmbeddedConfig &MinLogLevel(int level);

    /**
     * @brief Maximun size in Mib before log rotation.
     * @param[in] size Equivalent to flag max_log_size.
     * @return Reference to self for chaining.
     */
    EmbeddedConfig &MaxLogSize(int size);

    /**
     * @brief Verbose log level (vlog).
     * @param[in] level Equivalent to flag v.
     * @return Reference to self for chaining.
     */
    EmbeddedConfig &VLogLevel(int level);

    /**
     * @brief Directory for rocksdb metadata files.
     * @param[in] path Equivalent to flag rocksdb_store_dir.
     * @return Reference to self for chaining.
     */
    EmbeddedConfig &RocksdbStoreDir(const std::string &path);

    /**
     * @brief Maximum number of open files for rocksdb.
     * @param[in] num Equivalent to flag rocksdb_max_open_file.
     * @return Reference to self for chaining.
     */
    EmbeddedConfig &RocksdbMaxOpenFile(int num);

    /**
     * @brief Number of backgroud threads for rocksdb compaction/flushing.
     * @param[in] num Equivalent to flag rocksdb_background_threads.
     * @return Reference to self for chaining.
     */
    EmbeddedConfig &RocksdbBackgroundThreads(int num);

    /**
     * @brief Rocksdb write mode.
     * @param[in] mode Equivalent to flag rocksdb_write_mode.
     * @return Reference to self for chaining.
     */
    EmbeddedConfig &RocksdbWriteMode(int mode);

    /**
     * @brief Enable meta redirection.
     * @param[in] enable Equivalent to flag enable_redirect.
     * @return Reference to self for chaining.
     */
    EmbeddedConfig &EnableRedirect(bool enable);

    /**
     * @brief Enable data replication across zones.
     * @param[in] enable Equivalent to flag enable_data_replication.
     * @return Reference to self for chaining.
     */
    EmbeddedConfig &EnableDataReplication(bool enable);

    /**
     * @brief Set a single extra argument directly.
     * @param[in] name Argument name (maps to extraArgs[name]).
     * @param[in] value Argument value.
     * @return Reference to self for chaining.
     */
    EmbeddedConfig &SetArg(const std::string &name, const std::string &value);

    /**
     * @brief Set multiple extra arguments in bulk.
     * @param[in] args Map of name-value pairs; each maps to extraArgs[name]=value.
     * @return Reference to self for chaining.
     */
    EmbeddedConfig &SetArgs(const std::unordered_map<std::string, std::string> &args);

    /**
     * @brief Get extraArgs.
     * @return Map of name-value pairs.
     */
    const std::unordered_map<std::string, std::string> &GetArgs() const;

private:
    std::unordered_map<std::string, std::string> extraArgs;
};
}  // namespace datasystem

#endif  // DATASYSTEM_UTILS_CONFIG_H