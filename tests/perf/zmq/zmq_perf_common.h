/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: ZMQ perf common constants
 */
#ifndef DATASYSTEM_TESTS_ST_ZMQ_PERF_COMMON_H
#define DATASYSTEM_TESTS_ST_ZMQ_PERF_COMMON_H

namespace datasystem {
namespace st {
constexpr int kOneK = 1024;
constexpr int kDefaultPort = 5051;
constexpr char kDefaultHost[] = "127.0.0.1";
constexpr int kPortOpt = 1000;      // no short form for --port
constexpr int kHostOpt = 1001;      // no short form for --hostname
constexpr int kZmqIoOpt = 1002;     // no short form for --zmq_io_threads
constexpr int kUnixSockOpt = 1003;  // no short form for --unix_socket_dir
}  // namespace st
}  // namespace datasystem

#endif  // DATASYSTEM_TESTS_ST_ZMQ_PERF_COMMON_H
