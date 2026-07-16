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

#ifndef DATASYSTEM_COMMON_URMA_MOCK_TRANSPORT_UDS_TRANSPORT_H
#define DATASYSTEM_COMMON_URMA_MOCK_TRANSPORT_UDS_TRANSPORT_H

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace datasystem {
namespace urma_mock {
/**
 * @brief Magic value used to identify mock UDS frames.
 */
constexpr uint32_t kUdsMagic = 0xFA4B0001U;

/**
 * @brief Fixed frame header size: magic, type, flags, payload length, and reserved fields.
 */
constexpr size_t kUdsHeaderSize = 16;

/**
 * @brief Message types used by the mock UDS control channel.
 */
enum class UdsMsgType : uint8_t { HELLO = 1, HELLO_ACK = 2, POST_SEND = 10, POST_SEND_ACK = 11 };

/**
 * @brief Resolve the current process UDS socket path.
 * @param[in] overrideBase Optional base directory override for tests.
 * @return Absolute socket path for the current mock process.
 */
std::string ResolveUdsPath(const char *overrideBase = nullptr);

/**
 * @brief Resolve a peer UDS socket path by mock instance id.
 * @param[in] instance Peer UDS instance id.
 * @return Absolute socket path for the instance.
 */
std::string ResolveUdsPathForInstance(const std::string &instance);

/**
 * @brief Resolve a peer UDS socket path by wire-provided host and port.
 * @param[in] host Peer host from URMA wire address.
 * @param[in] port Peer port from URMA wire address.
 * @return Absolute socket path for the host and port pair.
 */
std::string ResolveUdsPathForHost(const std::string &host, int port);

/**
 * @brief RAII wrapper for the listening side of the mock UDS control channel.
 */
class UdsListener {
public:
    /**
     * @brief Create an unopened listener.
     */
    UdsListener();

    /**
     * @brief Close the listener and remove its socket path.
     */
    ~UdsListener();

    /**
     * @brief Listeners own an fd and socket path and cannot be copied.
     */
    UdsListener(const UdsListener &) = delete;
    /**
     * @brief Listeners own an fd and socket path and cannot be copy-assigned.
     */
    UdsListener &operator=(const UdsListener &) = delete;

    /**
     * @brief Bind and listen on a Unix domain socket path.
     * @param[in] path Socket path to bind.
     * @return true if bind and listen succeed.
     */
    bool Bind(const std::string &path);

    /**
     * @brief Accept one peer connection.
     * @return Connected fd on success; -1 on failure.
     */
    int Accept() const;

    /**
     * @brief Close the listening fd.
     */
    void Close();

    /**
     * @brief Get the bound socket path.
     * @return Socket path, or an empty string before Bind succeeds.
     */
    const std::string &GetPath() const;

    /**
     * @brief Get the listening fd.
     * @return Listening fd, or -1 before Bind succeeds or after Close.
     */
    int GetListenFd() const;

private:
    int listenFd_ = -1;
    std::string path_;
};

/**
 * @brief Connected Unix domain socket used to exchange mock control frames and fds.
 */
class UdsConnection {
public:
    /**
     * @brief Create an unopened connection.
     */
    UdsConnection();

    /**
     * @brief Close the connection fd.
     */
    ~UdsConnection();

    /**
     * @brief Connections own an fd and cannot be copied.
     */
    UdsConnection(const UdsConnection &) = delete;
    /**
     * @brief Connections own an fd and cannot be copy-assigned.
     */
    UdsConnection &operator=(const UdsConnection &) = delete;

    /**
     * @brief Connect to a peer UDS path.
     * @param[in] path Peer socket path.
     * @return true if the connection succeeds.
     */
    bool Connect(const std::string &path);

    /**
     * @brief Send a frame and optional file descriptors.
     * @param[in] type Message type.
     * @param[in] flags Frame flags.
     * @param[in] payload Payload buffer, or nullptr when payloadLen is 0.
     * @param[in] payloadLen Payload size in bytes.
     * @param[in] fds File descriptors sent with SCM_RIGHTS.
     * @param[out] outErrno errno value when send fails.
     * @return true if the complete frame is sent.
     */
    bool Send(UdsMsgType type, uint16_t flags, const uint8_t *payload, uint32_t payloadLen, const std::vector<int> &fds,
              int *outErrno = nullptr);

    /**
     * @brief Receive a frame and any attached file descriptors.
     * @param[out] outType Received message type.
     * @param[out] outFlags Received frame flags.
     * @param[out] outPayload Received payload bytes.
     * @param[out] outFds Received file descriptors.
     * @param[out] outPayloadTruncated Whether the payload exceeded the receive buffer.
     * @param[out] outErrno errno value when receive fails.
     * @return true if a complete frame is received.
     */
    bool Recv(UdsMsgType *outType, uint16_t *outFlags, std::vector<uint8_t> *outPayload, std::vector<int> *outFds,
              bool *outPayloadTruncated = nullptr, int *outErrno = nullptr) const;

    /**
     * @brief Set receive timeout for the connected fd.
     * @param[in] timeoutMs Timeout in milliseconds.
     * @return true if the timeout is set.
     */
    bool SetRecvTimeout(int timeoutMs);

    /**
     * @brief Adopt an existing connected fd.
     * @param[in] fd Connected Unix domain socket fd.
     */
    void AdoptFd(int fd);

private:
    int fd_ = -1;
};

/**
 * @brief Decode a UDS frame header from a 16-byte buffer.
 * @param[in] buf Source buffer with at least kUdsHeaderSize bytes.
 * @param[out] outMagic Decoded protocol magic.
 * @param[out] outType Decoded message type.
 * @param[out] outFlags Decoded frame flags.
 * @param[out] outPayloadLen Decoded payload size in bytes.
 * @return true if the header is well formed.
 */
bool UnpackUdsHeader(const uint8_t *buf, uint32_t *outMagic, UdsMsgType *outType, uint16_t *outFlags,
                     uint32_t *outPayloadLen);

}  // namespace urma_mock
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_URMA_MOCK_TRANSPORT_UDS_TRANSPORT_H
