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
 * Description: Wrapper for zmq message
 */
#include "datasystem/common/rpc/zmq/zmq_message.h"

#include <cstring>
#include "securec.h"

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
ZmqMessage::ZmqMessage() : flag_(ZmqMsgType::NONE)
{
    // zmq_msg_init() always return 0 according to ZMQ manual
    (void)zmq_msg_init(&msg_);
}

Status ZmqMessage::Close()
{
    int rc = zmq_msg_close(&msg_);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(rc == 0, K_RUNTIME_ERROR,
                                         FormatString("Unable to close msg_t: %s", zmq_strerror(errno)));
    return Status::OK();
}

ZmqMessage::~ZmqMessage()
{
    (void)Close();
}

Status ZmqMessage::AllocMem(size_t size)
{
    int rc = zmq_msg_init_size(&msg_, size);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        rc == 0, K_RUNTIME_ERROR, FormatString("Unable to create msg_t of size %zu: %s", size, zmq_strerror(errno)));
    return Status::OK();
}

Status ZmqMessage::TransferOwnership(void *data, size_t size, zmq_free_fn *ffn, void *hint)
{
    int rc = zmq_msg_init_data(&msg_, data, size, ffn, hint);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(rc == 0, K_RUNTIME_ERROR,
                                         FormatString("Unable to create msg_t: %s", zmq_strerror(errno)));
    return Status::OK();
}

Status ZmqMessage::CopyBuffer(const void *data, size_t size)
{
    RETURN_OK_IF_TRUE(size == 0);
    RETURN_IF_NOT_OK(AllocMem(size));
    int rc = memcpy_s(zmq_msg_data(&msg_), zmq_msg_size(&msg_), data, size);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        rc == 0, K_RUNTIME_ERROR,
        FormatString("Unable to copy %zu bytes into msg_t. rc = %d errno = %d", size, rc, errno));
    return Status::OK();
}

Status ZmqMessage::Move(ZmqMessage &src)
{
    int rc = zmq_msg_move(&msg_, &src.msg_);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(rc == 0, K_RUNTIME_ERROR,
                                         FormatString("Unable to move into msg_t: %s", zmq_strerror(errno)));
    return Status::OK();
}

Status ZmqMessage::Copy(ZmqMessage &src)
{
    int rc = zmq_msg_copy(&msg_, &src.msg_);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(rc == 0, K_RUNTIME_ERROR,
                                         FormatString("Unable to copy into msg_t: %s", zmq_strerror(errno)));
    return Status::OK();
}

std::string ZmqMessage::DebugString() const
{
    std::stringstream os;
    const size_t maxDumpSz = 256;
    const auto size = std::min<size_t>(Size(), maxDumpSz);
    os << "ZmqMessage [size " << std::dec << std::setw(3) << std::setfill('0') << Size() << "] ";
    if (Size() > maxDumpSz) {
        os << "(Dumping the first " << size << " number of bytes) : ";
    }
    os << "(";
    const char *p = reinterpret_cast<const char *>(Data());
    uint8_t prev = 0;
    for (size_t i = 0; i < size; ++i) {
        auto byte = p[i];
        if (std::isprint(byte)) {
            if (!std::isprint(prev)) {
                os << " ";
            }
            os << byte;
        } else {
            if (std::isprint(prev)) {
                os << " ";
            }
            os << std::hex << std::uppercase << std::setw(2) << std::setfill('0') << static_cast<short>(byte);
        }
        prev = byte;
    }
    os << ")";
    return os.str();
}

bool ZmqMessage::operator==(const ZmqMessage &other) const
{
    const auto sz = Size();
    return sz == other.Size() && 0 == memcmp(Data(), other.Data(), sz);
}
}  // namespace datasystem
