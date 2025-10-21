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
 * Description: Zmq RPC module utilities.
 */
#ifndef DATASYSTEM_COMMON_RPC_ZMQ_COMMON_H
#define DATASYSTEM_COMMON_RPC_ZMQ_COMMON_H

#include <deque>
#include <iostream>
#include <optional>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <zmq.h>

#include "datasystem/common/log/log.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/rpc/unix_sock_fd.h"
#include "datasystem/common/rpc/zmq/zmq_constants.h"
#include "datasystem/common/rpc/zmq/zmq_context.h"
#include "datasystem/common/rpc/zmq/zmq_message.h"
#include "datasystem/common/rpc/mem_view.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/protos/meta_zmq.pb.h"
#include "datasystem/protos/rpc_option.pb.h"
#include "datasystem/protos/utils.pb.h"

namespace datasystem {
/**
 * A holder for ZmqCurveUserId checking.
 */
struct ZmqCurveUserId {
    bool checkUserId_ = false;
    std::unique_ptr<char[]> userId_ = nullptr;
};

typedef std::deque<ZmqMessage> ZmqMsgFrames;
typedef std::deque<ZmqMessage> &ZmqMsgFramesRef;
typedef std::pair<MetaPb, ZmqMsgFrames> ZmqMetaMsgFrames;
typedef std::pair<MetaPb, ZmqMsgFrames> &ZmqMetaMsgFramesRef;

/**
 * @brief Parse a ZMQ message and convert it into protobuf.
 * @tparam T RespPb Type.
 * @param[in] msg ZMQ message.
 * @param[out] pb Protobuf.
 * @return Status of call.
 */
template <typename T>
inline Status ParseFromZmqMessage(const ZmqMessage &msg, T &pb)
{
    PerfPoint point(PerfKey::ZMQ_COM_PARSE_FROM_ZMQ_MESSAGE);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Validator::IsInNonNegativeInt32(msg.Size()), K_INVALID, "Parse out of range.");
    bool rc = pb.ParseFromArray(msg.Data(), msg.Size());
    point.Record();
    RETURN_OK_IF_TRUE(rc);
    const google::protobuf::Descriptor *descriptor = pb.GetDescriptor();
    LOG(WARNING) << "Parse from message " << msg << " into protobuf " << descriptor->full_name() << " unsuccessful.";
    RETURN_STATUS(StatusCode::K_INVALID, "ParseFromZmqMessage failed.");
}

/**
 * @brief This inline convert a protobuf into a ZMQ message.
 * @tparam T ReqPb Type.
 * @param[in] pb Source of the protobuf.
 * @param[out] dest Destination in the form of ZmqMessage.
 * @return Status of the call.
 */
template <typename T>
inline Status SerializeToZmqMessage(const T &pb, ZmqMessage &dest)
{
    PerfPoint point(PerfKey::ZMQ_COM_SERIAL_TO_ZMQ_MESSAGE);
    auto sz = pb.ByteSizeLong();
    RETURN_IF_NOT_OK(dest.AllocMem(sz));
    auto *p = dest.Data();
    bool rc = pb.SerializeToArray(p, sz);
    CHECK_FAIL_RETURN_STATUS(rc, K_RUNTIME_ERROR, "Serialization error");
    point.Record();
    return Status::OK();
}

/**
 * @brief Push proto buffer to the back of ZmqMessage frames.
 * @tparam T Type of protobuffer.
 * @param[in] pb Reference to proto buffer.
 * @param[out] frames Frames to push to.
 * @return StatusCode::K_Ok if successful.
 *         StatusCode::K_RUNTIME_ERROR if Serialization of protobuffer fails.
 */
template <typename T>
inline Status PushBackProtobufToFrames(const T &pb, ZmqMsgFrames &frames)
{
    ZmqMessage msg;
    RETURN_IF_NOT_OK(SerializeToZmqMessage(pb, msg));
    frames.push_back(std::move(msg));
    return Status::OK();
}

inline Status PushBackStringToFrames(const std::string &str, ZmqMsgFrames &frames)
{
    frames.emplace_back();
    auto &ele = frames.back();
    return ele.CopyString(str);
}

/**
 * @brief Push proto buffer to the front of ZmqMessage frames.
 * @tparam T Type of protobuffer.
 * @param[in] pb Reference to proto buffer.
 * @param[out] frames Frames to push to.
 * @return StatusCode::K_Ok if successful.
 *         StatusCode::K_RUNTIME_ERROR if Serialization of protobuffer fails.
 */
template <typename T>
inline Status PushFrontProtobufToFrames(const T &pb, ZmqMsgFrames &frames)
{
    ZmqMessage msg;
    RETURN_IF_NOT_OK(SerializeToZmqMessage(pb, msg));
    frames.push_front(std::move(msg));
    return Status::OK();
}

inline Status PushFrontStringToFrames(const std::string &str, ZmqMsgFrames &frames)
{
    frames.emplace_front();
    auto &ele = frames.front();
    return ele.CopyString(str);
}

/**
 * @brief Convert a Status object into ZMQ message.
 * @param[in] rc Status code.
 * @return ZMQ message.
 */
inline ZmqMessage StatusToZmqMessage(const Status &rc)
{
    PerfPoint point(PerfKey::ZMQ_COM_STATUS_TO_ZMQ_MESSAGE);
    ZmqMessage errorMsg;
    ErrorInfoPb err;
    err.set_error_code(rc.GetCode());
    err.set_error_msg(rc.GetMsg());
    Status tmpRc = SerializeToZmqMessage<ErrorInfoPb>(err, errorMsg);
    if (tmpRc.IsError()) {
        LOG(ERROR) << "SerializeToZmqMessage Fail";
    }
    return errorMsg;
}

/**
 * @brief Convert a ZMQ message into a Status object.
 * @note If the ZMQ message can't be parsed, other error can be returned.
 * @param[in] errMsg Error message buffer.
 * @return Status object.
 */
inline Status ZmqMessageToStatus(const ZmqMessage &errMsg)
{
    PerfPoint point(PerfKey::ZMQ_COM_ZMQ_MESSAGE_TO_STATUS);
    ErrorInfoPb err;
    RETURN_IF_NOT_OK(ParseFromZmqMessage(errMsg, err));
    Status rc(static_cast<StatusCode>(err.error_code()), err.error_msg());
    point.Record();
    return rc;
}

/**
 * @brief Copy the content of a ZMQ message into a string.
 * @param[in] msg Zmq message buffer.
 * @return The content of a ZMQ message as string.
 */
inline std::string ZmqMessageToString(const ZmqMessage &msg)
{
    PerfPoint point(PerfKey::ZMQ_COM_ZMQ_MESSAGE_TO_STRING);
    std::string s(reinterpret_cast<const char *>(msg.Data()), msg.Size());
    point.Record();
    return s;
}

/**
 * @brief Interpret the content of a ZMQ message as a 64 bit integer.
 * @param[int] msg Zmq message buffer.
 * @param[out] val Parsed int64.
 * @return Status of call.
 */
inline Status ZmqMessageToInt64(const ZmqMessage &msg, int64_t &val)
{
    PerfPoint point(PerfKey::ZMQ_COM_ZMQ_MESSAGE_TO_INT64);
    CHECK_FAIL_RETURN_STATUS(msg.Size() == sizeof(int64_t), StatusCode::K_INVALID, "Not a 64-bit integer");
    google::protobuf::io::CodedInputStream input(reinterpret_cast<const uint8_t *>(msg.Data()),
                                                 static_cast<int>(msg.Size()));
    CHECK_FAIL_RETURN_STATUS(input.ReadLittleEndian64(reinterpret_cast<std::uint64_t *>(&val)), StatusCode::K_INVALID,
                             "Google read error");
    (void)val;
    point.Record();
    return Status::OK();
}

/**
 * @brief Copy a 64 bit integer into a ZMQ message.
 * @param val int64.
 * @return Serialized zmq buffer from int64 val.
 */
inline ZmqMessage ZmqInt64ToMessage(int64_t val)
{
    PerfPoint point(PerfKey::ZMQ_COM_ZMQ_INT64_TO_MESSAGE);
    static constexpr int32_t workAreaSz = sizeof(int64_t);
    char bodyLen[workAreaSz] = { 0 };
    {
        google::protobuf::io::ArrayOutputStream osWrapper(&bodyLen[0], workAreaSz, workAreaSz);
        google::protobuf::io::CodedOutputStream output(&osWrapper);
        output.WriteLittleEndian64(val);
    }
    ZmqMessage bodyLenMsg;
    Status tmpRc = bodyLenMsg.CopyBuffer(bodyLen, workAreaSz);
    if (tmpRc.IsError()) {
        LOG(ERROR) << "ZmqInt64ToMessage Fail";
    }
    point.Record();
    return bodyLenMsg;
}

/**
 * @brief Check if rc is a rpc error.
 * @param[in] status Status object.
 */
inline bool IsRpcError(const Status &status)
{
    if (status.GetCode() == StatusCode::K_RPC_CANCELLED || status.GetCode() == StatusCode::K_RPC_DEADLINE_EXCEEDED
        || status.GetCode() == StatusCode::K_RPC_UNAVAILABLE) {
        return true;
    }
    return false;
}

/**
 * @brief Parse an incoming messages into meta.
 * @param[in] frames Zmq message frames.
 * @param[out] meta Parsed metaPb structure.
 * @param[in] fd File descriptor.
 * @param[in] type Event type.
 */
Status ParseMsgFrames(ZmqMsgFrames &frames, MetaPb &meta, int fd, EventType type, ZmqCurveUserId &userId);

/**
 * @brief Acknowledge a protobuf request sent earlier and get is reply.
 * @details The server will always send back a Status object followed by the message body if
 * the Status object received is OK.
 * @param[in] frames Zmq message frames.
 * @param[out] reply Reply message buffer.
 * @return Status object.
 */
Status AckRequest(ZmqMsgFrames &frames, ZmqMessage &reply);

/**
 * @brief Encode a binary data into a z85-encoded message. See ZMQ RFC 32 for more details.
 * @param[in] inMsg Raw message to be encoded.
 * @param[out] outMsg Pointer where the encoded message will be saved.
 * @return Status of call.
 */
inline Status Z85Encode(ZmqMessage inMsg, ZmqMessage *outMsg)
{
    RETURN_RUNTIME_ERROR_IF_NULL(outMsg);
    CHECK_FAIL_RETURN_STATUS(
        inMsg.Size() == ZMQ_CURVE_KEY_SIZE, StatusCode::K_RUNTIME_ERROR,
        "Expect input size " + std::to_string(ZMQ_CURVE_KEY_SIZE) + ", got " + std::to_string(inMsg.Size()));
    RETURN_IF_NOT_OK(outMsg->AllocMem(ZMQ_ENCODE_KEY_SIZE_NUL_TERM));
    if (zmq_z85_encode(static_cast<char *>(outMsg->Data()), static_cast<uint8_t *>(inMsg.Data()), inMsg.Size())
        == nullptr) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Fail to encode data with Z85 encoding!");
    }
    return Status::OK();
}

/**
 * @brief Return time since epoch in nanoseconds.
 * @return uint64_t Time since epoch.
 */
inline uint64_t TimeSinceEpoch()
{
    return std::chrono::high_resolution_clock::now().time_since_epoch().count();
}

/**
 * @brief Start the clock in meta proto buffer.
 * @param meta The meta proto buffer reference.
 */
inline void StartTheClock(MetaPb &meta)
{
    // Start the clock
    meta.mutable_ticks()->Clear();
    TickPb tick;
    tick.set_ts(TimeSinceEpoch());
    tick.set_tick_name("META_TICK_START");
    meta.mutable_ticks()->Add(std::move(tick));
}

inline uint64_t GetLapTime(MetaPb &meta, const std::string &tickName)
{
    auto ts = TimeSinceEpoch();
    auto n = meta.ticks_size();
    // To make sure ticks is not empty before access
    uint64_t diff = n > 0 ? (ts - meta.ticks(n - 1).ts()) : 0;
    TickPb tick;
    tick.set_ts(ts);
    tick.set_tick_name(tickName);
    meta.mutable_ticks()->Add(std::move(tick));
    return diff;
}

inline uint64_t GetTotalTime(MetaPb &meta)
{
    auto n = meta.ticks_size();
    if (n > 0) {
        return meta.ticks(n - 1).ts() - meta.ticks(0).ts();
    }
    return 0;
}

inline void UpdateMetaByThreadLocalValue(MetaPb &meta)
{
    meta.set_trace_id(Trace::Instance().GetTraceID());
    meta.set_timeout(reqTimeoutDuration.CalcRealRemainingTime());
    meta.set_db_name(g_MetaRocksDbName);
    VLOG(RPC_LOG_LEVEL) << FormatString("Send message with timeout %zu and db name %s", meta.timeout(),
                                        g_MetaRocksDbName);
}

inline MetaPb CreateMetaData(const std::string &svcName, int32_t methodIndex, int64_t payloadIndex,
                             const std::string &clientId)
{
    MetaPb meta;
    meta.set_svc_name(svcName);
    meta.set_method_index(methodIndex);
    meta.set_payload_index(payloadIndex);
    meta.set_client_id(clientId);
    UpdateMetaByThreadLocalValue(meta);
    // Start the clock
    StartTheClock(meta);
    return meta;
}
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_ZMQ_COMMON_H
