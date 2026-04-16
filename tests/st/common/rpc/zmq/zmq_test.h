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
 * Description: Test library
 */
#ifndef DATASYSTEM_COMMON_RPC_ZMQ_DEMO_H
#define DATASYSTEM_COMMON_RPC_ZMQ_DEMO_H

#include "datasystem/protos/zmq_test.service.rpc.pb.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/utils/status.h"

namespace arbitrary {
namespace workspace {

// CRC-32 (IEEE 802.3) reflected polynomial; used by bit-serial CalcCrc32 below.
static constexpr uint32_t CRC32_POLYNOMIAL = 0xEDB88320U;

inline uint32_t CalcCrc32(const void *buf, size_t sz, uint32_t checksumbase = 0)
{
    uint32_t crc = checksumbase;
    const auto *bytes = reinterpret_cast<const uint8_t *>(buf);
    for (size_t i = 0; i < sz; ++i) {
        crc ^= bytes[i];
        for (int bit = 0; bit < 8; ++bit) {
            crc = (crc & 1U) ? (crc >> 1U) ^ CRC32_POLYNOMIAL : (crc >> 1U);
        }
    }
    return crc;
}
/**
 * This are demo service classes to implement the DemoService service in datasystem/protos/zmq_test.proto
 */
class SimpleServiceImpl final : public SimpleService {
public:
    Status HelloWorld(const SayHelloPb &pb, ReplyHelloPb &helloPb) override
    {
        LOG(INFO) << "Server received " << pb.msg();
        helloPb.set_reply("World");
        return datasystem::Status::OK();
    }
};

class MTPServiceImpl final : public MTPService {
public:
    constexpr int static TWO = 2;
    Status HelloWorld3(
        std::shared_ptr<::datasystem::ServerUnaryWriterReader<ReplyHelloPb, SayHelloPb>> serverApi) override
    {
        static int val = 0;
        SayHelloPb rq;
        ReplyHelloPb reply;
        auto rc = serverApi->Read(rq);
        if (rc.IsError()) {
            return rc;
        }
        reply.set_reply(rq.msg());
        rc = serverApi->Write(reply);
        if (rc.IsError()) {
            return rc;
        }
        std::vector<datasystem::RpcMessage> payload;
        datasystem::RpcMessage msg;
        msg.CopyString("ABCDEF");
        payload.push_back(std::move(msg));
        rc = serverApi->SendAndTagPayload(payload, (val++) % TWO == 0);
        if (rc.IsError()) {
            return rc;
        }
        return datasystem::Status::OK();
    }

    Status HelloWorld4(
        std::shared_ptr<::datasystem::ServerUnaryWriterReader<ReplyHelloPb, SayHelloPb>> serverApi) override
    {
        static int val = 0;
        SayHelloPb rq;
        ReplyHelloPb reply;
        auto rc = serverApi->Read(rq);
        if (rc.IsError()) {
            return rc;
        }
        std::vector<datasystem::RpcMessage> payload;
        rc = serverApi->ReceivePayload(payload);
        if (rc.IsError()) {
            return rc;
        }
        reply.set_reply(rq.msg());
        rc = serverApi->Write(reply);
        if (rc.IsError()) {
            return rc;
        }
        rc = serverApi->SendAndTagPayload(payload, (val++) % TWO == 0);
        if (rc.IsError()) {
            return rc;
        }
        return datasystem::Status::OK();
    }
};
class DemoServiceImpl final : public DemoService {
public:
    DemoServiceImpl() = default;
    ~DemoServiceImpl() override = default;

    /**
     * This implement the SimpleGreeting method.
     * @param hello
     * @param ReplyHelloPb
     * @return
     */
    datasystem::Status SimpleGreeting(const SayHelloPb &hello, ReplyHelloPb &ReplyHelloPb) override
    {
        datasystem::Status rc;
        if (hello.msg() == "Hello") {
            ReplyHelloPb.set_reply("World");
        } else if (hello.msg() == "World") {
            std::cout << "Sleep (slow path). Client will timeout" << std::endl;
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            ReplyHelloPb.set_reply("Hello");
        } else {
            ReplyHelloPb.set_reply(hello.msg());
        }
        return datasystem::Status::OK();
    }

    /**
     * This implements the ClientStreamGreeting method. Client side is streaming.
     * @param reader
     * @param pb
     * @return
     */

    datasystem::Status ClientStreamGreeting(std::shared_ptr<datasystem::ServerReader<SayHelloPb>> reader,
                                            ReplyHelloPb &pb) override
    {
        datasystem::Status rc;
        SayHelloPb rq;
        int i = 0;
        while ((rc = reader->Read(rq)).IsOk()) {
            ++i;
        }
        if (rc.GetCode() != datasystem::StatusCode(datasystem::K_RPC_STREAM_END)) {
            return rc;
        }
        std::cout << "Replying " << i << std::endl;
        pb.set_reply(std::to_string(i));
        return datasystem::Status::OK();
    }

    /**
     * This implements the ServerStreamingGreeting. Server side is streaming.
     * @param writer
     * @param pb
     * @return
     */
    datasystem::Status ServerStreamGreeting(std::shared_ptr<datasystem::ServerWriter<ReplyHelloPb>> writer,
                                            SayHelloPb &pb) override
    {
        ReplyHelloPb reply;
        const int numIter = 5;
        for (auto i = 0; i < numIter; ++i) {
            reply.set_reply("I replied \"" + pb.msg() + "\" 5 times");
            writer->Write(reply);
        }
        writer->Finish();
        return datasystem::Status::OK();
    }

    /**
     * This implements the StreamGreeting method. Both sides are streaming.
     * @param stream
     * @return
     */
    datasystem::Status StreamGreeting(
        std::shared_ptr<::datasystem::ServerWriterReader<ReplyHelloPb, SayHelloPb>> stream) override
    {
        using datasystem::Status;
        Status rc;
        SayHelloPb rq;
        int count = 0;
        while ((rc = stream->Read(rq)).IsOk()) {
            ++count;
            ReplyHelloPb rsp;
            rsp.set_reply(rq.msg());
            rc = stream->Write(rsp);
            if (rc.IsError()) {
                break;
            }
            rq.Clear();
        }
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(count >= 1, datasystem::K_RUNTIME_ERROR, "rc = " + rc.ToString());
        if (rc.GetCode() != datasystem::StatusCode(datasystem::K_RPC_STREAM_END)) {
            return rc;
        }
        stream->Finish();
        return Status::OK();
    }

    /**
     * This implements the SendBigBuf method with custom option send_payload_option
     */
    datasystem::Status SendBigBuf(const SayHelloPb &hello, ChecksumPb &checksum,
                                  std::vector<datasystem::RpcMessage> payload) override
    {
        (void)hello;
        uint32_t crc32 = 0;
        size_t payload_len = 0;
        for (auto &msg : payload) {
            payload_len += msg.Size();
        }
        if (payload_len > 0) {
            auto buf = std::make_unique<char[]>(payload_len);
            char *p = buf.get();
            size_t size = payload_len;
            for (auto &msg : payload) {
                if (memcpy_s(p, size, msg.Data(), msg.Size()) != EOK) {
                    RETURN_STATUS(datasystem::StatusCode::K_RUNTIME_ERROR, "memcpy msg failed");
                }
                p += msg.Size();
                size -= msg.Size();
            }
            crc32 = CalcCrc32(buf.get(), static_cast<int64_t>(payload_len));
        }
        checksum.set_crc32(crc32);
        return datasystem::Status::OK();
    }

    /**
     * This implements the RecvBigBuf method with custom option recv_payload_option
     */
    datasystem::Status RecvBigBuf(const SayHelloPb &pb, ChecksumPb &checksumPb,
                                  std::vector<datasystem::RpcMessage> &outPayload) override
    {
        (void)pb;
        const int64_t len = 4096;
        datasystem::RpcMessage buffer;
        RETURN_IF_NOT_OK(buffer.AllocMem(len));
        std::ifstream ifs("/dev/urandom", std::ios_base::in | std::ios_base::binary);
        ifs.read(static_cast<char *>(buffer.Data()), len);
        ifs.close();
        uint32_t checksum = CalcCrc32(buffer.Data(), len);
        checksumPb.set_crc32(checksum);
        outPayload.push_back(std::move(buffer));
        return datasystem::Status();
    }

    /**
     * Implement the StreamSendBigBuf method with payload and stream option set.
     */
    datasystem::Status StreamSendBigBuf(
        std::shared_ptr<::datasystem::ServerWriterReader<ChecksumPb, SayHelloPb>> stream) override
    {
        SayHelloPb rq;
        datasystem::Status rc;
        while ((rc = stream->Read(rq)).IsOk()) {
            std::vector<datasystem::RpcMessage> payload;
            rc = stream->ReceivePayload(payload);
            if (rc.IsError()) {
                return rc;
            }
            ChecksumPb reply;
            rc = SendBigBuf(rq, reply, std::move(payload));
            if (rc.IsError()) {
                return rc;
            }
            rc = stream->Write(reply);
            if (rc.IsError()) {
                return rc;
            }
            rq.Clear();
        }
        if (rc.GetCode() != datasystem::StatusCode(datasystem::K_RPC_STREAM_END)) {
            return rc;
        }
        return stream->Finish();
    }

    datasystem::Status StreamRecvBigBuf(
        std::shared_ptr<::datasystem::ServerWriterReader<ChecksumPb, SayHelloPb>> stream) override
    {
        SayHelloPb rq;
        datasystem::Status rc;
        while ((rc = stream->Read(rq)).IsOk()) {
            std::vector<datasystem::RpcMessage> payload;
            ChecksumPb reply;
            rc = RecvBigBuf(rq, reply, payload);
            if (rc.IsError()) {
                return rc;
            }
            rc = stream->Write(reply);
            if (rc.IsError()) {
                return rc;
            }
            rc = stream->SendPayload(payload);
            if (rc.IsError()) {
                return rc;
            }
            rq.Clear();
        }
        if (rc.GetCode() != datasystem::StatusCode(datasystem::K_RPC_STREAM_END)) {
            return rc;
        }
        return stream->Finish();
    }

    datasystem::Status ServerStreamWithPayload(std::shared_ptr<::datasystem::ServerWriter<ReplyHelloPb>> writer,
                                               SayHelloPb &pb, std::vector<datasystem::RpcMessage> payload) override
    {
        (void)pb;
        ReplyHelloPb reply;
        size_t sz = 0;
        for (auto &msg : payload) {
            sz += msg.Size();
        }
        const int numIter = 5;
        for (auto i = 0; i < numIter; ++i) {
            reply.set_reply("The payload size is " + std::to_string(sz));
            writer->Write(reply);
        }
        writer->Finish();
        return datasystem::Status();
    }

    datasystem::Status ClientStreamWithPayload(std::shared_ptr<::datasystem::ServerReader<SayHelloPb>> reader,
                                               ReplyHelloPb &pb,
                                               std::vector<datasystem::RpcMessage> &outPayload) override
    {
        datasystem::Status rc;
        SayHelloPb rq;
        int i = 0;
        while ((rc = reader->Read(rq)).IsOk()) {
            ++i;
        }
        if (rc.GetCode() != datasystem::StatusCode(datasystem::K_RPC_STREAM_END)) {
            return rc;
        }
        std::cout << "Replying " << i << std::endl;
        pb.set_reply(std::to_string(i));
        datasystem::RpcMessage buf;
        RETURN_IF_NOT_OK(buf.CopyBuffer("ABC", strlen("ABC")));
        outPayload.push_back(std::move(buf));
        return datasystem::Status();
    }

    /**
     * This implements the UnarySocketSimpleGreeting method.
     * @param stream
     * @return
     */
    datasystem::Status UnarySocketSimpleGreeting(
        std::shared_ptr<::datasystem::ServerUnaryWriterReader<ReplyHelloPb, SayHelloPb>> serverApi) override
    {
        using datasystem::Status;
        SayHelloPb rq;
        RETURN_IF_NOT_OK(serverApi->Read(rq));
        ReplyHelloPb rsp;
        rsp.set_reply(rq.msg());
        RETURN_IF_NOT_OK(serverApi->Write(rsp));
        return Status::OK();
    }

    datasystem::Status UnarySocketSendPayload(
        std::shared_ptr<::datasystem::ServerUnaryWriterReader<ChecksumPb, SayHelloPb>> serverApi) override
    {
        using datasystem::Status;
        SayHelloPb rq;
        RETURN_IF_NOT_OK(serverApi->Read(rq));
        std::vector<datasystem::RpcMessage> payload;
        RETURN_IF_NOT_OK(serverApi->ReceivePayload(payload));
        ChecksumPb reply;
        RETURN_IF_NOT_OK(SendBigBuf(rq, reply, std::move(payload)));
        RETURN_IF_NOT_OK(serverApi->Write(reply));
        return Status::OK();
    }

    datasystem::Status UnarySocketRecvPayload(
        std::shared_ptr<::datasystem::ServerUnaryWriterReader<ChecksumPb, SayHelloPb>> serverApi) override
    {
        using datasystem::Status;
        SayHelloPb rq;
        RETURN_IF_NOT_OK(serverApi->Read(rq));
        std::vector<datasystem::RpcMessage> payload;
        ChecksumPb reply;
        RETURN_IF_NOT_OK(RecvBigBuf(rq, reply, payload));
        RETURN_IF_NOT_OK(serverApi->Write(reply));
        RETURN_IF_NOT_OK(serverApi->SendPayload(payload));
        return datasystem::Status::OK();
    }

    datasystem::Status UnarySocketSimpleError(
        std::shared_ptr<::datasystem::ServerUnaryWriterReader<ReplyHelloPb, SayHelloPb>> serverApi) override
    {
        using datasystem::Status;
        (void)serverApi;
        RETURN_STATUS(datasystem::StatusCode::K_RUNTIME_ERROR, "Just an error test.");
    }

    Status UnarySocketLargePayload(
        std::shared_ptr<::datasystem::ServerUnaryWriterReader<ChecksumPb, SayHelloPb>> serverApi) override
    {
        SayHelloPb rq;
        RETURN_IF_NOT_OK(serverApi->Read(rq));
        std::vector<datasystem::RpcMessage> payload;
        RETURN_IF_NOT_OK(serverApi->ReceivePayload(payload));
        ChecksumPb reply;
        uint32_t checksum = 0;
        for (auto &buf : payload) {
            checksum = CalcCrc32(buf.Data(), buf.Size(), checksum);
        }
        reply.set_crc32(checksum);
        RETURN_IF_NOT_OK(serverApi->Write(reply));
        return Status::OK();
    }

private:
};
}  // namespace workspace
}  // namespace arbitrary

#endif  // DATASYSTEM_COMMON_RPC_ZMQ_DEMO_H
