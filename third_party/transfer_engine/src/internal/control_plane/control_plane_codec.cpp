#include "internal/control_plane/control_plane_codec.h"

#include <arpa/inet.h>

#include <cstring>

namespace datasystem {
namespace {

uint64_t HostToBe64(uint64_t value)
{
    const uint32_t hi = htonl(static_cast<uint32_t>(value >> 32));
    const uint32_t lo = htonl(static_cast<uint32_t>(value & 0xffffffffULL));
    return (static_cast<uint64_t>(lo) << 32) | hi;
}

uint64_t Be64ToHost(uint64_t value)
{
    const uint32_t hi = ntohl(static_cast<uint32_t>(value >> 32));
    const uint32_t lo = ntohl(static_cast<uint32_t>(value & 0xffffffffULL));
    return (static_cast<uint64_t>(lo) << 32) | hi;
}

void AppendU32(std::vector<uint8_t> *buf, uint32_t v)
{
    const uint32_t be = htonl(v);
    const auto *p = reinterpret_cast<const uint8_t *>(&be);
    buf->insert(buf->end(), p, p + sizeof(be));
}

void AppendI32(std::vector<uint8_t> *buf, int32_t v)
{
    AppendU32(buf, static_cast<uint32_t>(v));
}

void AppendU64(std::vector<uint8_t> *buf, uint64_t v)
{
    const uint64_t be = HostToBe64(v);
    const auto *p = reinterpret_cast<const uint8_t *>(&be);
    buf->insert(buf->end(), p, p + sizeof(be));
}

void AppendBool(std::vector<uint8_t> *buf, bool v)
{
    buf->push_back(v ? 1 : 0);
}

void AppendString(std::vector<uint8_t> *buf, const std::string &v)
{
    AppendU32(buf, static_cast<uint32_t>(v.size()));
    buf->insert(buf->end(), v.begin(), v.end());
}

bool ReadU32(const std::vector<uint8_t> &buf, size_t *off, uint32_t *v)
{
    if (*off + sizeof(uint32_t) > buf.size()) {
        return false;
    }
    uint32_t tmp = 0;
    std::memcpy(&tmp, buf.data() + *off, sizeof(tmp));
    *v = ntohl(tmp);
    *off += sizeof(tmp);
    return true;
}

bool ReadI32(const std::vector<uint8_t> &buf, size_t *off, int32_t *v)
{
    uint32_t tmp = 0;
    if (!ReadU32(buf, off, &tmp)) {
        return false;
    }
    *v = static_cast<int32_t>(tmp);
    return true;
}

bool ReadU64(const std::vector<uint8_t> &buf, size_t *off, uint64_t *v)
{
    if (*off + sizeof(uint64_t) > buf.size()) {
        return false;
    }
    uint64_t tmp = 0;
    std::memcpy(&tmp, buf.data() + *off, sizeof(tmp));
    *v = Be64ToHost(tmp);
    *off += sizeof(tmp);
    return true;
}

bool ReadBool(const std::vector<uint8_t> &buf, size_t *off, bool *v)
{
    if (*off + 1 > buf.size()) {
        return false;
    }
    *v = (buf[*off] != 0);
    *off += 1;
    return true;
}

bool ReadString(const std::vector<uint8_t> &buf, size_t *off, std::string *v)
{
    uint32_t len = 0;
    if (!ReadU32(buf, off, &len)) {
        return false;
    }
    if (*off + len > buf.size()) {
        return false;
    }
    v->assign(reinterpret_cast<const char *>(buf.data() + *off), len);
    *off += len;
    return true;
}

}  // namespace

std::vector<uint8_t> EncodeExchangeReq(const ExchangeRootInfoRequest &req)
{
    std::vector<uint8_t> out;
    AppendString(&out, req.requesterHost);
    AppendU32(&out, req.requesterPort);
    AppendI32(&out, req.requesterDeviceId);
    AppendI32(&out, req.ownerDeviceId);
    AppendString(&out, req.rootInfo);
    return out;
}

bool DecodeExchangeReq(const std::vector<uint8_t> &in, ExchangeRootInfoRequest *req)
{
    size_t off = 0;
    return ReadString(in, &off, &req->requesterHost) && ReadU32(in, &off, &req->requesterPort) &&
           ReadI32(in, &off, &req->requesterDeviceId) && ReadI32(in, &off, &req->ownerDeviceId) &&
           ReadString(in, &off, &req->rootInfo) && off == in.size();
}

std::vector<uint8_t> EncodeExchangeRsp(const ExchangeRootInfoResponse &rsp)
{
    std::vector<uint8_t> out;
    AppendI32(&out, rsp.code);
    AppendString(&out, rsp.msg);
    AppendI32(&out, rsp.ownerDeviceId);
    return out;
}

bool DecodeExchangeRsp(const std::vector<uint8_t> &in, ExchangeRootInfoResponse *rsp)
{
    size_t off = 0;
    return ReadI32(in, &off, &rsp->code) && ReadString(in, &off, &rsp->msg) &&
           ReadI32(in, &off, &rsp->ownerDeviceId) && off == in.size();
}

std::vector<uint8_t> EncodeQueryReq(const QueryConnReadyRequest &req)
{
    std::vector<uint8_t> out;
    AppendString(&out, req.requesterHost);
    AppendU32(&out, req.requesterPort);
    AppendI32(&out, req.requesterDeviceId);
    AppendI32(&out, req.ownerDeviceId);
    return out;
}

bool DecodeQueryReq(const std::vector<uint8_t> &in, QueryConnReadyRequest *req)
{
    size_t off = 0;
    return ReadString(in, &off, &req->requesterHost) && ReadU32(in, &off, &req->requesterPort) &&
           ReadI32(in, &off, &req->requesterDeviceId) && ReadI32(in, &off, &req->ownerDeviceId) && off == in.size();
}

std::vector<uint8_t> EncodeQueryRsp(const QueryConnReadyResponse &rsp)
{
    std::vector<uint8_t> out;
    AppendI32(&out, rsp.code);
    AppendString(&out, rsp.msg);
    AppendBool(&out, rsp.ready);
    return out;
}

bool DecodeQueryRsp(const std::vector<uint8_t> &in, QueryConnReadyResponse *rsp)
{
    size_t off = 0;
    return ReadI32(in, &off, &rsp->code) && ReadString(in, &off, &rsp->msg) && ReadBool(in, &off, &rsp->ready) &&
           off == in.size();
}

std::vector<uint8_t> EncodeReadReq(const ReadTriggerRequest &req)
{
    std::vector<uint8_t> out;
    AppendU64(&out, req.requestId);
    AppendString(&out, req.requesterHost);
    AppendU32(&out, req.requesterPort);
    AppendI32(&out, req.requesterDeviceId);
    AppendI32(&out, req.ownerDeviceId);
    AppendU64(&out, req.remoteAddr);
    AppendU64(&out, req.length);
    return out;
}

bool DecodeReadReq(const std::vector<uint8_t> &in, ReadTriggerRequest *req)
{
    size_t off = 0;
    return ReadU64(in, &off, &req->requestId) && ReadString(in, &off, &req->requesterHost) &&
           ReadU32(in, &off, &req->requesterPort) && ReadI32(in, &off, &req->requesterDeviceId) &&
           ReadI32(in, &off, &req->ownerDeviceId) && ReadU64(in, &off, &req->remoteAddr) &&
           ReadU64(in, &off, &req->length) && off == in.size();
}

std::vector<uint8_t> EncodeReadRsp(const ReadTriggerResponse &rsp)
{
    std::vector<uint8_t> out;
    AppendI32(&out, rsp.code);
    AppendString(&out, rsp.msg);
    return out;
}

bool DecodeReadRsp(const std::vector<uint8_t> &in, ReadTriggerResponse *rsp)
{
    size_t off = 0;
    return ReadI32(in, &off, &rsp->code) && ReadString(in, &off, &rsp->msg) && off == in.size();
}

std::vector<uint8_t> EncodeBatchReadReq(const BatchReadTriggerRequest &req)
{
    std::vector<uint8_t> out;
    AppendString(&out, req.requesterHost);
    AppendU32(&out, req.requesterPort);
    AppendI32(&out, req.requesterDeviceId);
    AppendI32(&out, req.ownerDeviceId);
    AppendU32(&out, static_cast<uint32_t>(req.items.size()));
    for (const auto &item : req.items) {
        AppendU64(&out, item.requestId);
        AppendU64(&out, item.remoteAddr);
        AppendU64(&out, item.length);
    }
    return out;
}

bool DecodeBatchReadReq(const std::vector<uint8_t> &in, BatchReadTriggerRequest *req)
{
    size_t off = 0;
    uint32_t itemCount = 0;
    if (!(ReadString(in, &off, &req->requesterHost) && ReadU32(in, &off, &req->requesterPort) &&
          ReadI32(in, &off, &req->requesterDeviceId) && ReadI32(in, &off, &req->ownerDeviceId) &&
          ReadU32(in, &off, &itemCount))) {
        return false;
    }
    req->items.clear();
    req->items.reserve(itemCount);
    for (uint32_t i = 0; i < itemCount; ++i) {
        BatchReadItem item;
        if (!(ReadU64(in, &off, &item.requestId) && ReadU64(in, &off, &item.remoteAddr) &&
              ReadU64(in, &off, &item.length))) {
            return false;
        }
        req->items.push_back(item);
    }
    return off == in.size();
}

std::vector<uint8_t> EncodeBatchReadRsp(const BatchReadTriggerResponse &rsp)
{
    std::vector<uint8_t> out;
    AppendI32(&out, rsp.code);
    AppendString(&out, rsp.msg);
    AppendI32(&out, rsp.failedItemIndex);
    return out;
}

bool DecodeBatchReadRsp(const std::vector<uint8_t> &in, BatchReadTriggerResponse *rsp)
{
    size_t off = 0;
    return ReadI32(in, &off, &rsp->code) && ReadString(in, &off, &rsp->msg) &&
           ReadI32(in, &off, &rsp->failedItemIndex) && off == in.size();
}

Status MakeServerErrorPayload(const std::string &msg, std::vector<uint8_t> *payload)
{
    ReadTriggerResponse rsp;
    rsp.code = static_cast<int32_t>(StatusCode::kRuntimeError);
    rsp.msg = msg;
    *payload = EncodeReadRsp(rsp);
    return Status::OK();
}

}  // namespace datasystem
