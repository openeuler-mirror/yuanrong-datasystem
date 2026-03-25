#ifndef TRANSFER_ENGINE_INTERNAL_CONTROL_PLANE_CODEC_H
#define TRANSFER_ENGINE_INTERNAL_CONTROL_PLANE_CODEC_H

#include <cstdint>
#include <string>
#include <vector>

#include "datasystem/transfer_engine/control_plane_messages.h"
#include "datasystem/transfer_engine/status.h"

namespace datasystem {

enum class RpcMethod : uint8_t {
    kExchangeRootInfo = 1,
    kQueryConnReady = 2,
    kReadTrigger = 3,
    kBatchReadTrigger = 4,
};

std::vector<uint8_t> EncodeExchangeReq(const ExchangeRootInfoRequest &req);
bool DecodeExchangeReq(const std::vector<uint8_t> &in, ExchangeRootInfoRequest *req);
std::vector<uint8_t> EncodeExchangeRsp(const ExchangeRootInfoResponse &rsp);
bool DecodeExchangeRsp(const std::vector<uint8_t> &in, ExchangeRootInfoResponse *rsp);

std::vector<uint8_t> EncodeQueryReq(const QueryConnReadyRequest &req);
bool DecodeQueryReq(const std::vector<uint8_t> &in, QueryConnReadyRequest *req);
std::vector<uint8_t> EncodeQueryRsp(const QueryConnReadyResponse &rsp);
bool DecodeQueryRsp(const std::vector<uint8_t> &in, QueryConnReadyResponse *rsp);

std::vector<uint8_t> EncodeReadReq(const ReadTriggerRequest &req);
bool DecodeReadReq(const std::vector<uint8_t> &in, ReadTriggerRequest *req);
std::vector<uint8_t> EncodeReadRsp(const ReadTriggerResponse &rsp);
bool DecodeReadRsp(const std::vector<uint8_t> &in, ReadTriggerResponse *rsp);

std::vector<uint8_t> EncodeBatchReadReq(const BatchReadTriggerRequest &req);
bool DecodeBatchReadReq(const std::vector<uint8_t> &in, BatchReadTriggerRequest *req);
std::vector<uint8_t> EncodeBatchReadRsp(const BatchReadTriggerResponse &rsp);
bool DecodeBatchReadRsp(const std::vector<uint8_t> &in, BatchReadTriggerResponse *rsp);

Status MakeServerErrorPayload(const std::string &msg, std::vector<uint8_t> *payload);

}  // namespace datasystem

#endif  // TRANSFER_ENGINE_INTERNAL_CONTROL_PLANE_CODEC_H
