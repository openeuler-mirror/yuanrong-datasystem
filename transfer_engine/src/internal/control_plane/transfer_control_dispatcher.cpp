#include "internal/control_plane/transfer_control_dispatcher.h"

#include <glog/logging.h>

#include "internal/log/environment_dump.h"
#include "datasystem/transfer_engine/status_helper.h"

namespace datasystem {

Result DispatchControlRequest(const std::shared_ptr<ITransferControlService> &service, RpcMethod method,
                              const std::vector<uint8_t> &reqPayload, RpcMethod *rspMethod,
                              std::vector<uint8_t> *rspPayload)
{
    TE_CHECK_PTR_OR_RETURN(rspMethod);
    TE_CHECK_PTR_OR_RETURN(rspPayload);
    TE_CHECK_OR_RETURN(service != nullptr, ErrorCode::kRuntimeError, "service is null");

    *rspMethod = method;
    if (method == RpcMethod::kExchangeRootInfo) {
        internal::DumpProcessEnvironment("dispatch_exchange_root_info");
        ExchangeRootInfoRequest req;
        ExchangeRootInfoResponse rsp;
        if (!DecodeExchangeReq(reqPayload, &req)) {
            rsp.code = static_cast<int32_t>(ErrorCode::kInvalid);
            rsp.msg = "decode exchange request failed";
            LOG(WARNING) << "dispatch exchange root info failed to decode request";
        } else {
            Result rc = service->ExchangeRootInfo(req, &rsp);
            if (rc.IsError()) {
                rsp.code = static_cast<int32_t>(rc.GetCode());
                rsp.msg = rc.GetMsg();
                LOG(ERROR) << "dispatch exchange root info failed, reason=" << rc.ToString();
            }
        }
        *rspPayload = EncodeExchangeRsp(rsp);
        return Result::OK();
    }

    if (method == RpcMethod::kQueryConnReady) {
        QueryConnReadyRequest req;
        QueryConnReadyResponse rsp;
        if (!DecodeQueryReq(reqPayload, &req)) {
            rsp.code = static_cast<int32_t>(ErrorCode::kInvalid);
            rsp.msg = "decode query request failed";
            rsp.ready = false;
            LOG(WARNING) << "dispatch query conn ready failed to decode request";
        } else {
            Result rc = service->QueryConnReady(req, &rsp);
            if (rc.IsError()) {
                rsp.code = static_cast<int32_t>(rc.GetCode());
                rsp.msg = rc.GetMsg();
                rsp.ready = false;
                LOG(ERROR) << "dispatch query conn ready failed, reason=" << rc.ToString();
            }
        }
        *rspPayload = EncodeQueryRsp(rsp);
        return Result::OK();
    }

    if (method == RpcMethod::kReadTrigger) {
        internal::DumpProcessEnvironment("dispatch_read_trigger");
        ReadTriggerRequest req;
        ReadTriggerResponse rsp;
        if (!DecodeReadReq(reqPayload, &req)) {
            rsp.code = static_cast<int32_t>(ErrorCode::kInvalid);
            rsp.msg = "decode read request failed";
            LOG(WARNING) << "dispatch read trigger failed to decode request";
        } else {
            Result rc = service->ReadTrigger(req, &rsp);
            if (rc.IsError()) {
                rsp.code = static_cast<int32_t>(rc.GetCode());
                rsp.msg = rc.GetMsg();
                LOG(ERROR) << "dispatch read trigger failed, reason=" << rc.ToString();
            }
        }
        *rspPayload = EncodeReadRsp(rsp);
        return Result::OK();
    }

    if (method == RpcMethod::kBatchReadTrigger) {
        internal::DumpProcessEnvironment("dispatch_batch_read_trigger");
        BatchReadTriggerRequest req;
        BatchReadTriggerResponse rsp;
        if (!DecodeBatchReadReq(reqPayload, &req)) {
            rsp.code = static_cast<int32_t>(ErrorCode::kInvalid);
            rsp.msg = "decode batch read request failed";
            rsp.failedItemIndex = -1;
            LOG(WARNING) << "dispatch batch read trigger failed to decode request";
        } else {
            Result rc = service->BatchReadTrigger(req, &rsp);
            if (rc.IsError()) {
                rsp.code = static_cast<int32_t>(rc.GetCode());
                rsp.msg = rc.GetMsg();
                rsp.failedItemIndex = -1;
                LOG(ERROR) << "dispatch batch read trigger failed, reason=" << rc.ToString();
            }
        }
        *rspPayload = EncodeBatchReadRsp(rsp);
        return Result::OK();
    }

    ReadTriggerResponse rsp;
    rsp.code = static_cast<int32_t>(ErrorCode::kInvalid);
    rsp.msg = "unknown rpc method";
    LOG(WARNING) << "dispatch unknown rpc method, method=" << static_cast<int>(method);
    *rspMethod = RpcMethod::kReadTrigger;
    *rspPayload = EncodeReadRsp(rsp);
    return Result::OK();
}

}  // namespace datasystem
