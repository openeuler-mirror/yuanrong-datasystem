#include "brpc_server.h"

#include "common/simple_log.h"
#include "kvtest_control.pb.h"

#include <butil/logging.h>
#include <brpc/closure_guard.h>
#include <brpc/server.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>

#include <string>
#include <utility>

namespace {
constexpr const char *kServiceFullName = "kvtest_control.KvtestControl";
constexpr const char *kNotify = "Notify";
constexpr const char *kStats = "Stats";
constexpr const char *kStop = "Stop";
constexpr const char *kSummary = "Summary";
}  // namespace

// KvtestControlServiceImpl: protobuf 28.x no longer emits a C++ Service base
// class even with cc_generic_services=true, so (like brpc_server_example) we
// inherit google::protobuf::Service directly and dispatch CallMethod by method
// name. The ServiceDescriptor is fetched once from the generated DescriptorPool
// (the .proto declares `service KvtestControl`, so the descriptor is present).
class BrpcControlServer::Impl : public google::protobuf::Service {
public:
    Impl(NotifyDispatcher *dispatcher, MetricsCollector *metrics, std::atomic<bool> *running)
        : dispatcher_(dispatcher), metrics_(metrics), running_(running) {
        descriptor_ = google::protobuf::DescriptorPool::generated_pool()->FindServiceByName(kServiceFullName);
        if (descriptor_ == nullptr) {
            LOG(ERROR) << "Failed to find ServiceDescriptor for " << kServiceFullName
                       << "; brpc::Server::AddService will not be able to dispatch";
        }
    }

    void CallMethod(const google::protobuf::MethodDescriptor *method,
                    google::protobuf::RpcController * /*controller*/,
                    const google::protobuf::Message *request,
                    google::protobuf::Message *response,
                    google::protobuf::Closure *done) override {
        brpc::ClosureGuard done_guard(done);

        if (method == nullptr) {
            LOG(ERROR) << "CallMethod: null method descriptor";
            return;
        }
        const std::string &name = method->name();
        if (name == kNotify) {
            auto *req = static_cast<const kvtest_control::NotifyReq *>(request);
            auto *resp = static_cast<kvtest_control::NotifyResp *>(response);
            std::vector<std::string> keys(req->keys().begin(), req->keys().end());
            dispatcher_->DispatchNotify(req->action(), req->sender(), keys, req->size());
            resp->set_ok(true);
            resp->set_message("ok");
        } else if (name == kStats) {
            auto *resp = static_cast<kvtest_control::StatsResp *>(response);
            resp->set_stats_json(metrics_->GetStatsJson());
        } else if (name == kStop) {
            auto *resp = static_cast<kvtest_control::StopResp *>(response);
            SLOG_INFO("Received KvtestControl.Stop RPC");
            running_->store(false);
            resp->set_stopping(true);
        } else if (name == kSummary) {
            auto *resp = static_cast<kvtest_control::SummaryResp *>(response);
            metrics_->WriteSummary();
            resp->set_ok(true);
        } else {
            LOG(ERROR) << "Unknown KvtestControl method: " << name;
        }
    }

    const google::protobuf::ServiceDescriptor *GetDescriptor() override { return descriptor_; }

    const google::protobuf::Message &GetRequestPrototype(
        const google::protobuf::MethodDescriptor *method) const override {
        if (method != nullptr) {
            if (method->name() == kNotify) return kvtest_control::NotifyReq::default_instance();
            if (method->name() == kStats) return kvtest_control::StatsReq::default_instance();
            if (method->name() == kStop) return kvtest_control::StopReq::default_instance();
            if (method->name() == kSummary) return kvtest_control::SummaryReq::default_instance();
        }
        return kvtest_control::NotifyReq::default_instance();
    }

    const google::protobuf::Message &GetResponsePrototype(
        const google::protobuf::MethodDescriptor *method) const override {
        if (method != nullptr) {
            if (method->name() == kNotify) return kvtest_control::NotifyResp::default_instance();
            if (method->name() == kStats) return kvtest_control::StatsResp::default_instance();
            if (method->name() == kStop) return kvtest_control::StopResp::default_instance();
            if (method->name() == kSummary) return kvtest_control::SummaryResp::default_instance();
        }
        return kvtest_control::NotifyResp::default_instance();
    }

    brpc::Server &Server() { return server_; }

private:
    NotifyDispatcher *dispatcher_;
    MetricsCollector *metrics_;
    std::atomic<bool> *running_;
    const google::protobuf::ServiceDescriptor *descriptor_ = nullptr;
    brpc::Server server_;
};

BrpcControlServer::BrpcControlServer(const Config &cfg,
                                     std::shared_ptr<datasystem::KVClient> client,
                                     MetricsCollector &metrics, std::atomic<bool> &running)
    : cfg_(cfg), running_(running), metrics_(metrics),
      dispatcher_(cfg, std::move(client), metrics),
      impl_(std::make_unique<Impl>(&dispatcher_, &metrics_, &running_)) {}

BrpcControlServer::~BrpcControlServer() { Stop(); }

void BrpcControlServer::Start() {
    // Map the legacy httplib paths to the KvtestControl methods so external
    // curl/scripts keep using /stats, /stop, /summary, /notify unchanged.
    // allow_default_url=false hides the /KvtestControl/<Method> gateway URLs,
    // leaving only the preserved paths. The typed RPC (KvtestControl::Stub
    // over brpc::Channel, used by BrpcPeerClient) is unaffected — it speaks
    // binary protobuf, not HTTP paths.
    static constexpr const char *kRestfulMappings =
        "/stats => Stats, /stop => Stop, /summary => Summary, /notify => Notify";
    if (impl_->Server().AddService(impl_.get(), brpc::SERVER_DOESNT_OWN_SERVICE,
                                    kRestfulMappings,
                                    /*allow_default_url=*/false) != 0) {
        SLOG_ERROR("Failed to add KvtestControl service with restful mappings");
        return;
    }
    if (impl_->Server().Start(cfg_.listenPort, nullptr) != 0) {
        SLOG_ERROR("Failed to start brpc server on port " << cfg_.listenPort);
        return;
    }
    SLOG_INFO("brpc control server listening on port " << cfg_.listenPort
              << " (paths: /stats /stop /summary /notify)");
}

void BrpcControlServer::Stop() {
    impl_->Server().Stop(0);
    impl_->Server().Join();
    dispatcher_.Stop();
}
