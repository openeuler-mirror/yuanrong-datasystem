#ifndef TRANSFER_ENGINE_INTERNAL_CONTROL_PLANE_H
#define TRANSFER_ENGINE_INTERNAL_CONTROL_PLANE_H

#include <cstdint>
#include <atomic>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "datasystem/transfer_engine/control_plane_messages.h"
#include "datasystem/transfer_engine/status.h"

namespace datasystem {

class ITransferControlService {
public:
    virtual ~ITransferControlService() = default;

    virtual Status ExchangeRootInfo(const ExchangeRootInfoRequest &req, ExchangeRootInfoResponse *rsp) = 0;
    virtual Status QueryConnReady(const QueryConnReadyRequest &req, QueryConnReadyResponse *rsp) = 0;
    virtual Status ReadTrigger(const ReadTriggerRequest &req, ReadTriggerResponse *rsp) = 0;
    virtual Status BatchReadTrigger(const BatchReadTriggerRequest &req, BatchReadTriggerResponse *rsp) = 0;
};

class ITransferControlClient {
public:
    virtual ~ITransferControlClient() = default;

    virtual Status ExchangeRootInfo(const std::string &host, uint16_t port, const ExchangeRootInfoRequest &req,
                                    ExchangeRootInfoResponse *rsp) = 0;
    virtual Status QueryConnReady(const std::string &host, uint16_t port, const QueryConnReadyRequest &req,
                                  QueryConnReadyResponse *rsp) = 0;
    virtual Status ReadTrigger(const std::string &host, uint16_t port, const ReadTriggerRequest &req,
                               ReadTriggerResponse *rsp) = 0;
    virtual Status BatchReadTrigger(const std::string &host, uint16_t port, const BatchReadTriggerRequest &req,
                                    BatchReadTriggerResponse *rsp) = 0;
};

class SocketControlClient : public ITransferControlClient {
public:
    Status ExchangeRootInfo(const std::string &host, uint16_t port, const ExchangeRootInfoRequest &req,
                            ExchangeRootInfoResponse *rsp) override;
    Status QueryConnReady(const std::string &host, uint16_t port, const QueryConnReadyRequest &req,
                          QueryConnReadyResponse *rsp) override;
    Status ReadTrigger(const std::string &host, uint16_t port, const ReadTriggerRequest &req,
                       ReadTriggerResponse *rsp) override;
    Status BatchReadTrigger(const std::string &host, uint16_t port, const BatchReadTriggerRequest &req,
                            BatchReadTriggerResponse *rsp) override;
};

class SocketControlServer {
public:
    SocketControlServer();
    ~SocketControlServer();

    Status Start(const std::string &host, uint16_t port, std::shared_ptr<ITransferControlService> service,
                 int32_t workerThreads = 4);
    void Stop();

private:
    void AcceptLoop();
    void WorkerLoop();
    void HandleClient(int clientFd);

    std::atomic<bool> running_{ false };
    int listenFd_ = -1;
    int32_t workerCount_ = 0;
    std::thread acceptThread_;
    std::vector<std::thread> workerThreads_;
    std::mutex queueMutex_;
    std::condition_variable queueCv_;
    std::deque<int> clientFdQueue_;
    std::shared_ptr<ITransferControlService> service_;
};

}  // namespace datasystem

#endif  // TRANSFER_ENGINE_INTERNAL_CONTROL_PLANE_H
