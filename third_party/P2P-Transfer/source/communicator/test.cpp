/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "communicator/P2PCommunicator.h"
#include "securec.h"
#include "communicator/hccs-ipc/HccsReceiver.h"
#include "communicator/hccs-ipc/HccsSender.h"
#include "communicator/roce/RoceReceiver.h"
#include "communicator/roce/RoceSender.h"

Status p2pKindToCommRole(P2pKind kind, P2PCommRole &role)
{
    switch (kind) {
        case P2P_RECEIVER:
            role = P2PCommRole::P2P_COMM_RECEIVER;
            break;
        case P2P_SENDER:
            role = P2PCommRole::P2P_COMM_SENDER;
            break;
        default:
            return Status::Error(ErrorCode::NOT_SUPPORTED, "p2pKind unknown");
    }

    return Status::Success();
}

P2PCommunicator::P2PCommunicator(bool isRoot) : isRoot(isRoot)
{
    if (isRoot) {
        identifier = std::string(ROOTHANDLE_INDENTIFIER_MAX_LENGTH, ' ');
        FillRandom(identifier);
    }
}

Status P2PCommunicator::StartRoot()
{
    if (!isRoot) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "Communicator not of type root");
    }

    CHECK_STATUS(CreateServer());
    return Status::Success();
}

Status P2PCommunicator::StartClient(P2PRootHandle &rootHandle)
{
    if (isRoot) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "Communicator not of type client");
    }

    std::string ip = rootHandle.ip;
    identifier = std::string(&rootHandle.identifier[0], ROOTHANDLE_INDENTIFIER_MAX_LENGTH);

    CHECK_STATUS(CreateClient(ip, rootHandle.listenPort));
    return Status::Success();
}

Status P2PCommunicator::GetRootHandle(P2PRootHandle &rootHandle)
{
    if (!isRoot) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "Communicator not of type root");
    }

    std::string ip = server->GetIp();
    memcpy_s(rootHandle.ip, ip.length(), ip.data(), ip.length());
    rootHandle.ip[ip.length()] = '\0';
    rootHandle.listenPort = server->GetPort();
    memcpy_s(rootHandle.identifier, ROOTHANDLE_INDENTIFIER_MAX_LENGTH, identifier.data(),
             ROOTHANDLE_INDENTIFIER_MAX_LENGTH);

    return Status::Success();
}

Status P2PCommunicator::EstablishConnection(P2PCommArgs &args)
{
    role = args.role;
    if (args.channelType == P2PCommChannelType::P2P_COMM_HCCS) {
        if (role == P2P_COMM_RECEIVER) {
            receiver = std::make_unique<HccsReceiver>(args.deviceId, isRoot, args.blockSizeBytes, args.chunkSizeBytes,
                                                      args.nRecvBuffs);
        } else {
            sender = std::make_unique<HccsSender>(args.deviceId, isRoot, args.blockSizeBytes, args.chunkSizeBytes,
                                                  args.nRecvBuffs);
        }
    } else if (args.channelType == P2PCommChannelType::P2P_COMM_RDMA) {
        if (role == P2P_COMM_RECEIVER) {
            receiver = std::make_unique<RoceReceiver>(args.deviceId, isRoot, args.blockSizeBytes, args.chunkSizeBytes,
                                                      args.nRecvBuffs);
        } else {
            sender = std::make_unique<RoceSender>(args.deviceId, isRoot, args.blockSizeBytes, args.chunkSizeBytes,
                                                  args.nRecvBuffs);
        }
    } else {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "Unsupported channeltype");
    }

    if (isRoot) {
        CHECK_STATUS(AcceptClient());
        if (role == P2PCommRole::P2P_COMM_RECEIVER) {
            CHECK_STATUS(receiver->Initialize(nullptr, server.get()));
        } else {
            CHECK_STATUS(sender->Initialize(nullptr, server.get()));
        }
        return Status::Success();
    } else {
        CHECK_STATUS(ConnectServer());
        if (role == P2PCommRole::P2P_COMM_RECEIVER) {
            CHECK_STATUS(receiver->Initialize(client.get(), nullptr));
        } else {
            CHECK_STATUS(sender->Initialize(client.get(), nullptr));
        }
    }
    return Status::Success();
}

Status P2PCommunicator::Receive(void *dst, uint64_t sizeBytes, aclrtStream stream)
{
    if (role != P2PCommRole::P2P_COMM_RECEIVER) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "P2P Comm is not of type receiver");
    }

    CHECK_STATUS(receiver->Receive(dst, sizeBytes, stream));

    return Status::Success();
}

Status P2PCommunicator::Send(void *src, uint64_t sizeBytes, aclrtStream stream)
{
    if (role != P2PCommRole::P2P_COMM_SENDER) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "P2P Comm is not of type sender");
    }

    CHECK_STATUS(sender->Send(src, sizeBytes, stream));

    return Status::Success();
}

Status P2PCommunicator::CreateServer()
{
    uint16_t startPort;
    uint16_t endPort;
    CHECK_STATUS(GetPortRange(startPort, endPort));

    std::string ip;
    CHECK_STATUS(GetIfIp(ip));
    server = std::make_unique<TCPObjectServer>(ip, COMMUNICATOR_TCP_TIMEOUT_S);

    Status status = server->ListenFirstAvailable(startPort, endPort);
    if (!status.IsSuccess()) {
        server.reset();
        return status;
    }

    return Status::Success();
}

Status P2PCommunicator::AcceptClient()
{
    if (!server) {
        return Status::Error(ErrorCode::TCP_ERROR, "TCP Server not yet created");
    }

    CHECK_STATUS(server->Accept());

    AuthData authData;
    CHECK_STATUS(server->ReceiveObject(authData));

    AuthResponse authResponse;
    authResponse.set_success(true);

    if (authData.identifier() != identifier) {
        authResponse.set_success(false);
        CHECK_STATUS(server->SendObject(authResponse));
        return Status::Error(ErrorCode::TCP_ERROR, "Auth failed, client tried to connect with invalid identifier.");
    }

    CHECK_STATUS(server->SendObject(authResponse));
    return Status::Success();
}

Status P2PCommunicator::CreateClient(std::string ip, uint16_t port)
{
    client = std::make_unique<TCPObjectClient>(ip, port, COMMUNICATOR_TCP_TIMEOUT_S);

    Status status = client->Init();
    if (!status.IsSuccess()) {
        client.reset();
        return status;
    }

    return Status::Success();
}

Status P2PCommunicator::ConnectServer()
{
    CHECK_STATUS(client->Connect());

    AuthData authData;
    authData.set_identifier(identifier);
    CHECK_STATUS(client->SendObject(authData));

    AuthResponse authResponse;
    CHECK_STATUS(client->ReceiveObject(authResponse));

    if (!authResponse.success()) {
        return Status::Error(ErrorCode::TCP_ERROR, "Auth failed, server rejected identifier.");
    }

    return Status::Success();
}