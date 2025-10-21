/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef P2PCOMMMANAGER_H
#define P2PCOMMMANAGER_H

#include <string>
#include <cstring>
#include <mutex>
#include <memory>
#include <unordered_map>
#include "communicator/P2PCommunicator.h"
#include "tools/hccl-convert.h"

class P2PCommunicatorManager {
public:
    // Store root communicator which has not yet been associated with a client communicator
    void addUnboundRootComm(std::string &identifier, std::shared_ptr<P2PCommunicator> p2pComm)
    {
        {
            std::lock_guard<std::mutex> lock(unboundRootCommsMut);
            unboundRootComms.insert(std::make_pair(identifier, p2pComm));
        }
    }

    // Retreive and remove root communicator which has not yet been associated with a client communicator
    std::shared_ptr<P2PCommunicator> getAndRemoveUnboundCommunicator(const std::string &identifier)
    {
        std::unique_lock<std::mutex> lock(unboundRootCommsMut);
        auto pos = unboundRootComms.find(identifier);
        if (pos == unboundRootComms.end()) {
            return std::shared_ptr<P2PCommunicator>();
        }
        auto res = pos->second;
        unboundRootComms.erase(identifier);
        return res;
    }

    // Retrieve communicator for which a connection has been established
    std::shared_ptr<P2PCommunicator> getCommunicator(const P2PComm &comm)
    {
        std::unique_lock<std::mutex> lock(commsMut);
        auto pos = comms.find(comm);
        if (pos == comms.end()) {
            return std::shared_ptr<P2PCommunicator>();
        }
        return pos->second;
    }

    // Add communicator for which a connection has been established
    void addCommunicator(P2PComm &resComm, std::shared_ptr<P2PCommunicator> p2pComm)
    {
        {
            std::lock_guard<std::mutex> lock(commsMut);
            comms.insert(std::make_pair(resComm, p2pComm));
        }
    }

    // Remove communicator for which a connection has been established
    bool removeCommunicator(const P2PComm &comm)
    {
        std::unique_lock<std::mutex> lock(commsMut);
        auto pos = comms.find(comm);
        if (pos == comms.end()) {
            return false;
        }
        comms.erase(comm);
        return true;
    }

private:
    // Port -> Communicator
    std::unordered_map<std::string, std::shared_ptr<P2PCommunicator>> unboundRootComms;
    std::mutex unboundRootCommsMut;
    std::unordered_map<P2PComm, std::shared_ptr<P2PCommunicator>> comms;
    std::mutex commsMut;
};

#endif
