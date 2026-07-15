/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

/** Description: Defines the shared-memory transporter placeholder. */
#ifndef DATASYSTEM_CLIENT_TRANSPORT_SHM_TRANSPORTER_H
#define DATASYSTEM_CLIENT_TRANSPORT_SHM_TRANSPORTER_H

#include "datasystem/client/transport/data_plane/i_data_transporter.h"

namespace datasystem {
namespace client {
class ShmTransporter : public IDataTransporter {
public:
    AccessTransportKind Kind() const override
    {
        return AccessTransportKind::SHM;
    }

    bool IsAlive() const override
    {
        return false;
    }

    Status Get(const DataGetRequest & /* input */, DataGetResult & /* output */) override
    {
        return Status(K_NOT_SUPPORTED, "ShmTransporter::Get not implemented");
    }

    // SHM Create and Set remain explicit unsupported placeholders until transport integration is available.
    Status Create(const HostPort & /* workerAddr */, const std::string & /* key */, uint64_t /* size */,
                  const TransportCreateParam & /* param */, std::shared_ptr<ObjectBuffer> & /* buffer */) override
    {
        return Status(K_NOT_SUPPORTED, "ShmTransporter::Create not implemented");
    }

    Status Set(ObjectBuffer & /* buffer */, const TransportSetParam & /* param */) override
    {
        return Status(K_NOT_SUPPORTED, "ShmTransporter::Set not implemented");
    }

    Status MCreate(const HostPort & /* workerAddr */, const std::vector<std::string> & /* keys */,
                   const std::vector<uint64_t> & /* sizes */, const TransportCreateParam & /* param */,
                   std::vector<std::shared_ptr<ObjectBuffer>> & /* buffers */) override
    {
        return Status(K_NOT_SUPPORTED, "ShmTransporter::MCreate not implemented");
    }

    Status MSet(const std::vector<std::shared_ptr<ObjectBuffer>> & /* buffers */,
                const TransportSetParam & /* param */, TransportMSetResult & /* result */) override
    {
        return Status(K_NOT_SUPPORTED, "ShmTransporter::MSet not implemented");
    }

    Status Release(const ShmKey & /* shmId */, const TransportRequestContext & /* context */) override
    {
        return Status::OK();
    }
};
}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_TRANSPORT_SHM_TRANSPORTER_H
