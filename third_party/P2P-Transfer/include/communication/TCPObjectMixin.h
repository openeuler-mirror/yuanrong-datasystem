/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#ifndef P2P_TCP_OBJECT_MIXIN_H
#define P2P_TCP_OBJECT_MIXIN_H

template <typename Base>
class TCPObjectMixin : public Base {
public:
    using Base::Base;  // Inherit constructors

    /**
     * @brief Receive protobuf object over TCP
     * @tparam T Protobuf object type
     * @param object The object to receive
     * @return Status::Success() on success, error status on failure
     */
    template <typename T>
    Status ReceiveObject(T &object)
    {
        // Receive the object size
        uint32_t objectSize = 0;
        int bytesReceived = this->Read(reinterpret_cast<unsigned char *>(&objectSize), sizeof(objectSize));
        if (bytesReceived == 0) {
            return Status::Error(ErrorCode::TCP_ERROR, "Client disconnected");
        } else if (bytesReceived != sizeof(objectSize)) {
            return Status::Error(ErrorCode::TCP_ERROR, "Receive failed: bytes received does not match expected size");
        }

        // Allocate a buffer large enough to hold the full object
        std::vector<unsigned char> buffer(objectSize);
        uint32_t totalReceived = 0;

        // Receive the object in chunks if needed
        while (totalReceived < objectSize) {
            bytesReceived = this->Read(buffer.data() + totalReceived, objectSize - totalReceived);
            if (bytesReceived == 0) {
                return Status::Error(ErrorCode::TCP_ERROR, "Client disconnected");
            } else if (bytesReceived < 0) {
                return Status::Error(ErrorCode::TCP_ERROR, "Receive failed");
            }

            totalReceived += bytesReceived;
        }

        // Deserialize the object
        CHECK_STATUS(
            Serializer::Deserialize(std::string(reinterpret_cast<char *>(buffer.data()), buffer.size()), object));
        return Status::Success();
    }

    /**
     * @brief Send protobuf object over TCP
     * @tparam T Protobuf object type
     * @param object The object to send
     * @return Status::Success() on success, error status on failure
     */
    template <typename T>
    Status SendObject(const T &object)
    {
        // Serialize the object
        std::string serializedData;
        CHECK_STATUS(Serializer::Serialize(object, serializedData));

        // Send the object size
        uint32_t objectSize = serializedData.size();
        if (this->Write(reinterpret_cast<const unsigned char *>(&objectSize), sizeof(objectSize)) <= 0) {
            return Status::Error(ErrorCode::TCP_ERROR, "Failed to write object size");
        }

        // Send the object
        if (objectSize > 0) {
            if (this->Write(reinterpret_cast<const unsigned char *>(serializedData.data()), serializedData.size())
                <= 0) {
                return Status::Error(ErrorCode::TCP_ERROR, "Failed to write object");
            }
        }

        return Status::Success();
    }
};

#endif  // P2P_TCP_OBJECT_MIXIN_H