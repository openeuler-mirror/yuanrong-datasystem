/*
* Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#ifndef P2P_SERIALIZER_H
#define P2P_SERIALIZER_H

#include <string>
#include <type_traits>
#include <google/protobuf/message.h>
#include "../tools/Status.h"

class Serializer {
public:
    /**
     * @brief Serialize a Protobuf message to string
     * @tparam T Protobuf message type
     * @param object The message to serialize
     * @param outString Output string (will be overwritten)
     * @return Status::Success() on success, error status on failure
     */
    template <typename T>
    static Status Serialize(const T &object, std::string &outString) noexcept
    {
        static_assert(std::is_base_of<google::protobuf::Message, T>::value,
                      "Serialization is only supported for Protobuf Messages.");

        if (!object.SerializeToString(&outString)) {
            return Status::Error(ErrorCode::PROTOBUF_ERROR, "Failed to serialize object to string");
        }

        return Status::Success();
    }

    /**
     * @brief Deserialize a Protobuf message from string
     * @tparam T Protobuf message type
     * @param data Input string containing serialized data
     * @param object Output message (will be overwritten)
     * @return Status::Success() on success, error status on failure
     */
    template <typename T>
    static Status Deserialize(const std::string &data, T &object) noexcept
    {
        static_assert(std::is_base_of<google::protobuf::Message, T>::value,
                      "Deserialization is only supported for Protobuf Messages.");

        if (data.empty()) {
            return Status::Success();
        }

        if (!object.ParseFromString(data)) {
            return Status::Error(ErrorCode::PROTOBUF_ERROR, "Failed to deserialize object from string");
        }

        return Status::Success();
    }
};

#endif  // P2P_SERIALIZER_H