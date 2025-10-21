/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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

/**
 * Description: Generate binary file "hashring_parser", which use to convert HashRingPb to json and json to HashRingPb.
 */

#include <fstream>
#include <getopt.h>
#include <iostream>
#include <sstream>
#include <string>

#include <google/protobuf/util/json_util.h>

#include "datasystem/protos/hash_ring.pb.h"

using datasystem::HashRingPb;

static void PrintHelp()
{
    std::cout << "This tool supports format conversion between HashRingPb and json.\n"
                 "Options:\n"
                 "    -d,--decode           Decode the HashRingPb binary data file to json.\n"
                 "    -e,--encode           Encode the json file to HashRingPb binary data.\n"
                 "    -p,--print            Print the json file as proto debug string.\n"
                 "    -h,--help             This help message.\n"
                 "Guide for O&M:\n"
                 " 1. Get the HashRingPb binary data:\n"
                 "    etcdctl get /datasystem/ring/ --write-out=json | jq -r '.kvs[0].value' | base64 -d > ring.dat\n"
                 " 2. Get the json file:\n"
                 "    ./hashring_parser -d ring.dat > ring.json\n"
                 " 3. Update the ring.json and then re-encode to binary\n"
                 "    ./hashring_parser -e ring.json > newring.dat\n"
                 " 4. Put the new ring to the etcd:\n"
                 "    cat newring.dat | etcdctl put /datasystem/ring/ "
              << std::endl;
}

int ReadFile(const std::string &file, std::string &content)
{
    std::ifstream infile(file);
    if (!infile.is_open()) {
        std::cerr << "Cannot open file " << file;
        return -1;
    }
    std::stringstream buffer;
    buffer << infile.rdbuf();
    content = buffer.str();
    return 0;
}

int Decode(const std::string &file)
{
    std::string content;
    auto ret = ReadFile(file, content);
    if (ret != 0) {
        return ret;
    }

    HashRingPb ring;
    if (!ring.ParseFromString(content)) {
        std::cerr << "Parse HashRingPb failed.";
        return -1;
    }

    std::string jsonStr;
    auto status = google::protobuf::util::MessageToJsonString(ring, &jsonStr);
    if (!status.ok()) {
        std::cerr << "Parse HashRingPb to json failed: " << status.ToString();
        return -1;
    }

    std::cout << jsonStr;
    return 0;
}

int GetHashRingPbFromJsonFile(const std::string &file, HashRingPb &hashRing)
{
    std::string content;
    auto ret = ReadFile(file, content);
    if (ret != 0) {
        return ret;
    }

    auto status = google::protobuf::util::JsonStringToMessage(content, &hashRing);
    if (!status.ok()) {
        std::cerr << "Parse json failed: " << status.ToString();
        return -1;
    }

    return 0;
}

int Encode(const std::string &file)
{
    HashRingPb hashRing;
    auto ret = GetHashRingPbFromJsonFile(file, hashRing);
    if (ret != 0) {
        return ret;
    }

    std::cout << hashRing.SerializeAsString();
    return 0;
}

int PrintDebugString(const std::string &file)
{
    HashRingPb hashRing;
    auto ret = GetHashRingPbFromJsonFile(file, hashRing);
    if (ret != 0) {
        return ret;
    }

    std::cout << hashRing.DebugString();
    return 0;
}

int main(int argc, char **argv)
{
    const option longOpts[] = { { "decode", required_argument, nullptr, 'd' },
                                { "encode", required_argument, nullptr, 'e' },
                                { "print", required_argument, nullptr, 'p' },
                                { "help", no_argument, nullptr, 'h' },
                                { nullptr, no_argument, nullptr, 0 } };

    try {
        int32_t optionIndex;
        const auto opt = getopt_long(argc, argv, "d:e:p:h", longOpts, &optionIndex);
        if (opt == -1) {
            PrintHelp();
            return -1;
        }
        switch (opt) {
            case 'd':
                return Decode(optarg);
            case 'e':
                return Encode(optarg);
            case 'p':
                return PrintDebugString(optarg);
            case 'h':
                PrintHelp();
                return 0;
            default:
                std::cerr << "Error: Failed to parse input arguments!" << std::endl;
                PrintHelp();
                return -1;
        }
    } catch (const std::exception &e) {
        std::cerr << e.what();
        return -1;
    }

    return 0;
}