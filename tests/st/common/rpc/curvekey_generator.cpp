/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: Generate curve public/private key pair.
 */
#include <getopt.h>

#include <sys/stat.h>
#include <cerrno>

#include "datasystem/common/rpc/rpc_auth_key_manager.h"
#include "datasystem/common/rpc/zmq/zmq_common.h"
#include "datasystem/common/util/file_util.h"

static void PrintHelp()
{
    std::cout << "Options:\n"
                 "    -n,--name               Name for generated key files.\n"
                 "    -d,--output             Output directory for the generated key files. Default is current dir\n"
                 "    -h,--help               This help message.\n";
}

/**
 * @brief Save message to the file.
 * @param[in] fileName The file name.
 * @param[in] fileContent The file content.
 */
static void SaveFile(const std::string &fileName, const char *fileContent)
{
    std::cout << "Saving file: " << fileName << std::endl;
    std::ofstream public_file(fileName);
    public_file << fileContent;
    public_file.close();
}

/**
 * @brief Change the file mode.
 * @param[in] fileName The file name.
 * @return 0 if success, 1 otherwise.
 */
static int ChangeFileMod(const std::string &fileName)
{
    if (chmod(fileName.c_str(), S_IRUSR | S_IWUSR) != 0) {
        std::cerr << "Failed to change mode for file " << fileName << std::endl;
        return 1;
    }
    return 0;
}

/**
 * @brief Working with key files.
 * @param[in] fileName The file name.
 * @param[in] outputDir The output dir.
 * @return 0 if success, 1 otherwise.
 */
static int ProcessKeyFiles(const std::string &fileName, const std::string &outputDir)
{
    datasystem::Status rc;
    if (fileName.empty()) {
        std::cerr << "Missing the required name argument!\n";
        return 1;
    }
    if (!outputDir.empty()) {
        bool isDir = true;
        rc = datasystem::IsDirectory(outputDir, isDir);
        if (rc.IsError() || !isDir) {
            std::cerr << "Output directory " << outputDir << " check failed or doesn't exist!\n";
            return 1;
        }
    }
    std::string outputFile = (outputDir.empty() ? outputDir : outputDir + "/") + fileName;
    char publicKey[datasystem::ZMQ_ENCODE_KEY_SIZE_NUL_TERM];
    char secretKey[datasystem::ZMQ_ENCODE_KEY_SIZE_NUL_TERM];
    errno = 0;
    int result = zmq_curve_keypair(publicKey, secretKey);
    if (result != 0) {
        std::cerr << "Failed to generate curve key pair, errno: " << errno << std::endl;
        return 1;
    }

    try {
        SaveFile(outputFile + datasystem::PUBLIC_KEY_EXT, publicKey);
        SaveFile(outputFile + datasystem::PRIVATE_KEY_EXT, secretKey);
    } catch (const std::exception &e) {
        std::cerr << "Failed to save curve keys to file, error: " << e.what() << std::endl;
        return 1;
    }

    if (ChangeFileMod(outputFile + datasystem::PUBLIC_KEY_EXT)
        || ChangeFileMod(outputFile + datasystem::PRIVATE_KEY_EXT)) {
        return 1;
    }
    return 0;
}

int main(int argc, char **argv)
{
    std::string fileName, outputDir;
    const option longOpts[] = { { "name", required_argument, nullptr, 'n' },
                                { "output", required_argument, nullptr, 'd' },
                                { "help", no_argument, nullptr, 'h' },
                                { nullptr, no_argument, nullptr, 0 } };
    int argsRc = 0;
    try {
        while (argsRc == 0) {
            int32_t optionIndex;
            const auto opt = getopt_long(argc, argv, "n:p:d:h", longOpts, &optionIndex);
            if (opt == -1) {
                break;
            }
            switch (opt) {
                case 'n':
                    fileName = optarg;
                    break;

                case 'd':
                    outputDir = optarg;
                    break;
                case 'h':
                    PrintHelp();
                    break;
                default:
                    std::cerr << "Unknown option " << char(optopt) << std::endl;
                    PrintHelp();
                    argsRc = -1;
                    break;
            }
        }
    } catch (const std::exception &e) {
        PrintHelp();
    }

    if (argsRc == -1) {
        std::cerr << "Failed to parse input arguments!\n";
        return 1;
    }

    if (ProcessKeyFiles(fileName, outputDir) == 1) {
        PrintHelp();
        return 1;
    }
    return 0;
}