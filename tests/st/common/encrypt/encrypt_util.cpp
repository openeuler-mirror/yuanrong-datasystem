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
 * Description: Generate binary file "encrypt", which use to generate ciphertext and four key components.
 */
#include <getopt.h>
#include <securec.h>

#include <climits>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <vector>

#include "datasystem/common/encrypt/secret_manager.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/util/file_util.h"
#include "re2/re2.h"

DS_DECLARE_string(secret_key1);
DS_DECLARE_string(secret_key2);
DS_DECLARE_string(secret_key3);
DS_DECLARE_string(secret_salt);
DS_DECLARE_string(encrypt_kit);

const int CIPHER_LEN = 10240;

const std::string K1_PATH = "./k1.txt";
const std::string K2_PATH = "./k2.txt";
const std::string K3_PATH = "./k3.txt";
const std::string SALT_PATH = "./salt.txt";

static void PrintHelp()
{
    std::cout << "Options:\n"
                 "    -d,--dir              Plaintexts directory with some plaintext files that need to encrypt.\n"
                 "    -f,--file             A plaintext file that you want to encrypt.\n"
                 "    -o,--output           Output directory for the ciphertexts. Default is current dir.\n"
                 "    -h,--help             This help message."
              << std::endl;
}

/**
 * @brief Read plaintext file.
 * @param[in] filePath plaintext file path.
 * @return string of plaintext.
 */
static std::string ReadFile(const std::string &filePath)
{
    std::string plaintext;
    std::ifstream ifs(filePath);
    if (!ifs.is_open()) {
        std::cerr << "Error: Cannot open plaintext file." << std::endl;
        return "";
    }
    std::stringstream buffer;
    buffer << ifs.rdbuf();
    ifs.close();

    plaintext = buffer.str();
    // Remove all of '\n' and '\r' at the end of string.
    re2::RE2 re(R"((\r\n|\n|\r)*$)");
    re2::RE2::GlobalReplace(&plaintext, re, "");
    if (plaintext.length() > CIPHER_LEN) {
        std::cerr << "Error: Content exceeds the length limit." << std::endl;
        return "";
    }
    return plaintext;
}

/**
 * @brief Datasystem encrypt. First, load key components and generate rootkey; Last, use rootkey to encrypt plaintext.
 * @param[in] k1Path Key component file path.
 * @param[in] k2Path Key component file path.
 * @param[in] k3Path Key component file path.
 * @param[in] saltPath Salt component file path.
 * @param[in] plaintext plaintext that needs to encrypt.
 * @param[out] ciphertext ciphertext.
 * @return 0 if success, 1 otherwise.
 */
static int Encrypt(const std::string &k1Path, const std::string &k2Path, const std::string &k3Path,
                   const std::string &saltPath, const std::string &plaintext, std::string &ciphertext)
{
    FLAGS_secret_key1 = k1Path;
    FLAGS_secret_key2 = k2Path;
    FLAGS_secret_key3 = k3Path;
    FLAGS_secret_salt = saltPath;
    datasystem::Status rc;
    datasystem::SecretManager secretManager;
    rc = secretManager.LoadSecretKeys();
    if (rc.IsError()) {
        std::cerr << "Error: Load secret keys failed. status: " << rc.ToString() << std::endl;
        return 1;
    }
    rc = secretManager.GenerateRootKey();
    if (rc.IsError()) {
        std::cerr << "Error: Generate rootkey failed. status: " << rc.ToString() << std::endl;
        return 1;
    }
    rc = secretManager.Encrypt(plaintext, ciphertext);
    if (rc.IsError()) {
        std::cerr << "Error: Encrypt plaintext failed. status: " << rc.ToString() << std::endl;
        return 1;
    }
    return 0;
}

/**
 * @brief Encrypt a plaintext file
 * @param[in] file The path of plaintext file.
 * @param[in] outputPath The output dir of ciphertext.
 * @param[in] k1Path Key component file path.
 * @param[in] k2Path Key component file path.
 * @param[in] k3Path Key component file path.
 * @param[in] saltPath Salt component file path.
 * @return 0 if success, 1 otherwise.
 */
static int EncryptForFile(const std::string &file, const std::string &outputPath, const std::string &k1Path,
                          const std::string &k2Path, const std::string &k3Path, const std::string &saltPath)
{
    char path[PATH_MAX + 1] = { 0 };
    if (realpath(file.c_str(), path) == nullptr) {
        std::cerr << "Error: Invalid file path!" << std::endl;
        return 1;
    }
    std::string fileRealPath(path);
    if (outputPath.empty()) {
        std::cerr << "Error: Output dir should not be empty!" << std::endl;
        return 1;
    }
    if (!datasystem::FileExist(outputPath)) {
        datasystem::Status rc = datasystem::CreateDir(outputPath);
        if (rc.IsError()) {
            std::cerr << "Error: Create output directory failed. status: " << rc.ToString() << std::endl;
            return 1;
        }
    }

    std::string fileName;
    // Remove the path prefix.
    int pos = fileRealPath.find_last_of('/');
    fileName = fileRealPath.substr(pos + 1);
    std::string outputFilePath = outputPath + "/" + fileName;

    // Read plaintext.
    std::string plaintext = ReadFile(fileRealPath);
    if (plaintext.empty()) {
        std::cerr << "Error: plaintext is empty.";
        return 1;
    }
    std::string cipherTextStr;

    // Encrypt
    if (Encrypt(k1Path, k2Path, k3Path, saltPath, plaintext, cipherTextStr) != 0) {
        std::cerr << "Error: encrypt text failed!";
        return 1;
    }

    // Print ciphertext to output dir.
    std::ofstream ofs(outputFilePath);
    if (!ofs.is_open()) {
        std::cerr << "Error: Cannot open ciphertext file." << std::endl;
        return 1;
    }
    ofs << cipherTextStr;
    ofs.close();
    return 0;
}

/**
 * @brief Encrypt a directory with plaintext files.
 * @param[in] inputDir The directory with some plaintext files.
 * @param[in] outputPath The output dir of ciphertext.
 * @param[in] k1Path Key component file path.
 * @param[in] k2Path Key component file path.
 * @param[in] k3Path Key component file path.
 * @param[in] saltPath Salt component file path.
 * @return 0 if success, 1 otherwise.
 */
static int EncryptForDir(const std::string &inputDir, const std::string &outputPath, const std::string &k1Path,
                         const std::string &k2Path, const std::string &k3Path, const std::string &saltPath)
{
    char path[PATH_MAX + 1] = { 0 };
    if (realpath(inputDir.c_str(), path) == nullptr) {
        std::cerr << "Error: Invalid input dir path!" << std::endl;
        return 1;
    }
    std::string inputRealPath(path);
    if (outputPath.empty()) {
        std::cerr << "Error: Output dir should not be empty!" << std::endl;
        return 1;
    }

    // Get all files in input dir.
    std::vector<std::string> files;
    std::string pattern = inputRealPath + "/*";
    datasystem::Status rc = datasystem::Glob(pattern, files);
    if (rc.IsError()) {
        std::cerr << "Error: Cannot Glob the plaintexts, status: " << rc.ToString() << std::endl;
        return 1;
    }

    for (auto &file : files) {
        int ret = EncryptForFile(file, outputPath, k1Path, k2Path, k3Path, saltPath);
        if (ret != 0) {
            std::cerr << "Error: Encrypt plaintext file failed!" << std::endl;
            return 1;
        }
    }
    return 0;
}

/**
 * @brief Check whether filePath and outputDir are in the same folder.
 * @param[in] filePath Key component path.
 * @param[in] outputDir The output dir of key components.
 * @return true if the same, false if not.
 */
static bool IsInSameDir(const std::string &filePath, const std::string &outputDir)
{
    int pos = filePath.find_last_of('/');
    std::string inputDir = filePath.substr(0, pos);
    if (inputDir == outputDir) {
        return true;
    }
    return false;
}

/**
 * @brief Move all key components to output dir.
 * @param[in] k1Path Key component file path.
 * @param[in] k2Path Key component file path.
 * @param[in] k3Path Key component file path.
 * @param[in] saltPath Salt component file path.
 * @param[in] outputDir The output dir of key components.
 */
static void MoveKeyComponent(const std::string &k1Path, const std::string &k2Path, const std::string &k3Path,
                             const std::string &saltPath, const std::string &outputDir)
{
    datasystem::Status rc;
    if (!IsInSameDir(k1Path, outputDir)) {
        rc = datasystem::MoveFileToNewPath(k1Path, outputDir);
        if (rc.IsOk()) {
            std::cout << "Move k1 to " << outputDir << " success!" << std::endl;
        }
    }
    if (!IsInSameDir(k2Path, outputDir)) {
        rc = datasystem::MoveFileToNewPath(k2Path, outputDir);
        if (rc.IsOk()) {
            std::cout << "Move k2 to " << outputDir << " success!" << std::endl;
        }
    }
    if (!IsInSameDir(k3Path, outputDir)) {
        rc = datasystem::MoveFileToNewPath(k3Path, outputDir);
        if (rc.IsOk()) {
            std::cout << "Move k3 to " << outputDir << " success!" << std::endl;
        }
    }
    if (!IsInSameDir(saltPath, outputDir)) {
        rc = datasystem::MoveFileToNewPath(saltPath, outputDir);
        if (rc.IsOk()) {
            std::cout << "Move salt to " << outputDir << " success!" << std::endl;
        }
    }
}

/**
 * @brief Encrypt plaintext.
 * @param[in] file Plaintext file that needs to encrypt.
 * @param[in] inputDir Plaintexts directory with some plaintext files that need to encrypt.
 * @param[in] outputDir Ciphertext output directory, default is current dir.
 * @return 0 if success, 1 otherwise.
 */
static int Encrypt(const std::string &file, const std::string &inputDir, const std::string &outputDir)
{
    if (file.empty() && inputDir.empty()) {
        std::cerr << "Error: Must set one of file or file input directory." << std::endl;
        return 1;
    }

    // Realpath the output dir
    char outputPath[PATH_MAX + 1] = { 0 };
    int ret = 0;
    if (outputDir.empty() || realpath(outputDir.c_str(), outputPath) == nullptr) {
        std::cerr << "Warning: Invalid output dir path, output cipher dir will be set to current folder." << std::endl;
        ret = memset_s(outputPath, PATH_MAX, '\0', PATH_MAX);
        if (ret != 0) {
            std::cerr << "Error: Memory set failed, ret: " << ret << std::endl;
            return 1;
        }
        std::string cur = "./output_cipher";
        ret = memcpy_s(outputPath, PATH_MAX, cur.c_str(), cur.length());
        if (ret != 0) {
            std::cerr << "Error: Memory copy failed, ret: " << ret << std::endl;
            return 1;
        }
    }

    // If realpath is success, outputPath will be output dir path.
    std::string outputPathStr(outputPath);
    std::cout << "outputPathStr: " << outputPathStr << std::endl;

    FLAGS_encrypt_kit = "kmc";
    datasystem::Status rc =
        datasystem::SecretManager::Instance()->GenerateAllKeyComponent(K1_PATH, K2_PATH, K3_PATH, SALT_PATH);
    if (rc.IsError()) {
        std::cerr << "Error: Generate key components failed. status: " << rc.ToString() << std::endl;
        return 1;
    }

    if (!file.empty()) {
        ret = EncryptForFile(file, outputPathStr, K1_PATH, K2_PATH, K3_PATH, SALT_PATH);
        if (ret != 0) {
            std::cerr << "Error: Encrypt plaintext file failed, ret: " << ret << std::endl;
            return 1;
        }
    }
    if (!inputDir.empty()) {
        ret = EncryptForDir(inputDir, outputPathStr, K1_PATH, K2_PATH, K3_PATH, SALT_PATH);
        if (ret != 0) {
            std::cerr << "Error: Encrypt plaintext dir failed, ret: " << ret << std::endl;
            return 1;
        }
    }
    MoveKeyComponent(K1_PATH, K2_PATH, K3_PATH, SALT_PATH, outputPathStr);
    return 0;
}

int main(int argc, char **argv)
{
    std::string file;
    std::string inputDir;
    std::string outputDir;
    const option longOpts[] = { { "dir", optional_argument, nullptr, 'd' },
                                { "file", optional_argument, nullptr, 'f' },
                                { "output", required_argument, nullptr, 'o' },
                                { "help", no_argument, nullptr, 'h' },
                                { nullptr, no_argument, nullptr, 0 } };
    int argsRc = 0;
    try {
        while (argsRc == 0) {
            int32_t optionIndex;
            const auto opt = getopt_long(argc, argv, "d:f:o:h", longOpts, &optionIndex);
            if (opt == -1) {
                break;
            }
            switch (opt) {
                case 'd':
                    inputDir = optarg;
                    break;
                case 'f':
                    file = optarg;
                    break;
                case 'o':
                    outputDir = optarg;
                    break;
                case 'h':
                    PrintHelp();
                    return 1;
                default:
                    std::cerr << "Error: Unknown option " << char(optopt) << std::endl;
                    PrintHelp();
                    argsRc = -1;
                    break;
            }
        }
    } catch (const std::exception &e) {
        PrintHelp();
    }

    if (argsRc == -1) {
        std::cerr << "Error: Failed to parse input arguments!" << std::endl;
        return 1;
    }

    if (Encrypt(file, inputDir, outputDir) != 0) {
        PrintHelp();
        return 1;
    }
    return 0;
}