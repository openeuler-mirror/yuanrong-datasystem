/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: SSL Auth
 */
#include "datasystem/common/util/ssl_authorization.h"

#include <climits>
#include <memory>
#include <string>

#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/safestack.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <openssl/x509_vfy.h>
#include <openssl/ossl_typ.h>
#include <openssl/pkcs12.h>
#include <openssl/rsa.h>

#include "datasystem/common/util/format.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"

namespace datasystem {
bool LoadKeyAndCertFromMemory(SSL_CTX *context, const char *certBuffer, size_t certSize, const char *keyBuffer,
                              size_t keySize)
{
    BIO *certbio = nullptr;
    BIO *kbio = nullptr;
    X509 *cert = nullptr;
    RSA *rsa = nullptr;
    Raii freePtr([&certbio, &kbio, &cert, &rsa]() {
        BIO_free(certbio);
        BIO_free(kbio);
        X509_free(cert);
        RSA_free(rsa);
    });
    if (certSize > INT_MAX) {
        return false;
    }
    certbio = BIO_new_mem_buf(certBuffer, static_cast<int>(certSize));
    if (certbio == nullptr) {
        return false;
    }
    cert = PEM_read_bio_X509(certbio, nullptr, nullptr, nullptr);
    if (cert == nullptr || !SSL_CTX_use_certificate(context, cert)) {
        return false;
    }
    if (keySize > INT_MAX) {
        return false;
    }
    kbio = BIO_new_mem_buf(keyBuffer, static_cast<int>(keySize));
    if (kbio == nullptr) {
        return false;
    }
    rsa = PEM_read_bio_RSAPrivateKey(kbio, nullptr, nullptr, nullptr);
    if (rsa == nullptr || !SSL_CTX_use_RSAPrivateKey(context, rsa)) {
        return false;
    }
    return true;
}

bool LoadCaFromMemory(SSL_CTX *context, const char *caBuffer, size_t caSize)
{
    if (caSize > INT_MAX) {
        return false;
    }
    BIO *cabio = nullptr;
    Raii freePtr([&cabio]() { BIO_free(cabio); });
    cabio = BIO_new_mem_buf(caBuffer, static_cast<int>(caSize));
    if (cabio == nullptr) {
        return false;
    }
    X509_STORE *store = SSL_CTX_get_cert_store(context);
    if (store == nullptr) {
        return false;
    }
    // maybe more than one certificates in ca files.
    for (int i = 0;; i++) {
        X509 *cacert = nullptr;
        Raii freeCacert([&cacert]() { X509_free(cacert); });
        cacert = PEM_read_bio_X509(cabio, nullptr, nullptr, nullptr);
        if (cacert == nullptr) {
            if (i == 0) {
                return false;
            } else {
                break;
            }
        }
        if (!X509_STORE_add_cert(store, cacert)) {
            return false;
        }
    }
    return true;
}

void LoadSslStrings()
{
    SSL_load_error_strings();
    ERR_load_crypto_strings();
}

std::string GetSslErrorMessage()
{
    auto err = ERR_get_error();
    if (err != 0) {
        std::string msg = "Error: " + std::string(ERR_error_string(err, nullptr));
        auto reason = ERR_reason_error_string(err);
        if (reason != nullptr) {
            msg += ", reason: " + std::string(reason);
        }
        return msg;
    }
    return "ssl is ok";
}

Status ParsePKCS12(const std::string &p12Path, const SensitiveValue &passphrase, SensitiveValue &ca,
                   SensitiveValue &cert, SensitiveValue &key)
{
    EVP_PKEY *pkey = nullptr;
    X509 *crt = nullptr;
    STACK_OF(X509) *caCert = nullptr;
    FILE *file = fopen(p12Path.c_str(), "rb");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        file != nullptr, K_RUNTIME_ERROR,
        FormatString("failed to open file: %s, errno: %d, msg: %s", p12Path, errno, StrErr(errno)));
    Raii releaseFile([file] {
        int result = fclose(file);
        LOG_IF(ERROR, result != 0) << "failed to fclose PKCS#12 file";
    });
    PKCS12 *p12 = d2i_PKCS12_fp(file, nullptr);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(p12 != nullptr, K_RUNTIME_ERROR, "failed to read PKCS#12 file");
    Raii releaseP12([p12] { PKCS12_free(p12); });
    if (!PKCS12_parse(p12, passphrase.GetData(), &pkey, &crt, &caCert)) {
        RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR,
                                FormatString("parse p12 failed: %s", ERR_reason_error_string(ERR_get_error())));
    }
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR((pkey != nullptr && crt != nullptr), K_RUNTIME_ERROR, "failed to parse p12");
    Raii releaseCrtAndKey([crt, pkey, caCert] {
        X509_free(crt);
        EVP_PKEY_free(pkey);
        sk_X509_pop_free(caCert, X509_free);
    });

    BUF_MEM *bptr = nullptr;
    BIO *bio = BIO_new(BIO_s_mem());
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(bio != nullptr, K_RUNTIME_ERROR, "failed to alloc bio mem");
    Raii releaseBio([bio] { BIO_free(bio); });
    // get key
    int result = PEM_write_bio_PrivateKey(bio, pkey, nullptr, nullptr, 0, nullptr, nullptr);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(result != 0, K_RUNTIME_ERROR, "failed to write pkey to bio");
    result = BIO_get_mem_ptr(bio, &bptr);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(result != 0, K_RUNTIME_ERROR, "failed to BIO_get_mem_ptr");
    key = SensitiveValue(bptr->data, bptr->length);
    // get crt
    BIO_reset(bio);
    result = PEM_write_bio_X509(bio, crt);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(result != 0, K_RUNTIME_ERROR, "failed to write cert to bio");
    result = BIO_get_mem_ptr(bio, &bptr);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(result != 0, K_RUNTIME_ERROR, "failed to BIO_get_mem_ptr");
    cert = SensitiveValue(bptr->data, bptr->length);
    // get ca
    std::string tempCa;
    for (int32_t i = 0; caCert && i < sk_X509_num(caCert); i++) {
        BIO_reset(bio);
        result = PEM_write_bio_X509(bio, sk_X509_value(caCert, i));
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(result != 0, K_RUNTIME_ERROR, "failed to write ca to bio");
        result = BIO_get_mem_ptr(bio, &bptr);
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(result != 0, K_RUNTIME_ERROR, "failed to BIO_get_mem_ptr");
        tempCa += std::string(bptr->data, bptr->length);
    }
    ca = SensitiveValue(tempCa);
    return Status::OK();
}

Status DecryptRSAPrivateKeyToMemoryInPemFormat(const std::string &keyPath, const SensitiveValue &passphrase,
                                               SensitiveValue &key)
{
    RSA *rsa = nullptr;
    FILE *fp = fopen(keyPath.c_str(), "r");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        fp != nullptr, K_RUNTIME_ERROR,
        FormatString("failed to open file: %s, errno: %d, msg: %s", keyPath, errno, StrErr(errno)));
    Raii releaseFile([fp, &keyPath] {
        int result = fclose(fp);
        LOG_IF(ERROR, result != 0) << FormatString("failed to close file: %s, errno: %d, msg: %s", keyPath, errno,
                                                   StrErr(errno));
    });
    rsa = PEM_read_RSAPrivateKey(fp, NULL, NULL, (void *)passphrase.GetData());
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(rsa != nullptr, K_RUNTIME_ERROR, "failed to read RSA privateKey");
    Raii releaseRSA([rsa] { RSA_free(rsa); });
    BIO *bio = BIO_new(BIO_s_mem());
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(bio != nullptr, K_RUNTIME_ERROR, "failed to alloc bio mem");
    Raii releaseBIO([bio] { BIO_free(bio); });
    auto ret = PEM_write_bio_RSAPrivateKey(bio, rsa, NULL, NULL, 0, NULL, NULL);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(ret == 1, K_RUNTIME_ERROR, "failed to write bio RSA privateKey");
    char *pemStr;
    long pemLen = BIO_get_mem_data(bio, &pemStr);
    key = SensitiveValue(pemStr, pemLen);
    return Status::OK();
}
}  // namespace datasystem
