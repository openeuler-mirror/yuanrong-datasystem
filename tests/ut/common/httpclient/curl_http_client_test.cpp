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
 * Description: curl http client test.
 */

#include <sstream>
#include "common.h"
#include "datasystem/common/httpclient/curl_http_client.h"
#include "datasystem/common/httpclient/http_response.h"
#include "datasystem/common/log/logging.h"

namespace datasystem {
namespace ut {

class CurlHttpClientTest : public CommonTest {
public:
    void SetUp() override
    {
        Logging::GetInstance()->Start("ds_llt", true, 1);
    };
};

TEST_F(CurlHttpClientTest, TestHttpMessageHeader)
{
    HttpMessage httpMessage;
    httpMessage.AddHeader("test", "testv");
    const std::string res = httpMessage.GetHeader("test");
    ASSERT_TRUE(res == "testv");
}

TEST_F(CurlHttpClientTest, TestHttpMessageHeaderLeftRef)
{
    HttpMessage httpMessage;
    std::string key = "test";
    std::string value = "testv";
    httpMessage.AddHeader(key, value);
    const std::string res = httpMessage.GetHeader(key);
    ASSERT_TRUE(res == "testv");
}

TEST_F(CurlHttpClientTest, TestHttpMessageGetHeaders)
{
    HttpMessage httpMessage;
    httpMessage.AddHeader("test", "testv");
    auto res = httpMessage.Headers();
    ASSERT_TRUE(res["test"] == "testv");
}

TEST_F(CurlHttpClientTest, TestHttpMessageHeaderRemove)
{
    HttpMessage httpMessage;
    httpMessage.AddHeader("test", "testv");
    const std::string res = httpMessage.GetHeader("test");
    ASSERT_TRUE(res == "testv");
    httpMessage.RemoveHeader("test");
    const std::string res1 = httpMessage.GetHeader("test");
    ASSERT_TRUE(res1.empty());
}

TEST_F(CurlHttpClientTest, TestHttpMessageBody)
{
    HttpMessage httpMessage;
    std::shared_ptr<std::stringstream> body = std::make_shared<std::stringstream>("123");
    httpMessage.SetBody(body);
    std::shared_ptr<std::iostream> &bodyRef = httpMessage.GetBody();
    char ptr[4]{ 0 };
    bodyRef->read(ptr, 10);
    ASSERT_TRUE(bodyRef->gcount() == 3);
}

TEST_F(CurlHttpClientTest, TestHttpRequestMethod)
{
    HttpRequest httpRequest;
    httpRequest.SetMethod(HttpMethod::PUT);
    HttpMethod method = httpRequest.GetMethod();
    ASSERT_TRUE(method == HttpMethod::PUT);
}

TEST_F(CurlHttpClientTest, TestHttpRequestUrl)
{
    HttpRequest httpRequest;
    httpRequest.SetUrl("https://123.com");
    const std::string url = httpRequest.GetUrl();
    ASSERT_TRUE(url == "https://123.com");
}

TEST_F(CurlHttpClientTest, TestHttpRequestRequestTimout)
{
    HttpRequest httpRequest;
    httpRequest.SetRequestTimeoutMs(30000);
    ASSERT_TRUE(httpRequest.GetRequestTimeoutMs() == 30000);
}

TEST_F(CurlHttpClientTest, TestHttpRequestConnectTimout)
{
    HttpRequest httpRequest;
    httpRequest.SetConnectTimeoutMs(50000);
    ASSERT_TRUE(httpRequest.GetConnectTimeoutMs() == 50000);
}

TEST_F(CurlHttpClientTest, TestHttpResponseDefaultStatus)
{
    HttpResponse httpResponse;
    ASSERT_TRUE(httpResponse.GetStatus() == HttpResponse::STATUS_CLIENT_ERR);
}

TEST_F(CurlHttpClientTest, TestHttpResponseStatus)
{
    HttpResponse httpResponse;
    httpResponse.SetStatus(200);
    int code = httpResponse.GetStatus();
    ASSERT_TRUE(code == 200);
}

TEST_F(CurlHttpClientTest, TestHttpResponseSuccess)
{
    HttpResponse httpResponse;
    httpResponse.SetStatus(200);
    ASSERT_TRUE(httpResponse.IsSuccess());
}

TEST_F(CurlHttpClientTest, TestHttpResponseFail)
{
    HttpResponse httpResponse;
    httpResponse.SetStatus(400);
    ASSERT_FALSE(httpResponse.IsSuccess());
}

TEST_F(CurlHttpClientTest, TestHttpRecvHeader)
{
    char header[] = { 'a', ':', ' ', 'b', '\r' };
    HttpResponse response;

    size_t res = CurlHttpClient::RecvHeadersCallBack(header, 5, 1, &response);
    std::map<std::string, std::string> h = response.Headers();
    ASSERT_TRUE(res == 5);
    ASSERT_TRUE(h["a"] == "b");
}

TEST_F(CurlHttpClientTest, TestHttpRecvHeaderWithoutEnd)
{
    char header[] = { 'a', ':', ' ', 'b' };
    HttpResponse response;

    size_t res = CurlHttpClient::RecvHeadersCallBack(header, 4, 1, &response);
    std::map<std::string, std::string> h = response.Headers();
    ASSERT_TRUE(res == 4);
    ASSERT_TRUE(h["a"] == " b");
}

TEST_F(CurlHttpClientTest, TestHttpRecvHeaderInvalid)
{
    char header[] = { 'a', '=', ' ', 'b' };
    HttpResponse response;

    size_t res = CurlHttpClient::RecvHeadersCallBack(header, 4, 1, &response);
    std::map<std::string, std::string> h = response.Headers();
    ASSERT_TRUE(res == 4);
    ASSERT_TRUE(h.empty());
}

TEST_F(CurlHttpClientTest, TestHttpRecvBody)
{
    char header[] = { 'a', '=', ' ', 'b' };

    std::shared_ptr<std::stringstream> s = std::make_shared<std::stringstream>();
    HttpResponse response;
    response.SetBody(s);

    size_t res = CurlHttpClient::RecvBodyCallBack(header, 4, 1, &response);
    ASSERT_TRUE(res == 4);
    ASSERT_TRUE(s->str() == "a= b");
}

TEST_F(CurlHttpClientTest, TestHttpRecvBodyNull)
{
    char header[] = { 'a', '=', ' ', 'b' };
    HttpResponse response;
    size_t res = CurlHttpClient::RecvBodyCallBack(header, 4, 1, &response);
    size_t RECV_ERR = -1;
    ASSERT_TRUE(res == RECV_ERR);
}

TEST_F(CurlHttpClientTest, TestHttpRecvHttpResponseNull)
{
    char header[] = { 'a', '=', ' ', 'b' };
    size_t res = CurlHttpClient::RecvBodyCallBack(header, 4, 1, nullptr);
    size_t RECV_ERR = -1;
    ASSERT_TRUE(res == RECV_ERR);
}

TEST_F(CurlHttpClientTest, TestHttpSendBodyCallBack)
{
    std::shared_ptr<std::stringstream> s = std::make_shared<std::stringstream>("hello");
    HttpRequest request;
    request.SetBody(s);

    char buffer[6] = { 0 };
    size_t res = CurlHttpClient::SendBodyCallBack(buffer, 6, 1, &request);
    ASSERT_TRUE(res == 5);
    ASSERT_TRUE(buffer[4] == 'o');
}

TEST_F(CurlHttpClientTest, TestHttpSendBodyNull)
{
    char buffer[6] = { 0 };
    size_t res = CurlHttpClient::SendBodyCallBack(buffer, 6, 1, nullptr);
    ASSERT_TRUE(res == 0);
}

TEST_F(CurlHttpClientTest, TestClientError)
{
    CurlHttpClient client;
    std::shared_ptr<HttpRequest> req = std::make_shared<HttpRequest>();
    std::shared_ptr<HttpResponse> resp = std::make_shared<HttpResponse>();
    client.Send(req, resp);
    ASSERT_EQ(-1, resp->GetStatus());
}

TEST_F(CurlHttpClientTest, TestMutipleClient)
{
    CurlHttpClient client;
    CurlHttpClient client1;
    std::shared_ptr<HttpRequest> req = std::make_shared<HttpRequest>();
    std::shared_ptr<HttpResponse> resp = std::make_shared<HttpResponse>();
    client.Send(req, resp);
    ASSERT_EQ(-1, resp->GetStatus());
    client1.Send(req, resp);
    ASSERT_EQ(-1, resp->GetStatus());
}

TEST_F(CurlHttpClientTest, TestCurlPoolManagerDefaultIsEmpty)
{
    CurlHandleManager poolManager;
    ASSERT_FALSE(poolManager.HasIdleResource());
}

TEST_F(CurlHttpClientTest, TestCurlPoolManagerHasIdelResource)
{
    CurlHandleManager poolManager;
    CURL *curl = curl_easy_init();
    poolManager.Release(curl);
    ASSERT_TRUE(poolManager.HasIdleResource());
}

TEST_F(CurlHttpClientTest, TestCurlPoolManagerIdelResourceHasBeenUsed)
{
    CurlHandleManager poolManager;
    CURL *curl = curl_easy_init();
    poolManager.Release(curl);
    curl = poolManager.Acquire();
    ASSERT_FALSE(poolManager.HasIdleResource());
}

TEST_F(CurlHttpClientTest, TestCurlPoolManagerShutdonw)
{
    CurlHandleManager poolManager;
    CURL *curl = curl_easy_init();
    CURL *curl1 = curl_easy_init();
    poolManager.Release(curl);
    poolManager.Release(curl1);
    std::vector<CURL *> shutdownList = poolManager.ShutdownAndWait(2);
    ASSERT_TRUE(shutdownList.size() == 2);
}

TEST_F(CurlHttpClientTest, TestCurlPoolManagerShutdonwNeedWait)
{
    CurlHandleManager poolManager;
    CURL *curl = curl_easy_init();
    CURL *curl1 = curl_easy_init();
    poolManager.Release(curl);
    poolManager.Release(curl1);
    std::vector<CURL *> shutdownList;
    // acquire one, and shutdown; shutdown need wait the give back.
    CURL *curl2 = poolManager.Acquire();
    std::thread t([&shutdownList, &poolManager]() { shutdownList = poolManager.ShutdownAndWait(2); });
    ASSERT_TRUE(shutdownList.empty());
    poolManager.Release(curl2);
    t.join();
    ASSERT_TRUE(shutdownList.size() == 2);
}

TEST_F(CurlHttpClientTest, TestCurlPoolResourceCanBeReUse)
{
    CurlHandlePool pool;
    CURL *curl = pool.Acquire();
    pool.Release(curl, false);
    CURL *curl1 = pool.Acquire();
    ASSERT_TRUE(curl == curl1);
    pool.Release(curl1, false);
}

TEST_F(CurlHttpClientTest, TestCurlPoolAquireTwiceNotTheSameResource)
{
    CurlHandlePool pool;
    CURL *curl = pool.Acquire();
    CURL *curl1 = pool.Acquire();
    ASSERT_TRUE(curl != curl1);
    pool.Release(curl, false);
    pool.Release(curl1, false);
}

TEST_F(CurlHttpClientTest, TestCurlSeek)
{
    std::string data = "Value";
    HttpRequest req;
    auto body = std::make_shared<std::stringstream>();
    (*body) << data;
    req.SetBody(body);

    char buff[32] = { 0 };
    // test read body
    auto size = CurlHttpClient::SendBodyCallBack(buff, sizeof(buff), 1, &req);
    ASSERT_EQ(data, std::string(buff, size));

    // test read body again.
    size = CurlHttpClient::SendBodyCallBack(buff, sizeof(buff), 1, &req);
    ASSERT_EQ(size, 0ul);

    // test invalid seek argument.
    auto ret = CurlHttpClient::SeekBodyCallBack(&req, 0, SEEK_CUR);
    ASSERT_EQ(ret, CURL_SEEKFUNC_CANTSEEK);

    // test null
    ret = CurlHttpClient::SeekBodyCallBack(nullptr, 0, SEEK_SET);
    ASSERT_EQ(ret, CURL_SEEKFUNC_FAIL);

    // seek to begin.
    ret = CurlHttpClient::SeekBodyCallBack(&req, 0, SEEK_SET);
    ASSERT_EQ(ret, CURL_SEEKFUNC_OK);

    // read again.
    size = CurlHttpClient::SendBodyCallBack(buff, sizeof(buff), 1, &req);
    ASSERT_EQ(data, std::string(buff, size));
}
}  // namespace ut
}  // namespace datasystem