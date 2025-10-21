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
 * Description: http request test.
 */
#include "common.h"
#include "datasystem/common/httpclient/http_message.h"
#include "datasystem/common/httpclient/http_request.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/logging.h"

namespace datasystem {
namespace ut {

class HttpRequestTest : public CommonTest {
public:
    void SetUp() override
    {
        Logging::GetInstance()->Start("ds_llt", true, 1);
    };
};

TEST_F(HttpRequestTest, UrlEscapeTest)
{
    EXPECT_EQ(EscapeURL("", false), "");
 
    std::string url = "https://www.example.com/path/to/resource?param=value 1+2*3~4";
    std::string expectedNotReplacePath =
        "https%3A%2F%2Fwww.example.com%2Fpath%2Fto%2Fresource%3Fparam%3Dvalue%201%2B2%2A3~4";
    EXPECT_EQ(EscapeURL(url, false), expectedNotReplacePath);
    EXPECT_EQ(EscapeURL(url, true), "https%3A//www.example.com/path/to/resource%3Fparam%3Dvalue%201%2B2%2A3~4");
}

TEST_F(HttpRequestTest, GetCanonicalRequestTest)
{
    HttpRequest req;
    req.SetMethod(HttpMethod::GET);
    req.SetUrl("https://www.example.com/path/to/resource");
    req.AddQueryParam("p2", "value2");  // need sort
    req.AddQueryParam("p3", "value3");
    req.AddQueryParam("p1", "value1");
    req.AddHeader("Host", "example.com"); // need sort
    req.AddHeader("h2", "**");
 
    std::string expected =
        "GET\n"
        "/path/to/resource\n"
        "p1=value1&p2=value2&p3=value3\n"
        "host:example.com\nh2:**\n\n"
        "host;h2\n"
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    std::string canonicalRequest;
    DS_ASSERT_OK(req.GetCanonicalRequest(true, canonicalRequest));
    EXPECT_EQ(canonicalRequest, expected);
}
 
TEST_F(HttpRequestTest, GetCanonicalRequestWhenEmptyArgsTest)
{
    std::string expected = "GET\n/\n\n\n\ne3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    HttpRequest req;
    req.SetMethod(HttpMethod::GET);
    req.AddHeader(HEADER_AUTHORIZATION, "**");
    req.AddHeader(HEADER_CONNECTION, "**");
    std::string canonicalRequest;
    DS_ASSERT_OK(req.GetCanonicalRequest(true, canonicalRequest));
    EXPECT_EQ(canonicalRequest, expected);
}

}  // namespace ut
}  // namespace datasystem