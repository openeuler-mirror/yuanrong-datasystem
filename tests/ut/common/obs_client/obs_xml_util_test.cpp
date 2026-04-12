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

/**
 * Description: OBS XML utility unit test.
 */

#include <gtest/gtest.h>

#include <string>
#include <vector>
#include <utility>

#include "datasystem/common/l2cache/obs_client/obs_xml_util.h"

namespace datasystem {
namespace ut {

// --------------- BuildBatchDeleteXml ---------------

TEST(ObsXmlUtilTest, BuildBatchDeleteXmlContainsKeys)
{
    std::vector<std::string> keys = { "key1", "key2", "key3" };
    std::string xml = ObsXmlUtil::BuildBatchDeleteXml(keys);
    EXPECT_NE(xml.find("<Key>key1</Key>"), std::string::npos);
    EXPECT_NE(xml.find("<Key>key2</Key>"), std::string::npos);
    EXPECT_NE(xml.find("<Key>key3</Key>"), std::string::npos);
}

TEST(ObsXmlUtilTest, BuildBatchDeleteXmlQuietTrue)
{
    std::vector<std::string> keys = { "key1" };
    std::string xml = ObsXmlUtil::BuildBatchDeleteXml(keys);
    EXPECT_NE(xml.find("<Quiet>true</Quiet>"), std::string::npos);
}

TEST(ObsXmlUtilTest, BuildBatchDeleteXmlStructure)
{
    std::vector<std::string> keys = { "obj1", "obj2" };
    std::string xml = ObsXmlUtil::BuildBatchDeleteXml(keys);
    // Should contain Delete root element
    EXPECT_NE(xml.find("<Delete>"), std::string::npos);
    EXPECT_NE(xml.find("</Delete>"), std::string::npos);
    // Each key should be wrapped in Object/Key
    EXPECT_NE(xml.find("<Object>"), std::string::npos);
    EXPECT_NE(xml.find("</Object>"), std::string::npos);
}

TEST(ObsXmlUtilTest, BuildBatchDeleteXmlEmptyKeys)
{
    std::vector<std::string> keys;
    std::string xml = ObsXmlUtil::BuildBatchDeleteXml(keys);
    // Should still have Delete and Quiet
    EXPECT_NE(xml.find("<Delete>"), std::string::npos);
    EXPECT_NE(xml.find("<Quiet>true</Quiet>"), std::string::npos);
    // Should NOT have any Object elements
    EXPECT_EQ(xml.find("<Key>"), std::string::npos);
}

// --------------- BuildCompleteMultipartXml ---------------

TEST(ObsXmlUtilTest, BuildCompleteMultipartXmlBasic)
{
    std::vector<std::pair<int, std::string>> parts = { { 1, "\"etag1\"" }, { 2, "\"etag2\"" } };
    std::string xml = ObsXmlUtil::BuildCompleteMultipartXml(parts);
    EXPECT_NE(xml.find("<PartNumber>1</PartNumber>"), std::string::npos);
    EXPECT_NE(xml.find("<ETag>\"etag1\"</ETag>"), std::string::npos);
    EXPECT_NE(xml.find("<PartNumber>2</PartNumber>"), std::string::npos);
    EXPECT_NE(xml.find("<ETag>\"etag2\"</ETag>"), std::string::npos);
}

TEST(ObsXmlUtilTest, BuildCompleteMultipartXmlStructure)
{
    std::vector<std::pair<int, std::string>> parts = { { 1, "\"abc\"" } };
    std::string xml = ObsXmlUtil::BuildCompleteMultipartXml(parts);
    EXPECT_NE(xml.find("<CompleteMultipartUpload>"), std::string::npos);
    EXPECT_NE(xml.find("</CompleteMultipartUpload>"), std::string::npos);
    EXPECT_NE(xml.find("<Part>"), std::string::npos);
    EXPECT_NE(xml.find("</Part>"), std::string::npos);
}

TEST(ObsXmlUtilTest, BuildCompleteMultipartXmlEmpty)
{
    std::vector<std::pair<int, std::string>> parts;
    std::string xml = ObsXmlUtil::BuildCompleteMultipartXml(parts);
    EXPECT_NE(xml.find("<CompleteMultipartUpload>"), std::string::npos);
    // No Part elements when empty
    EXPECT_EQ(xml.find("<PartNumber>"), std::string::npos);
}

// --------------- GetXmlElement ---------------

TEST(ObsXmlUtilTest, GetXmlElementSingleElement)
{
    std::string xml = "<Root><Name>test-value</Name></Root>";
    std::string result = ObsXmlUtil::GetXmlElement(xml, "Name");
    EXPECT_EQ(result, "test-value");
}

TEST(ObsXmlUtilTest, GetXmlElementNested)
{
    std::string xml = "<Root><Outer><Inner>deep-value</Inner></Outer></Root>";
    std::string result = ObsXmlUtil::GetXmlElement(xml, "Inner");
    EXPECT_EQ(result, "deep-value");
}

TEST(ObsXmlUtilTest, GetXmlElementNotFound)
{
    std::string xml = "<Root><Name>value</Name></Root>";
    std::string result = ObsXmlUtil::GetXmlElement(xml, "NonExistent");
    EXPECT_EQ(result, "");
}

TEST(ObsXmlUtilTest, GetXmlElementEmptyContent)
{
    std::string xml = "<Root><Empty></Empty></Root>";
    std::string result = ObsXmlUtil::GetXmlElement(xml, "Empty");
    EXPECT_EQ(result, "");
}

TEST(ObsXmlUtilTest, GetXmlElementReturnsFirstMatch)
{
    std::string xml = "<Root><Item>first</Item><Item>second</Item></Root>";
    std::string result = ObsXmlUtil::GetXmlElement(xml, "Item");
    EXPECT_EQ(result, "first");
}

// --------------- GetXmlElements ---------------

TEST(ObsXmlUtilTest, GetXmlElementsMultiple)
{
    std::string xml = "<Root><Item>a</Item><Item>b</Item><Item>c</Item></Root>";
    auto results = ObsXmlUtil::GetXmlElements(xml, "Item");
    ASSERT_EQ(results.size(), 3u);
    EXPECT_EQ(results[0], "a");
    EXPECT_EQ(results[1], "b");
    EXPECT_EQ(results[2], "c");
}

TEST(ObsXmlUtilTest, GetXmlElementsSingle)
{
    std::string xml = "<Root><Item>only-one</Item></Root>";
    auto results = ObsXmlUtil::GetXmlElements(xml, "Item");
    ASSERT_EQ(results.size(), 1u);
    EXPECT_EQ(results[0], "only-one");
}

TEST(ObsXmlUtilTest, GetXmlElementsNone)
{
    std::string xml = "<Root><Other>value</Other></Root>";
    auto results = ObsXmlUtil::GetXmlElements(xml, "Item");
    EXPECT_EQ(results.size(), 0u);
}

// --------------- ParseInitiateMultipartResponse ---------------

TEST(ObsXmlUtilTest, ParseInitiateMultipartResponseExtractsUploadId)
{
    std::string xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<InitiateMultipartUploadResult>"
        "<Bucket>my-bucket</Bucket>"
        "<Key>my-object</Key>"
        "<UploadId>0004B999EF518A1FE585841299A07AE0</UploadId>"
        "</InitiateMultipartUploadResult>";
    std::string uploadId = ObsXmlUtil::ParseInitiateMultipartResponse(xml);
    EXPECT_EQ(uploadId, "0004B999EF518A1FE585841299A07AE0");
}

TEST(ObsXmlUtilTest, ParseInitiateMultipartResponseMissingUploadId)
{
    std::string xml = "<InitiateMultipartUploadResult><Bucket>bucket</Bucket></InitiateMultipartUploadResult>";
    std::string uploadId = ObsXmlUtil::ParseInitiateMultipartResponse(xml);
    EXPECT_EQ(uploadId, "");
}

TEST(ObsXmlUtilTest, ParseInitiateMultipartResponseEmptyXml)
{
    std::string uploadId = ObsXmlUtil::ParseInitiateMultipartResponse("");
    EXPECT_EQ(uploadId, "");
}

// --------------- ParseListObjectsResponse ---------------

TEST(ObsXmlUtilTest, ParseListObjectsResponseBasic)
{
    std::string xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult>"
        "<IsTruncated>false</IsTruncated>"
        "<Contents>"
        "<Key>file1.txt</Key>"
        "<Size>1024</Size>"
        "</Contents>"
        "<Contents>"
        "<Key>file2.txt</Key>"
        "<Size>2048</Size>"
        "</Contents>"
        "</ListBucketResult>";

    std::vector<std::string> keys;
    std::vector<uint64_t> sizes;
    bool isTruncated = true;
    std::string nextMarker;

    Status rc = ObsXmlUtil::ParseListObjectsResponse(xml, keys, sizes, isTruncated, nextMarker);
    ASSERT_EQ(rc.GetCode(), 0);

    ASSERT_EQ(keys.size(), 2u);
    EXPECT_EQ(keys[0], "file1.txt");
    EXPECT_EQ(keys[1], "file2.txt");

    ASSERT_EQ(sizes.size(), 2u);
    EXPECT_EQ(sizes[0], 1024u);
    EXPECT_EQ(sizes[1], 2048u);

    EXPECT_FALSE(isTruncated);
}

TEST(ObsXmlUtilTest, ParseListObjectsResponseTruncated)
{
    std::string xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult>"
        "<IsTruncated>true</IsTruncated>"
        "<NextMarker>marker-value</NextMarker>"
        "<Contents>"
        "<Key>file1.txt</Key>"
        "<Size>100</Size>"
        "</Contents>"
        "</ListBucketResult>";

    std::vector<std::string> keys;
    std::vector<uint64_t> sizes;
    bool isTruncated = false;
    std::string nextMarker;

    Status rc = ObsXmlUtil::ParseListObjectsResponse(xml, keys, sizes, isTruncated, nextMarker);
    ASSERT_EQ(rc.GetCode(), 0);
    EXPECT_TRUE(isTruncated);
    EXPECT_EQ(nextMarker, "marker-value");
}

TEST(ObsXmlUtilTest, ParseListObjectsResponseEmpty)
{
    std::string xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult>"
        "<IsTruncated>false</IsTruncated>"
        "</ListBucketResult>";

    std::vector<std::string> keys;
    std::vector<uint64_t> sizes;
    bool isTruncated = true;
    std::string nextMarker;

    Status rc = ObsXmlUtil::ParseListObjectsResponse(xml, keys, sizes, isTruncated, nextMarker);
    ASSERT_EQ(rc.GetCode(), 0);
    EXPECT_EQ(keys.size(), 0u);
    EXPECT_EQ(sizes.size(), 0u);
    EXPECT_FALSE(isTruncated);
}

// --------------- ParseErrorResponse ---------------

TEST(ObsXmlUtilTest, ParseErrorResponseExtractsCodeAndMessage)
{
    std::string xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<Error>"
        "<Code>NoSuchKey</Code>"
        "<Message>The specified key does not exist.</Message>"
        "</Error>";

    std::string code, message;
    ObsXmlUtil::ParseErrorResponse(xml, code, message);
    EXPECT_EQ(code, "NoSuchKey");
    EXPECT_EQ(message, "The specified key does not exist.");
}

TEST(ObsXmlUtilTest, ParseErrorResponseDifferentError)
{
    std::string xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<Error>"
        "<Code>AccessDenied</Code>"
        "<Message>Access Denied.</Message>"
        "</Error>";

    std::string code, message;
    ObsXmlUtil::ParseErrorResponse(xml, code, message);
    EXPECT_EQ(code, "AccessDenied");
    EXPECT_EQ(message, "Access Denied.");
}

TEST(ObsXmlUtilTest, ParseErrorResponseEmptyXml)
{
    std::string code = "initial-code";
    std::string message = "initial-message";
    ObsXmlUtil::ParseErrorResponse("", code, message);
    // With empty XML, outputs should be empty or unchanged depending on implementation
    // At minimum, it should not crash
}

TEST(ObsXmlUtilTest, ParseErrorResponseMissingFields)
{
    std::string xml = "<Error></Error>";
    std::string code, message;
    ObsXmlUtil::ParseErrorResponse(xml, code, message);
    // Should not crash, code and message should be empty
    EXPECT_EQ(code, "");
    EXPECT_EQ(message, "");
}

}  // namespace ut
}  // namespace datasystem
