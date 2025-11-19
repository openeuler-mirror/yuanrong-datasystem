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
 * Description: update object data test
 */
#include <gtest/gtest.h>

#include <securec.h>

#include "oc_client_common.h"

// user define class
class DataClass {
public:
    std::string s;
    int i;
    bool b;
    float f;
    double d;
};

// | 4-bytes total length | d.s | d.i | d.b | d.f | d.d |
std::string Serialize(const DataClass &d)
{
    std::string content;
    uint32_t totalLength = d.s.size() + sizeof(d.b) + sizeof(d.d) + sizeof(d.f) + sizeof(d.i);
    content.reserve(totalLength);

    content.append(std::string((char *)&totalLength, sizeof(totalLength)));
    content.append(d.s);
    content.append(std::string((char *)&d.i, sizeof(d.i)));
    content.append(std::string((char *)&d.b, sizeof(d.b)));
    content.append(std::string((char *)&d.f, sizeof(d.f)));
    content.append(std::string((char *)&d.d, sizeof(d.d)));
    return content;
}

// | 4-bytes total length | d.s | d.i | d.b | d.f | d.d |
DataClass Deserialize(const std::string &data)
{
    DataClass d;
    auto *p = data.data();
    uint32_t totalLength = *(uint32_t *)p;
    p += sizeof(uint32_t);
    uint32_t dsLength = totalLength - (sizeof(d.b) + sizeof(d.d) + sizeof(d.f) + sizeof(d.i));
    d.s = std::string(p, dsLength);
    p += dsLength;
    d.i = *(int *)p;
    p += sizeof(int);
    d.b = *(bool *)p;
    p += sizeof(bool);
    d.f = *(float *)p;
    p += sizeof(float);
    d.d = *(double *)p;
    return d;
}

namespace datasystem {
namespace st {
class OCClientUpdateTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts)
    {
        opts.numWorkers = 2;
        opts.numEtcd = 1;
    }
};

TEST_F(OCClientUpdateTest, LEVEL1_UpdateStringTest)
{
    std::shared_ptr<ObjectClient> client;
    std::shared_ptr<ObjectClient> client2;
    std::shared_ptr<ObjectClient> client3;
    InitTestClient(0, client);
    InitTestClient(0, client2);
    InitTestClient(1, client3);

    // create
    std::string objectKey = NewObjectKey();
    std::shared_ptr<Buffer> data;
    int bufferSize = 10;
    DS_ASSERT_OK(client->Create(objectKey, bufferSize, CreateParam{}, data));
    std::string test = GenRandomString(bufferSize);
    DS_ASSERT_OK(data->MemoryCopy(const_cast<char *>(test.data()), test.length()));
    DS_ASSERT_OK(data->Publish());

    // update success
    std::string updateTest = GenRandomString();
    std::vector<std::string> getObjList{ objectKey };
    std::vector<Optional<Buffer>> dataList;
    DS_ASSERT_OK(client->Get(getObjList, 0, dataList));
    ASSERT_TRUE(NotExistsNone(dataList));
    DS_ASSERT_OK(dataList[0]->MemoryCopy(const_cast<char *>(updateTest.data()), updateTest.length()));
    DS_ASSERT_OK(dataList[0]->Seal());

    // get local update
    std::vector<Optional<Buffer>> updateData;
    DS_ASSERT_OK(client2->Get(getObjList, 0, updateData));
    ASSERT_TRUE(NotExistsNone(updateData));
    std::string result((const char *)updateData[0]->ImmutableData(), updateTest.length());
    ASSERT_EQ(result, updateTest);

    // get remote update
    std::vector<Optional<Buffer>> updateData2;
    DS_ASSERT_OK(client3->Get(getObjList, 0, updateData2));
    ASSERT_TRUE(NotExistsNone(updateData2));
    std::string result2((const char *)updateData2[0]->ImmutableData(), updateTest.length());
    ASSERT_EQ(result2, updateTest);
}

TEST_F(OCClientUpdateTest, LocalUpdateDataClassTest)
{
    std::shared_ptr<ObjectClient> client;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client);
    InitTestClient(0, client2);

    // create
    std::string objectKey = NewObjectKey();
    std::shared_ptr<Buffer> data;
    DS_ASSERT_OK(client->Create(objectKey, 100, CreateParam{}, data));
    DataClass myDataClass;
    myDataClass.s = "abcdefg";
    myDataClass.b = true;
    myDataClass.d = 1.1;
    myDataClass.f = 2.2;
    myDataClass.i = 3;

    // serialize DataClass
    std::string content = Serialize(myDataClass);
    DS_ASSERT_OK(data->MemoryCopy(const_cast<char *>(content.data()), content.length()));
    DS_ASSERT_OK(data->Publish());

    // update
    myDataClass.s = "12345";
    myDataClass.b = false;
    myDataClass.d = 1.3;
    myDataClass.f = 2.3;
    myDataClass.i = 4;
    // serialize DataClass
    content = Serialize(myDataClass);
    // 2.record data size
    size_t datasize = content.length();
    // 3.get buffer
    std::vector<std::string> getObjList = { objectKey };
    std::vector<Optional<Buffer>> dataList;
    DS_ASSERT_OK(client->Get(getObjList, 0, dataList));
    ASSERT_TRUE(NotExistsNone(dataList));
    // 4.write binary data to buffer and Publish again
    DS_ASSERT_OK(dataList[0]->MemoryCopy(const_cast<char *>(content.data()), content.length()));
    DS_ASSERT_OK(dataList[0]->Publish());

    // get update
    std::vector<Optional<Buffer>> updateData;
    DS_ASSERT_OK(client2->Get(getObjList, 0, updateData));
    ASSERT_TRUE(NotExistsNone(dataList));
    // 1.read by data size
    std::string result((const char *)updateData[0]->ImmutableData(), datasize);
    ASSERT_EQ(result, content);
    // 2.deserialize
    DataClass updateResult = Deserialize(result);
    ASSERT_EQ(updateResult.i, myDataClass.i);
    ASSERT_FLOAT_EQ(updateResult.f, myDataClass.f);
    ASSERT_DOUBLE_EQ(updateResult.d, myDataClass.d);
    ASSERT_EQ(updateResult.b, myDataClass.b);
    ASSERT_EQ(updateResult.s, myDataClass.s);
}

TEST_F(OCClientUpdateTest, UpdateFailedTest)
{
    std::shared_ptr<ObjectClient> client;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client);
    InitTestClient(1, client2);

    // create
    std::string objectKey = NewObjectKey();
    std::shared_ptr<Buffer> data;
    int bufferSize = 8;
    DS_ASSERT_OK(client->Create(objectKey, bufferSize, CreateParam{}, data));
    std::string test = "abcdefg";
    DS_ASSERT_OK(data->MemoryCopy(const_cast<char *>(test.data()), test.length()));
    DS_ASSERT_OK(data->Publish());

    // update failed
    std::string updateTest = "abcdefghijklm";
    std::vector<std::string> getObjList{ objectKey };
    std::vector<Optional<Buffer>> dataList;
    DS_ASSERT_OK(client->Get(getObjList, 0, dataList));
    ASSERT_TRUE(NotExistsNone(dataList));
    ASSERT_EQ(dataList[0]->MemoryCopy(const_cast<char *>(updateTest.data()), -1).GetCode(), K_INVALID);
    ASSERT_EQ(dataList[0]->MemoryCopy(nullptr, 1).GetCode(), K_INVALID);
    ASSERT_EQ(dataList[0]->MemoryCopy(const_cast<char *>(updateTest.data()), updateTest.length()).GetCode(), K_INVALID);
    // data in buffer is uncompleted
    DS_ASSERT_OK(dataList[0]->Seal());
}

TEST_F(OCClientUpdateTest, BufferInvalidedTest)
{
    std::shared_ptr<ObjectClient> client;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client);
    InitTestClient(1, client2);

    // client1 create
    std::string objectKey = NewObjectKey();
    std::shared_ptr<Buffer> data;
    int bufferSize = 8;
    CreateParam param{ .consistencyType = ConsistencyType::CAUSAL };
    DS_ASSERT_OK(client->Create(objectKey, bufferSize, param, data));
    std::string test = "abcdefg";
    DS_ASSERT_OK(data->MemoryCopy(const_cast<char *>(test.data()), test.length()));
    DS_ASSERT_OK(data->Publish());

    // client2 get
    std::vector<std::string> getObjList{ objectKey };
    std::vector<Optional<Buffer>> dataList;
    DS_ASSERT_OK(client2->Get(getObjList, 0, dataList));

    // client1 update
    DS_ASSERT_OK(data->Seal());
    DS_ASSERT_OK(client2->Get(getObjList, 0, dataList));
    ASSERT_TRUE(NotExistsNone(dataList));
}

TEST_F(OCClientUpdateTest, MultiUpdateTest)
{
    std::shared_ptr<ObjectClient> client;
    std::shared_ptr<ObjectClient> client2;
    std::shared_ptr<ObjectClient> client3;
    InitTestClient(0, client);
    InitTestClient(0, client2);
    InitTestClient(1, client3);

    // create
    std::string objectKey = NewObjectKey();
    std::shared_ptr<Buffer> workerOBuffer;
    int bufferSize = 8;
    CreateParam param{ .consistencyType = ConsistencyType::CAUSAL };
    DS_ASSERT_OK(client->Create(objectKey, bufferSize, param, workerOBuffer));
    std::string worker0_data = "abcdefg";
    DS_ASSERT_OK(workerOBuffer->MemoryCopy(const_cast<char *>(worker0_data.data()), worker0_data.length()));
    DS_ASSERT_OK(workerOBuffer->Publish());

    // worker1 get buffer to update
    std::string worker1_data = "12345";
    std::vector<std::string> getObjList{ objectKey };
    std::vector<Optional<Buffer>> dataList;
    DS_ASSERT_OK(client3->Get(getObjList, 0, dataList));
    ASSERT_TRUE(NotExistsNone(dataList));
    // worker1 update data
    DS_ASSERT_OK(dataList[0]->MemoryCopy(const_cast<char *>(worker1_data.data()), worker1_data.length()));
    DS_ASSERT_OK(dataList[0]->Publish());

    //     worker1 get updated data
    std::vector<Optional<Buffer>> updateData;
    DS_ASSERT_OK(client3->Get(getObjList, 0, updateData));
    ASSERT_TRUE(NotExistsNone(updateData));
    std::string worker1Result1((const char *)updateData[0]->ImmutableData(), worker1_data.length());
    ASSERT_EQ(worker1Result1, worker1_data);

    // worker0 get updated data
    dataList.clear();
    DS_ASSERT_OK(client->Get(getObjList, 0, dataList));
    ASSERT_TRUE(NotExistsNone(dataList));
    std::string worker0Result1((const char *)dataList[0]->ImmutableData(), worker1_data.length());
    ASSERT_EQ(worker0Result1, worker1_data);

    dataList.clear();
    DS_ASSERT_OK(client2->Get(getObjList, 0, dataList));
    ASSERT_TRUE(NotExistsNone(dataList));
    std::string worker0Result2((const char *)dataList[0]->ImmutableData(), worker1_data.length());
    ASSERT_EQ(worker0Result2, worker1_data);

    // use worker0Buffer recover updated data by worker1 before
    DS_ASSERT_OK(workerOBuffer->MemoryCopy(const_cast<char *>(worker0_data.data()), worker0_data.length()));
    // worker0 update data
    DS_ASSERT_OK(workerOBuffer->Seal());
    // worker0 get updated data
    std::vector<Optional<Buffer>> updateData2;
    DS_ASSERT_OK(client2->Get(getObjList, 0, updateData2));
    ASSERT_TRUE(NotExistsNone(updateData2));
    std::string worker0Result3((const char *)updateData2[0]->ImmutableData(), worker0_data.length());
    ASSERT_EQ(worker0Result3, worker0_data);
    // worker1 get updated data
    updateData2.clear();
    DS_ASSERT_OK(client3->Get(getObjList, 0, updateData2));
    ASSERT_TRUE(NotExistsNone(updateData2));
    std::string worker1Result2((const char *)updateData2[0]->ImmutableData(), worker0_data.length());
    ASSERT_EQ(worker1Result2, worker0_data);
}

TEST_F(OCClientUpdateTest, UpdateDataTest)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    std::shared_ptr<ObjectClient> client3;
    InitTestClient(0, client1);
    InitTestClient(0, client2);
    InitTestClient(1, client3);
    // create
    std::string objectKey = NewObjectKey();
    std::shared_ptr<Buffer> data;
    int bufferSize = 8;
    CreateParam param{ .consistencyType = ConsistencyType::CAUSAL };
    DS_ASSERT_OK(client1->Create(objectKey, bufferSize, param, data));
    std::string worker0_data = "abcdefg";
    DS_ASSERT_OK(data->MemoryCopy(const_cast<char *>(worker0_data.data()), worker0_data.length()));
    DS_ASSERT_OK(data->Publish());
    // worker1 update, worker1 buffer is valid
    std::string worker1_data = "1234567";
    std::vector<std::string> getObjList{ objectKey };
    std::vector<Optional<Buffer>> dataList;
    DS_ASSERT_OK(client3->Get(getObjList, 0, dataList));
    ASSERT_TRUE(NotExistsNone(dataList));
    DS_ASSERT_OK(dataList[0]->MemoryCopy(const_cast<char *>(worker1_data.data()), worker1_data.length()));
    DS_ASSERT_OK(dataList[0]->Seal());
    std::vector<Optional<Buffer>> updateData;
    DS_ASSERT_OK(client2->Get(getObjList, 0, updateData));
    ASSERT_TRUE(NotExistsNone(updateData));
    std::string result((const char *)updateData[0]->ImmutableData(), worker1_data.length());
    ASSERT_EQ(result, worker1_data);
}
}  // namespace st
}  // namespace datasystem
