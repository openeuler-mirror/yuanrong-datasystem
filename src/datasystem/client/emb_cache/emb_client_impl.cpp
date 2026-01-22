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
 * Description: emb client implementation.
 */

// new
// include EmbTableMeta.h
// include EmbTableBucket.h
// include ShareObjectMaster.h
// include EmbTableClient.h
#include "emb_client_impl.h"
// #include "datasystem/client/emb_cache/emb_table_bucket.h"
// #include "datasystem/client/emb_cache/emb_table_meta.h"

#include "hash_function/xxhash.h"
#include "hash_function/city.h"
#include "hash_function/MurmurHash3.h"
#include "datasystem/common/perf/perf_manager.h"
#include <filesystem>

namespace datasystem {
namespace emb_cache {
namespace {
    uint64_t CallXXHash(uint64_t key) {
        return XXH64(&key, sizeof(key), 0);
    }

    uint64_t CallMurmurHash(uint64_t key) {
        uint64_t out[2];
        MurmurHash3_x64_128(&key, sizeof(key), 0, out);
        return out[0];
    }

    uint64_t CallCityHash(uint64_t key) {
        return CityHash64(reinterpret_cast<const char*>(&key), sizeof(key));
    }

} // namespace

HashFuncPtr GetHashExecutor(HashFunction hashFunction) {
    static const std::map<HashFunction, HashFuncPtr> kHashFunctionMap = {
        {HashFunction::XXHASH64,        CallXXHash},
        {HashFunction::MURMURHASH3_X64, CallMurmurHash},
        {HashFunction::CITYHASH64,      CallCityHash}
    };

    auto it = kHashFunctionMap.find(hashFunction);
    if (it != kHashFunctionMap.end()) {
        return it->second;
    } else {
        LOG(ERROR) << "Unknown hash function: " << static_cast<uint32_t>(hashFunction);
        return nullptr;
    }
}

EmbClientImpl::EmbClientImpl(const ConnectOptions &connectOptions)
{
    // 构造master
    // 处理 options 的逻辑
}

Status EmbClientImpl::Init(const InitParams &params,
                           std::shared_ptr<EmbClient> embClient,
                           const std::string &etcdServer, 
                           std::shared_ptr<KVClient> kvClient,
                           const std::string &localPath)
{
    // 1.client加入map管理
    // 2.创建TableMeta对象 ,调用EmbImpl的函数CreateAndSaveTableMeta
    // 3.tableMeta加入map管理，同时将强引用交给client管理

    // EmbTableMeta tableMeta;
    master_ = std::make_shared<datasystem::master::ShareObjectMaster>(etcdServer, kvClient, localPath, params.tableKey);
    tableMeta_ = std::make_shared<EmbTableMeta>(embClient->kvClientImpl_);
    auto rc = CreateTableMeta(params, tableMeta_);
    if (rc.IsError()) {
        LOG(ERROR) << "EmbClientImpl CreateTableMeta Failed";
    }

    // EmbTableBucket tableBucket;
    // 结构体
    TableMetaInfo tableMetaInfo;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(tableMeta_->QueryTableMetaInfo(params.tableKey, tableMetaInfo), "QueryTableMetaInfo failed");

    // 开始打印 tableMetaInfo 的详细内容
    LOG(INFO) << "--- TableMetaInfo Details ---";
    LOG(INFO) << "Table Key: "       << tableMetaInfo.tableKey;
    LOG(INFO) << "Table Index: "     << tableMetaInfo.tableIndex;
    LOG(INFO) << "Table Name: "      << tableMetaInfo.tableName;
    LOG(INFO) << "Dim Size: "        << tableMetaInfo.dimSize;
    LOG(INFO) << "Table Capacity: "  << tableMetaInfo.tableCapacity;
    LOG(INFO) << "Bucket Num: "      << tableMetaInfo.bucketNum;
    LOG(INFO) << "Bucket Capacity: " << tableMetaInfo.bucketCapacity;
    // 注意：hashFunction 通常是枚举或对象，直接打印可能会报错，建议转为整数
    LOG(INFO) << "Hash Function: "   << static_cast<int>(tableMetaInfo.hashFunction);
    LOG(INFO) << "-----------------------------";

    EmbTableBucket::BucketInfo bucketInfo;
    bucketInfo.tableKey = tableMetaInfo.tableKey;
    bucketInfo.tableIndex = tableMetaInfo.tableIndex;
    bucketInfo.tableName = tableMetaInfo.tableName;
    bucketInfo.dimSize = tableMetaInfo.dimSize;
    bucketInfo.bucketNum = tableMetaInfo.bucketNum;
    bucketInfo.bucketCapacity = tableMetaInfo.bucketCapacity;
    // 这里我会将tableMetaInfo的枚举类转为hash算法的指针，传递给EmbTableBucket，所以bufferInfo.hashFunction是指针
    bucketInfo.hashFunction = GetHashExecutor(tableMetaInfo.hashFunction);

    tableBucket_ = std::make_shared<EmbTableBucket>(bucketInfo, embClient->kvClientImpl_, master_);
    queryThreadPool_ = std::make_unique<ThreadPool>(1, tableMetaInfo.bucketNum);

    // Init master_
    std::vector<std::string> bucketKeys;
    bucketKeys.reserve(params.bucketNum);
    for (size_t i = 0; i < params.bucketNum; ++i) {
        // 拼接格式：tablekey + "bucket" + 数字
        std::string value = params.tableKey + "_" + std::to_string(i);
        bucketKeys.emplace_back(value);
    }
    auto result = master_->Init(bucketKeys, params.dimSize);
    if (result.IsError()) {
        LOG(ERROR) << "EmbClientImpl CreateTableMeta Failed";
    }
    return Status::OK();
}

Status EmbClientImpl::CreateTableMeta(InitParams params, std::shared_ptr<EmbTableMeta>& tableMeta)
{
    // 1.构造tableMeta，调用EMbTableBucket的CreateTable函数
    // 存储是tableMeta的事情
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(tableMeta->CreateTableMeta(params), "table call CreateTableMeta Failed");

    return Status::OK();
}

// Status EmbClientImpl::GetClient(std::string tableKey, std::shared_ptr<EmbClient>& embClient)
// {
//     // 1.如果map中存在，那就从map拿
//     // 2.如果map中不存在，调用QueryTableMeta(tableKey, tableMetaInfo),拿到struct tableMetaInfo->InitParams, 调用Client构造函数和Init(params)构造一个新的client，同时还要将其添加到map中缓存管理
//     // 从map中获取tableKey对应的client
//     auto it = EmbTableClientMap.find(tableKey);
//     if (it != EmbTableClientMap.end()) {
//         embClient = it->second.lock();
//     } else {
//         EmbTableMeta tableMeta;
//         TableMetaInfo tableMetaInfo;
//         RETURN_IF_NOT_OK_PRINT_ERROR_MSG(tableMeta.QueryTableMetaInfo(tableKey, tableMetaInfo),
//                                          "QueryTableMetaInfo failed when call EmbClientImpl::GetClient");
//         InitParams params(tableMetaInfo);
//         // InitParams params = {tableMetaInfo.tableKey, tableMetaInfo.tableIndex, tableMetaInfo.tableName, tableMetaInfo.dimSize,
//         //                      tableMetaInfo.tableCapacity, tableMetaInfo.bucketNum, tableMetaInfo.hashFunction,
//         //                      tableMetaInfo.bucketCapacity};
//         embClient = std::make_shared<EmbClient>();
//         embClient->Init(params);
//         // 自动降为weak_ptr，加入map管理
//         EmbTableClientMap[tableKey] = embClient;
//     }
//     return Status::OK();
// }

Status EmbClientImpl::Insert(const std::vector<uint64_t> &keys, const std::vector<StringView> &values)
{
    // 1.参数校验
    // 2.创建tableBucket：
        // 参数1：bufferInfo要从tableMeta中拿，所以要调用getTableMeta)
        // 参数2：kvClientImpl_要先拿到client，这里在EmbClientImpl中写一个方法来实现从缓存拿或者从持久层拿meta构造client，（Client::GetClient的逻辑好像也需要修改）
    // 4.调用tableBucket->Insert

    // //不能是空的
    // auto weakTableMeta = EmbTableMetaMap[tableKey];
    // // 2. 尝试提升为 shared_ptr
    // std::shared_ptr<EmbTableMeta> tableMeta = weakTableMeta.lock();


    // std::shared_ptr<EmbClient> embClient;
    // RETURN_IF_NOT_OK_PRINT_ERROR_MSG(GetClient(tableKey, embClient), "GetClient by tableKey failed");

    // 创建tableBucket
    // auto tableBucket = std::make_shared<EmbTableBucket>(bucketInfo, embClient->kvClientImpl_, master_);
    // // 自动将强引用降为弱引用,交给map管理
    // EmbTableBucketMap[tableKey] = tableBucket;
    // // 将强引用交给client
    // embClient->tableBucket_ = tableBucket;
    Status rc = tableBucket_->Insert(keys, values);
    return rc;
}

Status EmbClientImpl::BuildIndex()
{
    // 这里是和ShareObjectMaster交互，master将这个信号下发给它管理的每个shareMap
    // auto ClientWorkerApi = std::make_shared<ClientWorkerApi>(workerAddress, RpcCredential());
    // Status rc = ClientWorkerApi->BuildIndex();

    // 要想调用master的Index(),需要先持有一个master对象，根据tableKey去ShareObjectMasters拿

    // 由于每个clientImpl对应一个master，所以应该由clientImpl持有master的强引用
    CHECK_FAIL_RETURN_STATUS(master_ != nullptr, StatusCode::K_INVALID, "master_ is nullptr");

    // 这里的逻辑是根据tableKey拿到client，再拿到client.tableBucket_
    // std::shared_ptr<EmbClient> embClient;
    // RETURN_IF_NOT_OK_PRINT_ERROR_MSG(GetClient(tableKey, embClient), "GetClient by tableKey failed");
    auto bucketKeys = tableBucket_->GetAllBucketKeys();
    Status rc = master_->Index(bucketKeys);
    return rc;
}

// keys和values的顺序要保持一致，values的顺序取决于copy的顺序
// 所以还得是我这边来做copy
// Status EmbClientImpl::Find(const std::vector<uint32_t> &keys, Optional<Buffer> &buffer, const std::string tableKey)
Status EmbClientImpl::Find(const std::vector<uint64_t> &keys, std::vector<StringView> &buffer)
{
    PerfPoint point(PerfKey::EMBCLIENT_IMPL_KEY_TO_BUCKET);
    // 映射关系 key -> bucketKey -> map -> value

    // 1.将keys按照所处的bucket分组，构造bucketKeytoKeys这个map，分组是为了一个bucket只需要查询一次map，而不是一个key查询一次
    // std::shared_ptr<EmbClient> embClient;
    // RETURN_IF_NOT_OK_PRINT_ERROR_MSG(GetClient(tableKey, embClient), "GetClient by tableKey failed");
    // std::unordered_map<std::string, std::vector<std::pair<uint64_t, int>>> bucketGroups = GroupByBucketKey(keys);


    
    // 根据key计算bucketHash得到bucketKey,并分组存入map
    const uint64_t bucketNum = tableBucket_->GetBucketNum();
    std::vector<std::vector<uint64_t>> bucketGroupsKey(bucketNum);
    std::vector<std::vector<uint64_t>> bucketGroupsOrder(bucketNum);

    // std::unordered_map<std::string, std::vector<std::pair<uint64_t, int>>> bucketGroups;
    for (size_t i = 0; i < keys.size(); ++i) {
        uint64_t bucketIdx = tableBucket_->GetBucketIndex(keys[i]);
        bucketGroupsKey[bucketIdx].emplace_back(keys[i]);
        bucketGroupsOrder[bucketIdx].emplace_back(i);
    }

    std::vector<std::future<Status>> futs;
    futs.reserve(bucketNum);
    std::vector<std::vector<StringView>> queryBuffers;
    queryBuffers.resize(bucketNum);
    point.RecordAndReset(PerfKey::EMBCLIENT_IMPL_FIND_ASYNC);

    for (size_t bucketIdx = 0; bucketIdx < bucketNum; bucketIdx++) {
        if (bucketGroupsKey[bucketIdx].empty()) {
            continue;
        }
        futs.emplace_back(queryThreadPool_->Submit([&, bucketIdx]() {
            return master_->QueryData(tableBucket_->BucketIndex2Key(bucketIdx), bucketGroupsKey[bucketIdx], queryBuffers[bucketIdx]);
        }));
    }

    for (auto& fut: futs) {
        RETURN_IF_NOT_OK(fut.get());
    }

    point.RecordAndReset(PerfKey::EMBCLIENT_IMPL_SORT_RESULT);
    const auto& nKeys = keys.size();
    buffer.resize(nKeys);

    for (size_t i = 0; i < bucketNum; i++) {
        const auto& bucketSize = bucketGroupsKey[i].size();
        for (size_t j = 0; j < bucketSize; j++) {
            auto order = bucketGroupsOrder[i][j];
            buffer[order] = queryBuffers[i][j];
        }
    }
    point.Record();
    
    return Status::OK();
    
}

Status EmbClientImpl::Load(const std::string &tableKey, const std::vector<std::string> embKeyFilesPath, const std::vector<std::string> embValueFilesPath, const std::string fileFormat)
{   
    // 1.files_path推送到MQ,调用master_->enqueue()
    // 2.轮询MQ拿files_path,
    // 3.去cluster download文件到本地
    // 4.解析文件，输入解析前的文件本地路径，输出解析后的k，v文件的路径
    // 5.insert存储数据
 
    // 这里其实和client没有关系，还是clientImpl和队列对接，clientImpl和队列感知到的只是tableKeys_,推送和拉取都是队列和clientImpl交互
    // 一个path对应一个文件（这是前提），从队列中拿一个filePath，也就是拿一个文件

    // todo
    // 1.这里改成pair<keyfile, valuefile>,下面这行改成master_.enqueue(tableKey, {embKeyFilesPath, embValueFilesPath});
    RETURN_IF_NOT_OK(master_->enqueue(embKeyFilesPath, embValueFilesPath));

    // 轮询
    while (true) {
        auto msg = master_->dequeue();
        if (msg == nullptr) {
            break;
        }
        
        std::string combinedPath = msg->value;
        size_t pos = combinedPath.find(':');
        auto getKeyFilesPath = combinedPath.substr(0, pos);
        auto getValueFilesPath = combinedPath.substr(pos + 1);

        auto downloader = DownloaderFactory::createDownloader(getKeyFilesPath);
        auto keyFutrue = downloader->download(getKeyFilesPath, master_->localPath_);
        auto valueFutrue = downloader->download(getValueFilesPath, master_->localPath_);

        std::string keyFileName = std::filesystem::path(getKeyFilesPath).filename().string();
        std::string valueFileName = std::filesystem::path(getValueFilesPath).filename().string();

        std::string localKeyFilesPath = (std::filesystem::path(master_->localPath_) / keyFileName).string();
        std::string localValueFilesPath = (std::filesystem::path(master_->localPath_) / valueFileName).string();
        
        // @todo：keyFuture.get()返回值转为Status
        // keyFutrue.get();
        // valueFutrue.get();
        // RETURN_IF_NOT_OK_PRINT_ERROR_MSG(keyFutrue.get(), "download key file failed");
        // RETURN_IF_NOT_OK_PRINT_ERROR_MSG(valueFutrue.get(), "download value file failed");

        // 解析,localKeyFilesPath\localValueFilesPath
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(Parse(tableKey, localKeyFilesPath, localValueFilesPath), "Parse failed");
    }
    return Status::OK();
}

Status EmbClientImpl::Parse(const std::string tableKey, std::string localKeyFilesPath, std::string localValueFilesPath)
{
    TableMetaInfo tableMetaInfo;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(tableMeta_->QueryTableMetaInfo(tableKey, tableMetaInfo),
                                     "QueryTableMetaInfo failed");
    uint64_t dimSize = tableMetaInfo.dimSize;

    auto keyFileSize = std::filesystem::file_size(localKeyFilesPath);
    const size_t keyItemSize = 8;
    size_t totalKeys = keyFileSize / keyItemSize;
    const size_t batchSize = 64000;
    size_t loadedCount = 0;

    std::ifstream fKey(localKeyFilesPath, std::ios::binary);
    std::ifstream fEmb(localValueFilesPath, std::ios::binary);

    if (!fKey.is_open() || !fEmb.is_open()) {
        throw std::runtime_error("Failed to open files");
    }

    while (loadedCount < totalKeys) {
        size_t currentBatch = std::min(batchSize, totalKeys - loadedCount);

        std::vector<uint64_t> keysNp(currentBatch);
        fKey.read(reinterpret_cast<char *>(keysNp.data()), currentBatch * sizeof(uint64_t));

        std::vector<float> embsNp(currentBatch * dimSize);

        std::vector<StringView> strValues;

        for (size_t i = 0; i < currentBatch; ++i) {
            std::string value;
            value.resize(dimSize * sizeof(float));
            fEmb.read(reinterpret_cast<char *>(value.data()), dimSize * sizeof(float));

            StringView stringView = value;
            strValues.push_back(stringView);
        }

        try {
            tableBucket_->Insert(keysNp, strValues);
            loadedCount += currentBatch;
        } catch (const std::exception &e) {
            throw std::runtime_error("Insert Failed");
        }
    }
    return Status::OK();
}
}  // namespace emb_cache
}  // namespace datasystem