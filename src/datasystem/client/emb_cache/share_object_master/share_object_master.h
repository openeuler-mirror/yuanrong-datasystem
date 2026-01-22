#ifndef SHARE_OBJECT_MASTER_H
 
#include "share_object_api.h"
#include "downloader_factory.h"
#include "etcd_queue.h"
#include "datasystem/kv_client.h"
 
namespace datasystem {
namespace master{
class ShareObjectMaster {
public:
    std::string localPath_;
 
    explicit ShareObjectMaster(const std::string &etcdServer, 
                               std::shared_ptr<KVClient> kvClient,
                               const std::string &localPath,
                               const std::string &tableKey)
    : kvClient_(kvClient),
      localPath_(localPath)
    {
        etcdQueue = std::make_unique<EtcdMPMCQueue>(etcdServer, "/queue_" + tableKey);
    }
 
    ~ShareObjectMaster() noexcept = default;
 
    datasystem::Status Init(const std::vector<std::string>& bucketkeys,
                                uint64_t dimSize);
 
    datasystem::Status Publish(const std::string& bucketkey,
                               std::vector<uint64_t>& keys,
                               std::vector<StringView>& datas,
                               uint64_t dimSize);
 
    datasystem::Status QueryData(const std::string& bucketkey,
                                 const std::vector<uint64_t>& keys,
                                 std::vector<StringView>& datas);
 
    datasystem::Status Index(const std::vector<std::string>& bucketkeys);
 
    datasystem::Status enqueue(const std::vector<std::string> &embKeyFilesPath,
                               const std::vector<std::string> &embValueFilesPath);
 
    std::unique_ptr<EtcdMPMCQueue::Message> dequeue();
 
private:
    std::unordered_map<std::string, std::unique_ptr<ShareObjectApi>> shareObjectApiMap;
    std::unique_ptr<EtcdMPMCQueue> etcdQueue;
    std::shared_ptr<KVClient> kvClient_;
};
}
}
#endif  // SHARE_OBJECT_MASTER_H