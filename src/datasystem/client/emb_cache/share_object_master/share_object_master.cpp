#include "share_object_master.h"
 
namespace datasystem {
namespace master {
 
datasystem::Status ShareObjectMaster::Init(const std::vector<std::string>& bucketkeys,
                            uint64_t dimSize) {
      for (auto& bucketkey : bucketkeys) {
        shareObjectApiMap.emplace(bucketkey,
                    ShareObjectApiManager::CreateApi(bucketkey, kvClient_, dimSize));
      }
      return Status::OK();
    }
 
datasystem::Status ShareObjectMaster::Publish(const std::string& bucketkey,
                                              std::vector<uint64_t>& keys,
                                              std::vector<StringView>& datas,
                                              uint64_t dimSize) {
    auto iter = shareObjectApiMap.find(bucketkey);
    if (iter == shareObjectApiMap.end()) {
        iter = shareObjectApiMap.emplace(bucketkey,
                                       ShareObjectApiManager::CreateApi(bucketkey, kvClient_, dimSize)).first;
    }
    return iter->second->Publish(keys, datas);
}
 
datasystem::Status ShareObjectMaster::QueryData(const std::string& bucketkey,
                             const std::vector<uint64_t>& keys,
                             std::vector<StringView>& datas) {
    
    return shareObjectApiMap[bucketkey]->QueryData(keys, datas);
}
 
datasystem::Status ShareObjectMaster::Index(const std::vector<std::string>& bucketkeys) {
    for(auto& bucketkey : bucketkeys) {
        RETURN_IF_NOT_OK(shareObjectApiMap[bucketkey]->Index());
    }
    return datasystem::Status::OK();
}
 
datasystem::Status ShareObjectMaster::enqueue(const std::vector<std::string> &embKeyFilesPath,
                                              const std::vector<std::string> &embValueFilesPath) {
    for (int i = 0; i < embKeyFilesPath.size(); i++) {
        std::string msgValue = embKeyFilesPath[i] + ":" + embValueFilesPath[i];
        RETURN_IF_NOT_OK(etcdQueue->push(std::to_string(i), msgValue));
    }
    // 发布eof消息
    RETURN_IF_NOT_OK(etcdQueue->pushEOF(std::to_string(embKeyFilesPath.size())));
    return datasystem::Status::OK();
}

std::unique_ptr<Message> ShareObjectMaster::dequeue() {
    return etcdQueue->pop();
}
}
}