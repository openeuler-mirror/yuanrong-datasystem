#ifndef SHARE_OBJECT_API_H
 
#include <vector>
#include <memory>
 
#include "datasystem/utils/string_view.h"
#include "datasystem/object/share_map_object.h"
#include "datasystem/common/util/status_helper.h"
 
namespace datasystem {
namespace master{
 
// ShareObjectApi基类
class ShareObjectApi {
public:
    virtual ~ShareObjectApi() = default;
 
    virtual datasystem::Status Publish(const std::vector<uint64_t>& keys,
                                       const std::vector<StringView>& datas) = 0;
 
    virtual datasystem::Status QueryData(const std::vector<uint64_t>& keys,
                                         std::vector<StringView>& datas) = 0;
 
    virtual datasystem::Status Index() = 0;
};
 
// ShareObjectLocalApi类
class ShareObjectLocalApi : public ShareObjectApi {
public:
    ShareObjectLocalApi(std::shared_ptr<ShareMapObject> shareMapObject) : shareMapObject_(shareMapObject) 
    {
    }
 
    datasystem::Status Publish(const std::vector<uint64_t>& keys,
                               const std::vector<StringView>& datas) override {
        return shareMapObject_->Insert(keys, datas);
    }
 
    datasystem::Status QueryData(const std::vector<uint64_t>& keys,
                                std::vector<StringView>& datas) override {
        RETURN_IF_NOT_OK(shareMapObject_->Lookup(keys, datas));
        return Status::OK();
    }
 
    datasystem::Status Index() override {
        return shareMapObject_->BuildIndex();
    }
 
private:
    std::shared_ptr<ShareMapObject> shareMapObject_;
};
 
// TODO 增加RPC功能 ShareObjectRemoteApi类
class ShareObjectRemoteApi : public ShareObjectApi {
};
 
// API管理类
class ShareObjectApiManager {
public:
    static std::unique_ptr<ShareObjectApi> CreateApi(const std::string& bucketkey, std::shared_ptr<KVClient> kvClient, uint64_t dimSize) {
        return std::make_unique<ShareObjectLocalApi>(
            std::make_shared<ShareMapObject>(bucketkey, kvClient, dimSize));
    }
};
}
}
#endif  // SHARE_OBJECT_API_H