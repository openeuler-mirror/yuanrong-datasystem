#include <cstdlib>

#include "oc_client_common.h"
#include "common.h"

namespace datasystem {
namespace st {

class OCClientHappyPathTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 1;
        opts.numEtcd = 1;
        opts.numOcThreadNum = getThreadNum_;
        opts.workerGflagParams = " -shared_memory_size_mb=500 -v=2";
    }

    void SetUp() override
    {
        OCClientCommon::SetUp();
        unsetenv("DATASYSTEM_UB_GET_DATA_SIZE_BYTES");
    }

    void TearDown() override
    {
        unsetenv("DATASYSTEM_UB_GET_DATA_SIZE_BYTES");
        OCClientCommon::TearDown();
    }

protected:
    const int getThreadNum_ = 4;
};

// Verify Get works correctly with env var set to exact data size.
TEST_F(OCClientHappyPathTest, GetWithEnvVarExactSize)
{
    std::shared_ptr<ObjectClient> cli;
    InitTestClient(0, cli);

    std::string objKey = NewObjectKey();
    std::string data = GenRandomString(1024);
    CreateAndSealObject(cli, objKey, data);

    setenv("DATASYSTEM_UB_GET_DATA_SIZE_BYTES", "1024", 1);

    std::vector<Optional<Buffer>> dataList;
    DS_ASSERT_OK(cli->Get({ objKey }, 0, dataList));
    ASSERT_TRUE(NotExistsNone(dataList));
    AssertBufferEqual(*dataList[0], data);
}

// Verify Get works correctly with env var set larger than actual data.
TEST_F(OCClientHappyPathTest, GetWithEnvVarLargerSize)
{
    std::shared_ptr<ObjectClient> cli;
    InitTestClient(0, cli);

    std::string objKey = NewObjectKey();
    std::string data = GenRandomString(1024);
    CreateAndSealObject(cli, objKey, data);

    // Configure 2x larger than actual to verify buffer oversizing doesn't break Get.
    setenv("DATASYSTEM_UB_GET_DATA_SIZE_BYTES", "2048", 1);

    std::vector<Optional<Buffer>> dataList;
    DS_ASSERT_OK(cli->Get({ objKey }, 0, dataList));
    ASSERT_TRUE(NotExistsNone(dataList));
    AssertBufferEqual(*dataList[0], data);
}

// Verify Get works correctly with env var set smaller than actual data (TCP fallback).
TEST_F(OCClientHappyPathTest, GetWithEnvVarSmallerSize)
{
    std::shared_ptr<ObjectClient> cli;
    InitTestClient(0, cli);

    std::string objKey = NewObjectKey();
    std::string data = GenRandomString(1024);
    CreateAndSealObject(cli, objKey, data);

    // Configure smaller than actual to trigger TCP fallback.
    setenv("DATASYSTEM_UB_GET_DATA_SIZE_BYTES", "512", 1);

    std::vector<Optional<Buffer>> dataList;
    DS_ASSERT_OK(cli->Get({ objKey }, 0, dataList));
    ASSERT_TRUE(NotExistsNone(dataList));
    AssertBufferEqual(*dataList[0], data);
}

// Verify Get works correctly without env var (no regression).
TEST_F(OCClientHappyPathTest, GetWithoutEnvVar)
{
    std::shared_ptr<ObjectClient> cli;
    InitTestClient(0, cli);

    std::string objKey = NewObjectKey();
    std::string data = GenRandomString(1024);
    CreateAndSealObject(cli, objKey, data);

    // Env var not set — must fall through to normal GetObjMetaInfo path.
    std::vector<Optional<Buffer>> dataList;
    DS_ASSERT_OK(cli->Get({ objKey }, 0, dataList));
    ASSERT_TRUE(NotExistsNone(dataList));
    AssertBufferEqual(*dataList[0], data);
}

// Verify Get works with empty env var (treated as unset).
TEST_F(OCClientHappyPathTest, GetWithEmptyEnvVar)
{
    std::shared_ptr<ObjectClient> cli;
    InitTestClient(0, cli);

    std::string objKey = NewObjectKey();
    std::string data = GenRandomString(1024);
    CreateAndSealObject(cli, objKey, data);

    setenv("DATASYSTEM_UB_GET_DATA_SIZE_BYTES", "", 1);

    std::vector<Optional<Buffer>> dataList;
    DS_ASSERT_OK(cli->Get({ objKey }, 0, dataList));
    ASSERT_TRUE(NotExistsNone(dataList));
    AssertBufferEqual(*dataList[0], data);
}

// Verify Get works with non-numeric env var (treated as unset).
TEST_F(OCClientHappyPathTest, GetWithInvalidEnvVar)
{
    std::shared_ptr<ObjectClient> cli;
    InitTestClient(0, cli);

    std::string objKey = NewObjectKey();
    std::string data = GenRandomString(1024);
    CreateAndSealObject(cli, objKey, data);

    setenv("DATASYSTEM_UB_GET_DATA_SIZE_BYTES", "not_a_number", 1);

    std::vector<Optional<Buffer>> dataList;
    DS_ASSERT_OK(cli->Get({ objKey }, 0, dataList));
    ASSERT_TRUE(NotExistsNone(dataList));
    AssertBufferEqual(*dataList[0], data);
}

}  // namespace st
}  // namespace datasystem
