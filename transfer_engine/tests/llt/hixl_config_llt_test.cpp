/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 */

#include <gtest/gtest.h>

#include <string>

#include "internal/backend/ascend/hixl_config.h"

namespace datasystem {
namespace {

TEST(HixlConfigLltTest, DefaultRouteIsRoce)
{
    EXPECT_STREQ(K_DEFAULT_HIXL_ROUTE, "roce");
}

struct HixlInputOverrides {
    std::string globalResourceConfig;
    bool legacyRoceEnabled = false;
    std::string localCommRes;
};

HixlCsConfigInput MakeInput(const std::string &mode, const std::string &route, bool capabilityAvailable,
                            const HixlInputOverrides &overrides = HixlInputOverrides())
{
    HixlCsConfigInput input;
    input.requestedMode = mode;
    input.routePolicy = route;
    input.capabilityAvailable = capabilityAvailable;
    input.localCommRes = overrides.localCommRes;
    input.globalResourceConfig = overrides.globalResourceConfig;
    input.legacyRoceEnabled = overrides.legacyRoceEnabled;
    return input;
}

TEST(HixlConfigLltTest, DefaultClientServerModeIsOnAndFailsClosed)
{
    EXPECT_STREQ(K_DEFAULT_HIXL_CS_MODE, "on");

    HixlCsConfig config;
    EXPECT_EQ(ResolveHixlCsConfig(MakeInput("", "roce", false), &config).GetCode(), ErrorCode::kNotSupported);
    ASSERT_TRUE(ResolveHixlCsConfig(MakeInput("", "roce", true), &config).IsOk());
    EXPECT_EQ(config.engineMode, HixlEngineMode::kClientServer);
}

TEST(HixlConfigLltTest, AutoFallsBackToLegacyWithoutCapability)
{
    HixlCsConfig config;
    ASSERT_TRUE(ResolveHixlCsConfig(MakeInput("auto", "auto", false), &config).IsOk());
    EXPECT_EQ(config.engineMode, HixlEngineMode::kLegacy);
    EXPECT_TRUE(config.localCommRes.empty());
    EXPECT_TRUE(config.globalResourceConfig.empty());
}

TEST(HixlConfigLltTest, AutoEnablesClientServerWithCapability)
{
    HixlCsConfig config;
    ASSERT_TRUE(ResolveHixlCsConfig(MakeInput("auto", "auto", true), &config).IsOk());
    EXPECT_EQ(config.engineMode, HixlEngineMode::kClientServer);
    EXPECT_EQ(config.localCommRes, R"({"version":"1.3"})");
    EXPECT_TRUE(config.globalResourceConfig.empty());
}

TEST(HixlConfigLltTest, ClientServerModeIsCaseInsensitive)
{
    HixlCsConfig config;
    ASSERT_TRUE(ResolveHixlCsConfig(MakeInput("ON", "auto", true), &config).IsOk());
    EXPECT_EQ(config.engineMode, HixlEngineMode::kClientServer);
}

TEST(HixlConfigLltTest, InvalidClientServerModeIsRejected)
{
    HixlCsConfig config;
    EXPECT_EQ(ResolveHixlCsConfig(MakeInput("enabled", "auto", true), &config).GetCode(), ErrorCode::kInvalid);
}

TEST(HixlConfigLltTest, InvalidRouteIsRejected)
{
    HixlCsConfig config;
    EXPECT_EQ(ResolveHixlCsConfig(MakeInput("on", "ub", true), &config).GetCode(), ErrorCode::kInvalid);
}

TEST(HixlConfigLltTest, ExplicitClientServerRequiresCapability)
{
    HixlCsConfig config;
    Result result = ResolveHixlCsConfig(MakeInput("on", "roce", false), &config);
    EXPECT_EQ(result.GetCode(), ErrorCode::kNotSupported);
}

TEST(HixlConfigLltTest, ClientServerRoceInjectsProtocolFilter)
{
    HixlCsConfig config;
    ASSERT_TRUE(ResolveHixlCsConfig(MakeInput("on", "roce", true), &config).IsOk());
    EXPECT_EQ(config.engineMode, HixlEngineMode::kClientServer);
    EXPECT_EQ(config.globalResourceConfig, R"({"comm_resource_config.protocol_desc":"roce:device"})");
}

// 中文说明：CS 模式下未显式设置 YR_TE_HIXL_LOCAL_COMM_RES 时，
// 必然注入默认 {"version":"1.3"} 对象，而不是留空。
TEST(HixlConfigLltTest, UnsetLocalCommResInjectsDefaultInClientServerMode)
{
    HixlCsConfig config;
    ASSERT_TRUE(ResolveHixlCsConfig(MakeInput("on", "auto", true), &config).IsOk());
    EXPECT_EQ(config.engineMode, HixlEngineMode::kClientServer);
    EXPECT_EQ(config.localCommRes, R"({"version":"1.3"})");
}

TEST(HixlConfigLltTest, ExplicitLocalCommResIsNormalizedAndPreserved)
{
    HixlCsConfig config;
    const std::string localCommRes =
        R"({"version":"1.3","net_instance_id":"pod-a","endpoint_list":[)"
        R"({"protocol":"roce","comm_id":"192.0.2.10","placement":"device"}]})";
    ASSERT_TRUE(ResolveHixlCsConfig(MakeInput("on", "roce", true, { "", false, localCommRes }), &config).IsOk());
    EXPECT_NE(config.localCommRes.find(R"("net_instance_id":"pod-a")"), std::string::npos);
    EXPECT_NE(config.localCommRes.find(R"("protocol":"roce")"), std::string::npos);
}

TEST(HixlConfigLltTest, ExplicitLocalCommResRequiresVersionOnePointThree)
{
    HixlCsConfig config;
    EXPECT_EQ(ResolveHixlCsConfig(MakeInput("on", "roce", true, { "", false, R"({"version":"1.2"})" }),
                                 &config)
                  .GetCode(),
              ErrorCode::kInvalid);
    EXPECT_EQ(ResolveHixlCsConfig(MakeInput("on", "roce", true, { "", false, "not-json" }), &config).GetCode(),
              ErrorCode::kInvalid);
}

TEST(HixlConfigLltTest, ExplicitLocalCommResRequiresClientServerMode)
{
    HixlCsConfig config;
    EXPECT_EQ(ResolveHixlCsConfig(MakeInput("off", "auto", true, { "", false, R"({"version":"1.3"})" }), &config)
                  .GetCode(),
              ErrorCode::kInvalid);
}

TEST(HixlConfigLltTest, AutoConnectUsesCapabilityAndSupportsExplicitRollback)
{
    HixlAutoConnectConfig config;
    ASSERT_TRUE(ResolveHixlAutoConnectConfig("auto", true, &config).IsOk());
    EXPECT_TRUE(config.enabled);
    EXPECT_EQ(config.optionValue, "1");

    ASSERT_TRUE(ResolveHixlAutoConnectConfig("off", true, &config).IsOk());
    EXPECT_FALSE(config.enabled);
    EXPECT_EQ(config.optionValue, "0");

    ASSERT_TRUE(ResolveHixlAutoConnectConfig("auto", false, &config).IsOk());
    EXPECT_FALSE(config.enabled);
}

TEST(HixlConfigLltTest, AutoConnectOnFailsClosedWithoutCapability)
{
    HixlAutoConnectConfig config;
    EXPECT_EQ(ResolveHixlAutoConnectConfig("on", false, &config).GetCode(), ErrorCode::kNotSupported);
    EXPECT_EQ(ResolveHixlAutoConnectConfig("invalid", true, &config).GetCode(), ErrorCode::kInvalid);
}

TEST(HixlConfigLltTest, ClientServerHccsPreservesCompatibleResourceConfig)
{
    const std::string raw =
        R"({"comm_resource_config.listen_port":26666,"comm_resource_config.protocol_desc":["hccs:device"]})";
    HixlCsConfig config;
    ASSERT_TRUE(ResolveHixlCsConfig(MakeInput("on", "hccs", true, { raw, false, "" }), &config).IsOk());
    EXPECT_NE(config.globalResourceConfig.find(R"("comm_resource_config.listen_port":26666)"), std::string::npos);
    EXPECT_NE(config.globalResourceConfig.find(R"("comm_resource_config.protocol_desc":"hccs:device")"),
              std::string::npos);
}

TEST(HixlConfigLltTest, ExplicitRouteRejectsConflictingProtocolFilter)
{
    HixlCsConfig config;
    Result result = ResolveHixlCsConfig(
        MakeInput("on", "roce", true, { R"({"comm_resource_config.protocol_desc":"hccs:device"})", false, "" }),
        &config);
    EXPECT_EQ(result.GetCode(), ErrorCode::kInvalid);
}

TEST(HixlConfigLltTest, LegacyRejectsProtocolFilterThatWouldSelectClientServer)
{
    HixlCsConfig config;
    Result result = ResolveHixlCsConfig(
        MakeInput("off", "auto", true, { R"({"comm_resource_config.protocol_desc":"roce:device"})", false, "" }),
        &config);
    EXPECT_EQ(result.GetCode(), ErrorCode::kInvalid);
}

TEST(HixlConfigLltTest, LegacyRoceRequiresTheLegacyHcclSwitch)
{
    HixlCsConfig config;
    EXPECT_EQ(ResolveHixlCsConfig(MakeInput("auto", "roce", false), &config).GetCode(), ErrorCode::kNotSupported);
    ASSERT_TRUE(ResolveHixlCsConfig(MakeInput("auto", "roce", false, { "", true, "" }), &config).IsOk());
    EXPECT_EQ(config.engineMode, HixlEngineMode::kLegacy);
}

TEST(HixlConfigLltTest, InvalidResourceConfigIsRejected)
{
    HixlCsConfig config;
    Result result = ResolveHixlCsConfig(MakeInput("on", "roce", true, { "not-json", false, "" }), &config);
    EXPECT_EQ(result.GetCode(), ErrorCode::kInvalid);
}

TEST(HixlConfigLltTest, EmptyProtocolDescriptorIsRejectedBeforeHixlInitialization)
{
    HixlCsConfig config;
    Result result =
        ResolveHixlCsConfig(
            MakeInput("off", "auto", true, { R"({"comm_resource_config.protocol_desc":""})", false, "" }), &config);
    EXPECT_EQ(result.GetCode(), ErrorCode::kInvalid);
}

TEST(HixlConfigLltTest, LegacyRootInfoRemainsBackwardCompatible)
{
    const std::string encoded =
        "transfer_engine_hixl_root_info_v1\n"
        "backend=ascend\n"
        "endpoint=127.0.0.1:21000\n"
        "route=auto\n";
    HixlPeerInfo peerInfo;
    ASSERT_TRUE(ParseHixlPeerInfo(encoded, &peerInfo).IsOk());
    EXPECT_EQ(peerInfo.engineMode, HixlEngineMode::kLegacy);
    EXPECT_EQ(EncodeHixlPeerInfo(peerInfo), encoded);
}

TEST(HixlConfigLltTest, ClientServerRootInfoCarriesMode)
{
    HixlPeerInfo peerInfo;
    peerInfo.backendKind = "ascend";
    peerInfo.endpoint = "127.0.0.1:21000";
    peerInfo.routePolicy = "roce";
    peerInfo.engineMode = HixlEngineMode::kClientServer;
    const std::string encoded = EncodeHixlPeerInfo(peerInfo);
    EXPECT_NE(encoded.find("transfer_engine_hixl_root_info_v2"), std::string::npos);
    EXPECT_NE(encoded.find("mode=cs"), std::string::npos);

    HixlPeerInfo parsed;
    ASSERT_TRUE(ParseHixlPeerInfo(encoded, &parsed).IsOk());
    EXPECT_EQ(parsed.engineMode, HixlEngineMode::kClientServer);
    EXPECT_EQ(parsed.routePolicy, "roce");
}

TEST(HixlConfigLltTest, ClientServerRootInfoRequiresMode)
{
    const std::string encoded =
        "transfer_engine_hixl_root_info_v2\n"
        "backend=ascend\n"
        "endpoint=127.0.0.1:21000\n"
        "route=roce\n";
    HixlPeerInfo peerInfo;
    EXPECT_EQ(ParseHixlPeerInfo(encoded, &peerInfo).GetCode(), ErrorCode::kInvalid);
}

}  // namespace
}  // namespace datasystem
