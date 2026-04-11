/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: Test interface to HashRingHealthCheck
 */
#include "ut/common.h"
#include <google/protobuf/util/json_util.h>
#include <nlohmann/json.hpp>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/protos/worker_object.pb.h"
#include "datasystem/worker/hash_ring/hash_ring_health_check.h"
#include "datasystem/worker/hash_ring/hash_ring.h"

using namespace datasystem::worker;
namespace datasystem {
namespace ut {
class HashRingHealthCheckTest : public CommonTest, public HashRingHealthCheck {
public:
    HashRingHealthCheckTest() : HashRingHealthCheck(nullptr)
    {
    }
};

TEST_F(HashRingHealthCheckTest, TestNormalHashRing)
{
    std::string hashRingJsonStr = R"(
        {
            "clusterId": "127.0.0.1:9254",
            "workers": {
                "127.0.0.1:4618": {
                    "hashTokens": [715827882, 1789569705, 2863311528, 3937053351],
                    "workerUuid": "MTVlNDNhN2ItNWEzMC00NTk2LTg3MjYtZGY3Y2ZlYWFiODhj",
                    "state": "ACTIVE"
                },
                "127.0.0.1:6003": {
                    "hashTokens": [1073741823, 2147483646, 3221225469, 4294967292],
                    "workerUuid": "ZDc3YmE0NGItZjMxNi00OTJmLTllMTEtZGIwZTI0ZDJlNzJm",
                    "state": "ACTIVE"
                },
                "127.0.0.1:13102": {
                    "hashTokens": [357913941, 1431655764, 2505397587, 3579139410],
                    "workerUuid": "ZWQxZjA3ODctZWQ1NS00OTcwLWEwNzQtNjIyZDYwZjgxN2Iy",
                    "state": "ACTIVE"
                }
            },
            "clusterHasInit": true
        }
    )";
    HashRingPb hashRing;
    auto rc = google::protobuf::util::JsonStringToMessage(hashRingJsonStr, &hashRing);
    ASSERT_TRUE(rc.ok());

    ASSERT_TRUE(!CheckScaleUpInfo(false, hashRing));
    ASSERT_TRUE(!CheckVoluntaryScaleDownInfo(false, hashRing));
    ASSERT_TRUE(!CheckPassiveScaleDownInfo(false, hashRing));
    ASSERT_TRUE(!CheckWorkerInJoiningState(false, hashRing));
    ASSERT_TRUE(!CheckWorkerInLeavingState(false, hashRing));
    ASSERT_TRUE(!CheckWorkerInInitialState(false, hashRing));
    ASSERT_TRUE(!CheckPassiveScaleDownNotStart(false, hashRing));
}

TEST_F(HashRingHealthCheckTest, TestScaleUpHashRing)
{
    std::string hashRingJsonStr = R"(
        {
            "clusterId": "127.0.0.1:32044",
            "workers": {
                "127.0.0.1:47805": {
                    "hashTokens": [715827882, 1789569705, 2863311528, 3937053351],
                    "workerUuid": "OTJlYjE0NzctZDA3Yi00Y2NjLTk5MDEtZTVmYjg1ZjEzZjlh",
                    "state": "ACTIVE"
                },
                "127.0.0.1:53257": {
                    "hashTokens": [1073741823, 2147483646, 3221225469, 4294967292],
                    "workerUuid": "YWRjNzQyNGEtNGM1MC00ZmVhLTk5YTYtYzFkNDlkZDAxMmJi",
                    "state": "JOINING"
                },
                "127.0.0.1:14964": {
                    "hashTokens": [357913941, 1431655764, 2505397587, 3579139410],
                    "workerUuid": "ZTBmYWRlMDItMDIyOS00YzU3LWIwMmMtYzdhMjRhN2MwZDVm",
                    "state": "ACTIVE"
                }
            },
            "clusterHasInit": true,
            "addNodeInfo": {
                "127.0.0.1:53257": {
                    "changedRanges": [
                        {
                            "workerId": "127.0.0.1:14964",
                            "from": 715827882,
                            "end": 1073741823
                        },
                        {
                            "workerId": "127.0.0.1:14964",
                            "from": 1789569705,
                            "end": 2147483646
                        },
                        {
                            "workerId": "127.0.0.1:14964",
                            "from": 2863311528,
                            "end": 3221225469
                        },
                        {
                            "workerId": "127.0.0.1:14964",
                            "from": 3937053351,
                            "end": 4294967292
                        }
                    ]
                }
            }
        }
    )";
    HashRingPb hashRing;
    auto rc = google::protobuf::util::JsonStringToMessage(hashRingJsonStr, &hashRing);
    ASSERT_TRUE(rc.ok());

    ASSERT_TRUE(CheckScaleUpInfo(false, hashRing));
    ASSERT_TRUE(!CheckVoluntaryScaleDownInfo(false, hashRing));
    ASSERT_TRUE(!CheckPassiveScaleDownInfo(false, hashRing));
    ASSERT_TRUE(!CheckWorkerInJoiningState(false, hashRing));
    ASSERT_TRUE(!CheckWorkerInLeavingState(false, hashRing));
    ASSERT_TRUE(!CheckWorkerInInitialState(false, hashRing));
    ASSERT_TRUE(!CheckPassiveScaleDownNotStart(false, hashRing));

    ASSERT_TRUE(CheckScaleUpInfo(true, hashRing));

    ASSERT_EQ(hashRing.add_node_info_size(), 0);
    const int expectedWorkerSize = 2;
    ASSERT_EQ(hashRing.workers_size(), expectedWorkerSize);
    for (const auto &worker : hashRing.workers()) {
        ASSERT_EQ(worker.second.state(), WorkerPb::ACTIVE);
    }
}

TEST_F(HashRingHealthCheckTest, TestScaleDownHashRing)
{
    std::string hashRingJsonStr = R"(
        {
            "clusterId": "127.0.0.1:32044",
            "workers": {
                "127.0.0.1:47805": {
                    "hashTokens": [715827882, 1789569705, 2863311528, 3937053351],
                    "workerUuid": "OTJlYjE0NzctZDA3Yi00Y2NjLTk5MDEtZTVmYjg1ZjEzZjlh",
                    "state": "ACTIVE"
                },
                "127.0.0.1:53257": {
                    "hashTokens": [1073741823, 2147483646, 3221225469, 4294967292],
                    "workerUuid": "YWRjNzQyNGEtNGM1MC00ZmVhLTk5YTYtYzFkNDlkZDAxMmJi",
                    "state": "ACTIVE"
                },
                "127.0.0.1:14964": {
                    "hashTokens": [357913941, 1431655764, 2505397587, 3579139410],
                    "workerUuid": "ZTBmYWRlMDItMDIyOS00YzU3LWIwMmMtYzdhMjRhN2MwZDVm",
                    "state": "LEAVING",
                    "needScaleDown": true
                }
            },
            "clusterHasInit": true,
            "addNodeInfo": {
                "127.0.0.1:53257": {
                    "changedRanges": [
                        {
                            "workerId": "127.0.0.1:14964",
                            "from": 715827882,
                            "end": 1073741823
                        },
                        {
                            "workerId": "127.0.0.1:14964",
                            "from": 1789569705,
                            "end": 2147483646
                        },
                        {
                            "workerId": "127.0.0.1:14964",
                            "from": 2863311528,
                            "end": 3221225469
                        },
                        {
                            "workerId": "127.0.0.1:14964",
                            "from": 3937053351,
                            "end": 4294967292
                        }
                    ]
                }
            }
        }
    )";
    HashRingPb hashRing;
    auto rc = google::protobuf::util::JsonStringToMessage(hashRingJsonStr, &hashRing);
    ASSERT_TRUE(rc.ok());

    ASSERT_TRUE(!CheckScaleUpInfo(false, hashRing));
    ASSERT_TRUE(CheckVoluntaryScaleDownInfo(false, hashRing));
    ASSERT_TRUE(!CheckPassiveScaleDownInfo(false, hashRing));
    ASSERT_TRUE(!CheckWorkerInJoiningState(false, hashRing));
    ASSERT_TRUE(!CheckWorkerInLeavingState(false, hashRing));
    ASSERT_TRUE(!CheckWorkerInInitialState(false, hashRing));
    ASSERT_TRUE(!CheckPassiveScaleDownNotStart(false, hashRing));

    ASSERT_TRUE(CheckVoluntaryScaleDownInfo(true, hashRing));

    ASSERT_EQ(hashRing.add_node_info_size(), 0);
    const int expectedWorkerSize = 2;
    ASSERT_EQ(hashRing.workers_size(), expectedWorkerSize);
    for (const auto &worker : hashRing.workers()) {
        ASSERT_EQ(worker.second.state(), WorkerPb::ACTIVE);
    }
}

TEST_F(HashRingHealthCheckTest, TestPassiveScaleDownHashRing)
{
    std::string hashRingJsonStr = R"(
        {
            "clusterId": "127.0.0.1:32044",
            "workers": {
                "127.0.0.1:47805": {
                    "hashTokens": [715827882, 1789569705, 2863311528, 3937053351],
                    "workerUuid": "OTJlYjE0NzctZDA3Yi00Y2NjLTk5MDEtZTVmYjg1ZjEzZjlh",
                    "state": "ACTIVE"
                },
                "127.0.0.1:53257": {
                    "hashTokens": [1073741823, 2147483646, 3221225469, 4294967292],
                    "workerUuid": "YWRjNzQyNGEtNGM1MC00ZmVhLTk5YTYtYzFkNDlkZDAxMmJi",
                    "state": "ACTIVE"
                },
                "127.0.0.1:14964": {
                    "hashTokens": [357913941, 1431655764, 2505397587, 3579139410],
                    "workerUuid": "ZTBmYWRlMDItMDIyOS00YzU3LWIwMmMtYzdhMjRhN2MwZDVm",
                    "state": "ACTIVE"
                }
            },
            "clusterHasInit": true,
            "delNodeInfo": {
                "127.0.0.1:14964": {
                    "changedRanges": [
                        {
                            "workerId": "127.0.0.1:47805",
                            "from": 4294967292,
                            "end": 357913941
                        },
                        {
                            "workerId": "127.0.0.1:47805",
                            "from": 1073741823,
                            "end": 1431655764
                        },
                        {
                            "workerId": "127.0.0.1:47805",
                            "from": 2147483646,
                            "end": 2505397587
                        },
                        {
                            "workerId": "127.0.0.1:47805",
                            "from": 3221225469,
                            "end": 3579139410
                        }
                    ]
                }
            }
        }
    )";
    HashRingPb hashRing;
    auto rc = google::protobuf::util::JsonStringToMessage(hashRingJsonStr, &hashRing);
    ASSERT_TRUE(rc.ok());

    ASSERT_TRUE(!CheckScaleUpInfo(false, hashRing));
    ASSERT_TRUE(!CheckVoluntaryScaleDownInfo(false, hashRing));
    ASSERT_TRUE(CheckPassiveScaleDownInfo(false, hashRing));
    ASSERT_TRUE(!CheckWorkerInJoiningState(false, hashRing));
    ASSERT_TRUE(!CheckWorkerInLeavingState(false, hashRing));
    ASSERT_TRUE(!CheckWorkerInInitialState(false, hashRing));
    ASSERT_TRUE(!CheckPassiveScaleDownNotStart(false, hashRing));

    ASSERT_TRUE(CheckPassiveScaleDownInfo(true, hashRing));

    ASSERT_EQ(hashRing.del_node_info_size(), 0);
    const int expectedWorkerSize = 2;
    ASSERT_EQ(hashRing.workers_size(), expectedWorkerSize);
    for (const auto &worker : hashRing.workers()) {
        ASSERT_EQ(worker.second.state(), WorkerPb::ACTIVE);
    }
}

TEST_F(HashRingHealthCheckTest, TestPassiveScaleDownExistsScaleUpHashRing)
{
    std::string hashRingJsonStr = R"(
        {
            "clusterId": "127.0.0.1:32044",
            "workers": {
                "127.0.0.1:47805": {
                    "hashTokens": [715827882, 1789569705, 2863311528, 3937053351],
                    "workerUuid": "OTJlYjE0NzctZDA3Yi00Y2NjLTk5MDEtZTVmYjg1ZjEzZjlh",
                    "state": "ACTIVE"
                },
                "127.0.0.1:53257": {
                    "hashTokens": [1073741823, 2147483646, 3221225469, 4294967292],
                    "workerUuid": "YWRjNzQyNGEtNGM1MC00ZmVhLTk5YTYtYzFkNDlkZDAxMmJi",
                    "state": "JOINING"
                },
                "127.0.0.1:14964": {
                    "hashTokens": [357913941, 1431655764, 2505397587, 3579139410],
                    "workerUuid": "ZTBmYWRlMDItMDIyOS00YzU3LWIwMmMtYzdhMjRhN2MwZDVm",
                    "state": "ACTIVE"
                }
            },
            "clusterHasInit": true,
            "delNodeInfo": {
                "127.0.0.1:14964": {
                    "changedRanges": [
                        {
                            "workerId": "127.0.0.1:47805",
                            "from": 4294967292,
                            "end": 357913941
                        },
                        {
                            "workerId": "127.0.0.1:47805",
                            "from": 1073741823,
                            "end": 1431655764
                        },
                        {
                            "workerId": "127.0.0.1:47805",
                            "from": 2147483646,
                            "end": 2505397587
                        },
                        {
                            "workerId": "127.0.0.1:47805",
                            "from": 3221225469,
                            "end": 3579139410
                        }
                    ]
                }
            },
            "addNodeInfo": {
                "127.0.0.1:53257": {
                    "changedRanges": [
                        {
                            "workerId": "127.0.0.1:14964",
                            "from": 715827882,
                            "end": 1073741823
                        },
                        {
                            "workerId": "127.0.0.1:14964",
                            "from": 1789569705,
                            "end": 2147483646
                        },
                        {
                            "workerId": "127.0.0.1:14964",
                            "from": 2863311528,
                            "end": 3221225469
                        },
                        {
                            "workerId": "127.0.0.1:14964",
                            "from": 3937053351,
                            "end": 4294967292
                        }
                    ]
                }
            }
        }        
    )";
    HashRingPb hashRing;
    auto rc = google::protobuf::util::JsonStringToMessage(hashRingJsonStr, &hashRing);
    ASSERT_TRUE(rc.ok());

    std::string delWorkerAddress = hashRing.del_node_info().begin()->first;
    ASSERT_TRUE(!CheckScaleUpInfo(false, hashRing));
    ASSERT_TRUE(!CheckVoluntaryScaleDownInfo(false, hashRing));
    ASSERT_TRUE(CheckPassiveScaleDownInfo(false, hashRing));
    ASSERT_TRUE(!CheckWorkerInJoiningState(false, hashRing));
    ASSERT_TRUE(!CheckWorkerInLeavingState(false, hashRing));
    ASSERT_TRUE(!CheckWorkerInInitialState(false, hashRing));
    ASSERT_TRUE(!CheckPassiveScaleDownNotStart(false, hashRing));

    ASSERT_TRUE(CheckPassiveScaleDownInfo(true, hashRing));

    ASSERT_EQ(hashRing.del_node_info_size(), 0);
    const int expectedWorkerSize = 2;
    ASSERT_EQ(hashRing.workers_size(), expectedWorkerSize);
    for (const auto &info : hashRing.add_node_info()) {
        for (const auto &range : info.second.changed_ranges()) {
            if (range.workerid() == delWorkerAddress) {
                ASSERT_TRUE(range.finished());
            }
        }
    }
}

TEST_F(HashRingHealthCheckTest, TestPassiveScaleDownExistsScaleDownHashRing)
{
    std::string hashRingJsonStr = R"(
        {
            "clusterId": "127.0.0.1:32044",
            "workers": {
                "127.0.0.1:47805": {
                    "hashTokens": [715827882, 1789569705, 2863311528, 3937053351],
                    "workerUuid": "OTJlYjE0NzctZDA3Yi00Y2NjLTk5MDEtZTVmYjg1ZjEzZjlh",
                    "state": "ACTIVE"
                },
                "127.0.0.1:53257": {
                    "hashTokens": [1073741823, 2147483646, 3221225469, 4294967292],
                    "workerUuid": "YWRjNzQyNGEtNGM1MC00ZmVhLTk5YTYtYzFkNDlkZDAxMmJi",
                    "state": "ACTIVE"
                },
                "127.0.0.1:14964": {
                    "hashTokens": [357913941, 1431655764, 2505397587, 3579139410],
                    "workerUuid": "ZTBmYWRlMDItMDIyOS00YzU3LWIwMmMtYzdhMjRhN2MwZDVm",
                    "state": "LEAVING",
                    "needScaleDown": true
                }
            },
            "clusterHasInit": true,
            "delNodeInfo": {
                "127.0.0.1:14964": {
                    "changedRanges": [
                        {
                            "workerId": "127.0.0.1:47805",
                            "from": 4294967292,
                            "end": 357913941
                        },
                        {
                            "workerId": "127.0.0.1:47805",
                            "from": 1073741823,
                            "end": 1431655764
                        },
                        {
                            "workerId": "127.0.0.1:47805",
                            "from": 2147483646,
                            "end": 2505397587
                        },
                        {
                            "workerId": "127.0.0.1:47805",
                            "from": 3221225469,
                            "end": 3579139410
                        }
                    ]
                }
            },
            "addNodeInfo": {
                "127.0.0.1:53257": {
                    "changedRanges": [
                        {
                            "workerId": "127.0.0.1:14964",
                            "from": 715827882,
                            "end": 1073741823
                        },
                        {
                            "workerId": "127.0.0.1:14964",
                            "from": 1789569705,
                            "end": 2147483646
                        },
                        {
                            "workerId": "127.0.0.1:14964",
                            "from": 2863311528,
                            "end": 3221225469
                        },
                        {
                            "workerId": "127.0.0.1:14964",
                            "from": 3937053351,
                            "end": 4294967292
                        }
                    ]
                }
            }
        }
    )";
    HashRingPb hashRing;
    auto rc = google::protobuf::util::JsonStringToMessage(hashRingJsonStr, &hashRing);
    ASSERT_TRUE(rc.ok());

    std::string delWorkerAddress = hashRing.del_node_info().begin()->first;
    ASSERT_TRUE(!CheckScaleUpInfo(false, hashRing));
    ASSERT_TRUE(!CheckVoluntaryScaleDownInfo(false, hashRing));
    ASSERT_TRUE(CheckPassiveScaleDownInfo(false, hashRing));
    ASSERT_TRUE(!CheckWorkerInJoiningState(false, hashRing));
    ASSERT_TRUE(!CheckWorkerInLeavingState(false, hashRing));
    ASSERT_TRUE(!CheckWorkerInInitialState(false, hashRing));
    ASSERT_TRUE(!CheckPassiveScaleDownNotStart(false, hashRing));

    ASSERT_TRUE(CheckPassiveScaleDownInfo(true, hashRing));

    ASSERT_EQ(hashRing.del_node_info_size(), 0);
    const int expectedWorkerSize = 2;
    ASSERT_EQ(hashRing.workers_size(), expectedWorkerSize);
    ASSERT_EQ(hashRing.add_node_info_size(), 1);
    for (const auto &info : hashRing.add_node_info()) {
        for (const auto &range : info.second.changed_ranges()) {
            if (range.workerid() == delWorkerAddress) {
                ASSERT_TRUE(range.finished());
            }
        }
    }
}

TEST_F(HashRingHealthCheckTest, TestJoiningStateWithoutAddNodeInfo)
{
    std::string hashRingJsonStr = R"(
        {
            "clusterId": "127.0.0.1:32044",
            "workers": {
                "127.0.0.1:47805": {
                    "hashTokens": [715827882, 1789569705, 2863311528, 3937053351],
                    "workerUuid": "OTJlYjE0NzctZDA3Yi00Y2NjLTk5MDEtZTVmYjg1ZjEzZjlh",
                    "state": "ACTIVE"
                },
                "127.0.0.1:53257": {
                    "hashTokens": [1073741823, 2147483646, 3221225469, 4294967292],
                    "workerUuid": "YWRjNzQyNGEtNGM1MC00ZmVhLTk5YTYtYzFkNDlkZDAxMmJi",
                    "state": "JOINING"
                },
                "127.0.0.1:14964": {
                    "hashTokens": [357913941, 1431655764, 2505397587, 3579139410],
                    "workerUuid": "ZTBmYWRlMDItMDIyOS00YzU3LWIwMmMtYzdhMjRhN2MwZDVm",
                    "state": "ACTIVE"
                }
            },
            "clusterHasInit": true
        }
    )";
    HashRingPb hashRing;
    auto rc = google::protobuf::util::JsonStringToMessage(hashRingJsonStr, &hashRing);
    ASSERT_TRUE(rc.ok());

    ASSERT_TRUE(!CheckScaleUpInfo(false, hashRing));
    ASSERT_TRUE(!CheckVoluntaryScaleDownInfo(false, hashRing));
    ASSERT_TRUE(!CheckPassiveScaleDownInfo(false, hashRing));
    ASSERT_TRUE(CheckWorkerInJoiningState(false, hashRing));
    ASSERT_TRUE(!CheckWorkerInLeavingState(false, hashRing));
    ASSERT_TRUE(!CheckWorkerInInitialState(false, hashRing));
    ASSERT_TRUE(!CheckPassiveScaleDownNotStart(false, hashRing));
}

TEST_F(HashRingHealthCheckTest, TestLeavingStateWithoutAddNodeInfo)
{
    std::string hashRingJsonStr = R"(
        {
            "clusterId": "127.0.0.1:32044",
            "workers": {
                "127.0.0.1:47805": {
                    "hashTokens": [715827882, 1789569705, 2863311528, 3937053351],
                    "workerUuid": "OTJlYjE0NzctZDA3Yi00Y2NjLTk5MDEtZTVmYjg1ZjEzZjlh",
                    "state": "ACTIVE"
                },
                "127.0.0.1:53257": {
                    "hashTokens": [1073741823, 2147483646, 3221225469, 4294967292],
                    "workerUuid": "YWRjNzQyNGEtNGM1MC00ZmVhLTk5YTYtYzFkNDlkZDAxMmJi",
                    "state": "LEAVING"
                },
                "127.0.0.1:14964": {
                    "hashTokens": [357913941, 1431655764, 2505397587, 3579139410],
                    "workerUuid": "ZTBmYWRlMDItMDIyOS00YzU3LWIwMmMtYzdhMjRhN2MwZDVm",
                    "state": "ACTIVE"
                }
            },
            "clusterHasInit": true
        }
    )";
    HashRingPb hashRing;
    auto rc = google::protobuf::util::JsonStringToMessage(hashRingJsonStr, &hashRing);
    ASSERT_TRUE(rc.ok());

    ASSERT_TRUE(!CheckScaleUpInfo(false, hashRing));
    ASSERT_TRUE(!CheckVoluntaryScaleDownInfo(false, hashRing));
    ASSERT_TRUE(!CheckPassiveScaleDownInfo(false, hashRing));
    ASSERT_TRUE(!CheckWorkerInJoiningState(false, hashRing));
    ASSERT_TRUE(CheckWorkerInLeavingState(false, hashRing));
    ASSERT_TRUE(!CheckWorkerInInitialState(false, hashRing));
    ASSERT_TRUE(!CheckPassiveScaleDownNotStart(false, hashRing));
}

TEST_F(HashRingHealthCheckTest, TestInitialState)
{
    std::string hashRingJsonStr = R"(
        {
            "clusterId": "127.0.0.1:32044",
            "workers": {
                "127.0.0.1:47805": {
                    "hashTokens": [715827882, 1789569705, 2863311528, 3937053351],
                    "workerUuid": "OTJlYjE0NzctZDA3Yi00Y2NjLTk5MDEtZTVmYjg1ZjEzZjlh",
                    "state": "ACTIVE"
                },
                "127.0.0.1:53257": {
                    "hashTokens": [1073741823, 2147483646, 3221225469, 4294967292],
                    "workerUuid": "YWRjNzQyNGEtNGM1MC00ZmVhLTk5YTYtYzFkNDlkZDAxMmJi",
                    "state": "INITIAL"
                },
                "127.0.0.1:14964": {
                    "hashTokens": [357913941, 1431655764, 2505397587, 3579139410],
                    "workerUuid": "ZTBmYWRlMDItMDIyOS00YzU3LWIwMmMtYzdhMjRhN2MwZDVm",
                    "state": "ACTIVE"
                }
            },
            "clusterHasInit": true
        }
    )";
    HashRingPb hashRing;
    auto rc = google::protobuf::util::JsonStringToMessage(hashRingJsonStr, &hashRing);
    ASSERT_TRUE(rc.ok());

    ASSERT_TRUE(!CheckScaleUpInfo(false, hashRing));
    ASSERT_TRUE(!CheckVoluntaryScaleDownInfo(false, hashRing));
    ASSERT_TRUE(!CheckPassiveScaleDownInfo(false, hashRing));
    ASSERT_TRUE(!CheckWorkerInJoiningState(false, hashRing));
    ASSERT_TRUE(!CheckWorkerInLeavingState(false, hashRing));
    ASSERT_TRUE(CheckWorkerInInitialState(false, hashRing));
    ASSERT_TRUE(!CheckPassiveScaleDownNotStart(false, hashRing));
}

TEST_F(HashRingHealthCheckTest, TestPassiveScaleDownNotStart)
{
    std::string hashRingJsonStr = R"(
        {
            "clusterId": "127.0.0.1:32044",
            "workers": {
                "127.0.0.1:47805": {
                    "hashTokens": [715827882, 1789569705, 2863311528, 3937053351],
                    "workerUuid": "OTJlYjE0NzctZDA3Yi00Y2NjLTk5MDEtZTVmYjg1ZjEzZjlh",
                    "state": "ACTIVE"
                },
                "127.0.0.1:53257": {
                    "hashTokens": [1073741823, 2147483646, 3221225469, 4294967292],
                    "workerUuid": "YWRjNzQyNGEtNGM1MC00ZmVhLTk5YTYtYzFkNDlkZDAxMmJi",
                    "state": "ACTIVE"
                },
                "127.0.0.1:14964": {
                    "hashTokens": [357913941, 1431655764, 2505397587, 3579139410],
                    "workerUuid": "ZTBmYWRlMDItMDIyOS00YzU3LWIwMmMtYzdhMjRhN2MwZDVm",
                    "state": "ACTIVE"
                }
            },
            "clusterHasInit": true
        }
    )";
    HashRingPb hashRing;
    auto rc = google::protobuf::util::JsonStringToMessage(hashRingJsonStr, &hashRing);
    ASSERT_TRUE(rc.ok());

    ASSERT_TRUE(!CheckScaleUpInfo(false, hashRing));
    ASSERT_TRUE(!CheckVoluntaryScaleDownInfo(false, hashRing));
    ASSERT_TRUE(!CheckPassiveScaleDownInfo(false, hashRing));
    ASSERT_TRUE(!CheckWorkerInJoiningState(false, hashRing));
    ASSERT_TRUE(!CheckWorkerInLeavingState(false, hashRing));
    ASSERT_TRUE(!CheckWorkerInInitialState(false, hashRing));

    DS_ASSERT_OK(inject::Set("worker.CheckPassiveScaleDownNotStart", "call(127.0.0.1:14964)"));
    ASSERT_TRUE(CheckPassiveScaleDownNotStart(false, hashRing));
}
}  // namespace ut
}  // namespace datasystem
