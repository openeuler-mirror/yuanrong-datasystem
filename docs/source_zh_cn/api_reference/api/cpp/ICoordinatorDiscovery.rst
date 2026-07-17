ICoordinatorDiscovery
=====================

.. cpp:class:: ICoordinatorDiscovery

    :header-file: #include <datasystem/utils/service_discovery.h>
    :namespace: datasystem

    Coordinator 地址发现接口。用户可以实现该接口，并通过
    :cpp:member:`CoordinatorServiceDiscoveryOptions::coordinatorDiscovery` 注入 Coordinator 地址发现逻辑。

    **公共函数**

    .. cpp:function:: virtual Status GetCoordinators(std::vector<std::string> &serviceList) = 0

        获取 Coordinator 地址列表。

        当前 :cpp:class:`CoordinatorServiceDiscovery` 要求该接口返回且仅返回一个 Coordinator 地址；
        返回空列表或多个地址时，查询 Worker 将失败。

        参数：
            - **serviceList** - 传出参数。Coordinator 地址列表，每个地址的格式为 ``host:port``。

        返回：
            操作结果状态码。

    **实现示例**

    .. code-block:: cpp

        class CustomCoordinatorDiscovery : public datasystem::ICoordinatorDiscovery {
        public:
            datasystem::Status GetCoordinators(std::vector<std::string> &serviceList) override
            {
                serviceList = { "127.0.0.1:31511" };
                return datasystem::Status::OK();
            }
        };
