# API

```{eval-rst}
.. toctree::
   :glob:
   :maxdepth: 1
   :hidden:

   python/index
   cpp/index
   java/index
```

本节向您介绍开发openYuanrong datasystem的相关 API。

- [Python](./python/index.rst)
- [C++](./cpp/index.rst)
- [Java](./java/index.rst)

## AK/SK 初始化与轮换

C++ 的 `ObjectClient`、`KVClient`、`StreamClient` 在初始化时固定普通或 AK/SK 认证模式。
需要认证时，应在连接选项中同时配置非空 AK 和 SK，再初始化客户端。认证 RPC 的 AK 最大为 1024 字节。
已启用认证的客户端可以通过 `UpdateAkSk` 轮换非空凭证；初始化时没有启用认证的客户端，
调用该接口首次启用会返回 `K_NOT_SUPPORTED`，重试不会改变模式。

首次启用时，请停止使用旧客户端，释放其 Buffer、Producer、Consumer 等资源，关闭并释放旧客户端，
再用完整 AK/SK 创建并初始化新客户端。Java Object/Stream 的 `updateAkSk` 透传同一限制，失败时抛出
`DataSystemException`；Python 当前未提供运行时 AK/SK 更新接口，应通过重建客户端更换凭证。

升级时，未启用 AK/SK 的部署沿用普通 RPC 协议，可滚动升级。启用认证前，应先升级所有相关 SDK、Worker
及元数据服务组件；开启 AK/SK 的旧协议与新协议不支持混用，回滚前应先停止认证流量。
