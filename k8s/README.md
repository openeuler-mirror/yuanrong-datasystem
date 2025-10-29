# 构建Docker镜像指导文档

## 环境准备

1) 需要在Linux系统中安装好Docker。（[docker安装指导](https://docs.docker.com/engine/install/)）.

2) 本地下载基础的Linux docker镜像。可以是下面几种系统：

| 系统        |版本|
|-----------|---|
| ubuntu    |18.04|
| euler     |v2r9|
| openeuler |22.03|
| centos    |7.9|

可以通过`docker pull` 命令获取，或者其他渠道获取到这些基础镜像。下面以`docker pull`示例.

```shell
# docker pull ubuntu:18.04
```

## 构建镜像

通过`docker_build.sh`脚本执行构建。

构建示例如下：

```shell
# bash docker_build.sh -n datasystem -t 2.2 -s ubuntu -b "ubuntu:18.04"
```

在build目录输出 `datasystem_2.2.tar` docker镜像包。

## 加载镜像

通过 `docker load` 命令可以加载镜像到docker中。

**提醒：** 如果在本机中进行构建镜像，docker中已存在该镜像，可以不需要再加载镜像。

```shell
# docker load < datasystem_2.2.tar
```