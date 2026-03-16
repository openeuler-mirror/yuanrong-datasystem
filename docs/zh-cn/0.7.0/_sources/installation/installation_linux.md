(overview-installation)=

# 安装

<!-- TOC -->

- [安装 PyPI 正式发布版本（推荐）](#安装-pypi-正式发布版本推荐) 
- [环境准备](#环境准备)
    - [基础依赖](#基础依赖)
    - [源码编译额外依赖](#源码编译额外依赖)
    - [编译镜像下载](#编译镜像下载)
- [安装 nightly 版本](#安装-nightly-版本)
- [源码编译安装](#源码编译安装)
    - [下载源码](#下载源码)
    - [编译](#编译)
    - [安装](#安装)

<!-- /TOC -->

## 安装 PyPI 正式发布版本（推荐）


openYuanrong datasystem 已发布至 [PyPI](https://pypi.org/project/openyuanrong-datasystem/)，您可以通过 pip 直接安装。

**前置要求**

通过pip安装 openYuanrong datasystem 之前，请确保满足以下要求：

- **Python 版本**：Python 3.9、3.10 或 3.11
- **操作系统**：Linux（推荐 glibc 2.34+）
- **架构**：x86-64

您可以通过以下命令检查：

```bash
# python版本
python --version
# 操作系统
uname -s
# 架构
uname -m
# glibc版本
ldd --version
```

**安装完整发行版**（包含 Python SDK、C++ SDK 以及命令行工具）：

```bash
pip install openyuanrong-datasystem
```


**验证安装**：

安装完成后，您可以通过以下命令验证安装是否成功：

```bash
python -c "import yr.datasystem; print('openYuanrong datasystem installed successfully')"

dscli --version
```

> 如果在安装过程中遇到如 `pip: command not found` 等报错，请参照[环境准备](#环境准备)章节安装相关依赖

## 环境准备

本节列出两类依赖：
- **基础依赖**：运行和编译**都必须**满足
- **编译依赖**：仅源码编译时需要，在基础依赖之上**追加**
### 基础依赖

|软件名称|版本|作用|
|-|-|-|
|openEuler|22.03|运行/编译openYuanrong datasystem的操作系统|
|[Python](#安装-python)|3.9-3.11|openYuanrong datasystem的运行/编译依赖Python环境|
|[CANN](#安装-cann)|8.2.RC1|运行/编译异构相关特性的依赖库|
|[rdma-core](#安装-rdma-core)|35.1|运行/编译RDMA特性的依赖库|


#### 安装 Python

<details>
<summary>Python安装步骤（点我展开）</summary>

[Python](https://www.python.org/)可通过Conda进行安装。

安装Miniconda：

```bash
cd /tmp
curl -O https://mirrors.tuna.tsinghua.edu.cn/anaconda/miniconda/Miniconda3-py311_23.10.0-1-Linux-$(arch).sh
bash Miniconda3-py311_23.10.0-1-Linux-$(arch).sh -b
cd -
. ~/miniconda3/etc/profile.d/conda.sh
conda init bash
```

执行完上述命令后，重启shell，conda会在shell初始化完成后准备就绪。

安装完成后，可以为Conda设置清华源加速下载，参考[此处](https://mirrors.tuna.tsinghua.edu.cn/help/anaconda/)。

创建虚拟环境，以Python 3.11.4为例：

```bash
conda create -n py311 python=3.11.4 -y
conda activate py311
```

可以通过以下命令查看Python版本。

```bash
python --version
```

</details>

#### 安装 CANN

> 如无需运行/编译异构对象特性，可跳过 CANN 安装步骤

<details>
<summary>CANN安装步骤（点我展开）</summary>

CANN的安装依赖Python环境，请确保您在开始安装CANN之前环境中的Python已经就绪。

在[Ascend官网](https://www.hiascend.com/developer/download/community/result?module=cann&cann=8.2.RC1)下载CANN run包，安装 run 包：
```bash
chmod +x ./Ascend-cann-toolkit_<version>_linux-<arch>.run
./Ascend-cann-toolkit_<version>_linux-<arch>.run --install --quiet
```

安装完成后，若显示如下信息，则说明软件安装成功：
```bash
Toolkit:  Ascend-cann-toolkit_<version>_linux-<arch> install success
```

如果用户未指定安装路径，则软件会安装到默认路径下，默认安装路径如下：
| 用户身份   | 默认路径                |
| ------ | ------------------- |
| root   | `/usr/local/Ascend` |
| 非 root | `$HOME/Ascend`     |

加载环境变量（非 root 示例）：
```bash
source ${HOME}/Ascend/ascend-toolkit/set_env.sh
```
</details>

#### 安装 rdma-core

> 如无需运行/编译RDMA特性，可跳过 rdma-core 安装步骤

<details>
<summary>rdma-core安装步骤（点我展开）</summary>

[rdma-core](https://github.com/linux-rdma/rdma-core)可通过yum进行安装。

安装rdma-core:
```bash
sudo yum install rdma-core-devel
```

安装完成后，可通过以下命令查看软件是否安装成功：
```bash
ls -l /usr/lib64/libibverbs.so
ls -l /usr/lib64/librdmacm.so
```
若输出类似以下内容则说明安装成功：
```bash
lrwxrwxrwx. 1 root root 15 Mar 23  2022 /usr/lib64/libibverbs.so -> libibverbs.so.1
lrwxrwxrwx. 1 root root 14 Mar 23  2022 /usr/lib64/librdmacm.so -> librdmacm.so.1
```

</details>

### 源码编译额外依赖

> 如无需源码编译 openYuanrong datasystem，请跳过本章节。

下表列出了源码编译openYuanrong datasystem所需额外第三方依赖：

|软件名称|版本|作用|
|-|-|-|
|[wheel](#安装wheel和setuptools)|0.35.1+|openYuanrong datasystem使用的Python打包工具|
|[setuptools](#安装wheel和setuptools)|44.0+|openYuanrong datasystem使用的Python包管理工具|
|[GCC](#安装依赖库)|7.5.0+|用于编译openYuanrong datasystem的C编译器|
|[G++](#安装依赖库)|7.5.0+|用于编译openYuanrong datasystem的C++编译器|
|[libtool](#安装依赖库)|-|编译构建openYuanrong datasystem的工具|
|[git](#安装依赖库)|-|openYuanrong datasystem使用的源代码管理工具|
|[Make](#安装依赖库)|-|openYuanrong datasystem使用的源代码管理工具|
|[CMake](#安装依赖库)|3.18.3+|编译构建openYuanrong datasystem的工具|
|[patch](#安装依赖库)|2.5+|openYuanrong datasystem使用的源代码补丁工具|

#### 安装wheel和setuptools

在安装完成Python后，使用以下命令安装。

```bash
pip install wheel
pip install -U setuptools
```

#### 安装依赖库

使用以下命令安装。

```bash
sudo yum install gcc gcc-c++ git patch make libtool cmake -y
```

### 编译镜像下载

如果需要快速安装，可以使用构建好的编译镜像，镜像地址：swr.cn-southwest-2.myhuaweicloud.com/openyuanrong/datasystem-compile:<version>

 | 镜像版本 | 支持架构 |	 
 | ------ | ------ |	 
 | v0.6.3-openeuler-22.03 | X86_64、ARM64 |	 

```bash
docker pull swr.cn-southwest-2.myhuaweicloud.com/openyuanrong/datasystem-compile:v0.6.3-openeuler-22.03
```

#### 编译镜像说明

- 编译镜像包含编译所需的所有依赖，包括C++ SDK、命令行工具、Python SDK等，可以快速体验openYuanrong datasystem。
- 编译镜像不包含[Ascend](#安装-cann)和[Rdma](#安装-rdma-core)相关依赖，需要自行安装。

## 安装 nightly 版本

如需安装指定版本（或未正式发布 wheel），可用官方 nightly 地址。wheel文件命名遵循：

```shell
# 完整版wheel包（包含Python SDK、C++ SDK以及命令行工具）
openYuanrong_datasystem-{version}-cp{major}{minor}-cp{major}{minor}-manylinux_{glibc_major}_{glibc_minor}_{arch}.whl

# Python SDK wheel包（不包含C++ SDK以及命令行工具）
openYuanrong_datasystem_sdk-{version}-cp{major}{minor}-cp{major}{minor}-manylinux_{glibc_major}_{glibc_minor}_{arch}.whl
```

占位符说明：
- version：openYuanrong datasystem的版本号，例如 `0.5.0`
- major / minor：Python 解释器版本，例如 Python 3.9 → `39`
- glibc_major / glibc_minor：GLIBC 版本，例如 GLIBC 2.34 → `2_34`
- arch：CPU 架构，`x86_64` 或 `aarch64`

各版本 wheel 包的文件名与下载链接可在 openYuanrong 官方「版本发布」页面中查询（包含最新稳定版本与历史版本）：[版本发布与下载](https://pages.openeuler.openatom.cn/openyuanrong/docs/zh-cn/latest/reference/releases.html)。

下面示例安装 0.5.0，Python3.9，GLIBC2.34，aarch64 的完整版wheel包：
```bash
pip install https://openyuanrong.obs.cn-southwest-2.myhuaweicloud.com/openyuanrong_datasystem-0.5.0-cp39-cp39-manylinux_2_34_aarch64.whl
```

## 源码编译安装

### 下载源码

```bash
git clone https://gitcode.com/openeuler/yuanrong-datasystem.git
```

### 编译

如果构建环境中没有CANN或者无需使用异构对象的功能，可以通过 `-X` 参数禁用异构对象的编译：
```bash
cd yuanrong-datasystem
bash build.sh -X off
```

如需编译全量特性，需要在开始编译前需要初始化Ascend相关的环境变量，非 root 用户执行：
```bash
source ${HOME}/Ascend/ascend-toolkit/set_env.sh
```
root 用户执行：
```bash
source /usr/local/Ascend/ascend-toolkit/set_env.sh
```

执行编译脚本:

```bash
cd yuanrong-datasystem
bash build.sh
```

其中：

- `build.sh`中默认的编译线程数为8，如果编译机性能较差可能会出现编译错误，可在执行中增加-j{线程数}来减少线程数量。如`bash build.sh -j4`。
- 关于`build.sh`更多用法请执行`bash build.sh -h`获取帮助或者参看脚本头部的说明。


编译成功后，会在output产生如下编译产物：

```text
output/
|-- openyuanrong_datasystem-0.5.0-cp311-cp311-manylinux_2_34_x86_64.whl
|-- yr-datasystem-v0.5.0.tar.gz
```

### 安装

```bash
pip install output/openyuanrong_datasystem-*.whl
```
