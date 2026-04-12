(overview-installation)=

# 安装指南

## 环境依赖

|软件名称|版本|作用|
|-|-|-|
|openEuler|22.03/24.03|推荐运行openYuanrong datasystem的操作系统|
|[Python](#安装python)|3.9-3.13|openYuanrong datasystem的运行/编译依赖Python环境|
|[CANN](#安装-cann非必须)|8.5|（非必须）运行/编译异构相关特性的依赖库|
|[rdma-core](#安装-rdma-core非必须)|35.1|（非必须）运行/编译RDMA特性的依赖库|

## 安装 PyPI 正式发布版本（推荐）
openYuanrong datasystem 已发布至 [PyPI](https://pypi.org/project/openyuanrong-datasystem/)，您可以通过 pip 直接安装。

```bash
pip install openyuanrong-datasystem
```


**验证安装**：

安装完成后，您可以通过以下命令验证安装是否成功：

```bash
python -c "import yr.datasystem; print('openYuanrong datasystem installed successfully')"

dscli --version
```

> 如果在安装过程中遇到如 `pip: command not found` 等报错，请参照[环境依赖安装指南](#环境依赖安装指南)章节安装相关依赖

## 安装 nightly 版本

如需安装指定版本（或未正式发布 wheel），可用官方 nightly 地址。wheel文件命名遵循：

```shell
# 完整版wheel包（包含Python SDK、C++ SDK以及命令行工具）
openyuanrong_datasystem-{version}-cp{major}{minor}-cp{major}{minor}-manylinux_{glibc_major}_{glibc_minor}_{arch}.whl

# Python SDK wheel包（不包含C++ SDK以及命令行工具）
openyuanrong_datasystem_sdk-{version}-cp{major}{minor}-cp{major}{minor}-manylinux_{glibc_major}_{glibc_minor}_{arch}.whl
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

## 环境依赖安装指南

### 安装Python

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

### 安装 CANN（非必须）

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

### 安装 rdma-core（非必须）

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
