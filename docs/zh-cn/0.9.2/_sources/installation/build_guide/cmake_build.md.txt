# 基于Cmake自定义编译


## 编译环境准备

|软件名称|版本|作用|
|-|-|-|
|openEuler|22.03/24.03|运行/编译openYuanrong datasystem的操作系统|
|[Python](#安装-python)|3.9-3.13|openYuanrong datasystem的运行/编译依赖Python环境|
|[CANN](#安装-cann非必须)|8.5|（非必须）运行/编译异构相关特性的依赖库|
|[rdma-core](#安装-rdma-core非必须)|35.1|（非必须）运行/编译RDMA特性的依赖库|
|[wheel](#安装wheel和setuptools)|0.35.1+|openYuanrong datasystem使用的Python打包工具|
|[setuptools](#安装wheel和setuptools)|44.0+|openYuanrong datasystem使用的Python包管理工具|
|[GCC](#安装编译依赖库)|7.5.0+|用于编译openYuanrong datasystem的C编译器|
|[G++](#安装编译依赖库)|7.5.0+|用于编译openYuanrong datasystem的C++编译器|
|[libtool](#安装编译依赖库)|-|编译构建openYuanrong datasystem的工具|
|[git](#安装编译依赖库)|-|openYuanrong datasystem使用的源代码管理工具|
|[Make](#安装编译依赖库)|-|openYuanrong datasystem使用的编译构建工具|
|[CMake](#安装编译依赖库)|3.18.3+|编译构建openYuanrong datasystem的工具|
|[patch](#安装编译依赖库)|2.5+|openYuanrong datasystem使用的源代码补丁工具|

### 方式1：使用openYuanrong提供的编译镜像

如果需要快速安装，可以使用构建好的编译镜像，镜像地址：swr.cn-southwest-2.myhuaweicloud.com/openyuanrong/datasystem-compile:<version>

 | 镜像版本 | 支持架构 |	 
 | ------ | ------ |	 
 | v0.6.3-openeuler-22.03 | X86_64、ARM64 |	 

```bash
docker pull swr.cn-southwest-2.myhuaweicloud.com/openyuanrong/datasystem-compile:v0.6.3-openeuler-22.03
```

**编译镜像说明**

- 编译镜像包含编译所需的所有依赖，包括C++ SDK、命令行工具、Python SDK等，可以快速体验openYuanrong datasystem。
- 编译镜像不包含[Ascend](#安装-cann非必须)和[Rdma](#安装-rdma-core非必须)相关依赖，需要自行安装。

### 方式2：自定义准备环境
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


#### 安装wheel和setuptools

在安装完成Python后，使用以下命令安装。

```bash
pip install wheel
pip install -U setuptools
```

#### 安装编译依赖库

使用以下命令安装。

```bash
sudo yum install gcc gcc-c++ git patch make libtool cmake -y
```

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

如需编译异构对象带昇腾的特性，需要在开始编译前需要初始化Ascend相关的环境变量，非 root 用户执行：
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
bash build.sh -X on
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
