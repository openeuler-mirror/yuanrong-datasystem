# 安装yr-datasystem

<!-- TOC -->

- [快速安装yr-datasystem](#快速安装yr-datasystem版本)
    - [环境准备](#环境准备)
        - [安装Python](#安装python)
        - [安装CANN](#安装cann)
    - [pip安装](#pip安装)
    - [安装自定义版本](#安装自定义版本)
- [源码编译方式安装yr-datasystem](#源码编译方式安装yr-datasystem版本)
    - [环境准备](#环境准备-1)
        - [安装Python](#安装python-1)
        - [安装CANN](#安装cann-1)
        - [安装wheel和setuptools](#安装wheel和setuptools)
        - [安装GCC](#安装gcc)
        - [安装G++](#安装g)
        - [安装git patch make libtool](#安装git-patch-make-libtool)
        - [安装CMake](#安装cmake)
    - [从代码仓下载源码](#从代码仓下载源码)
    - [编译yr-datasystem](#编译yr-datasystem)
    - [安装yr-datasystem](#安装yr-datasystem)

<!-- /TOC -->

[![查看源文件](https://Mindspore-website.obs.cn-north-4.myhuaweicloud.com/website-images/master/resource/_static/logo_source.svg)](install.md)

本文档介绍如何在CPU/NPU环境的Linux系统上，快速安装yr-datasystem或者使用源码编译方式安装yr-datasystem。

## 快速安装yr-datasystem版本

### 环境准备

下表列出了运行yr-datasystem所需的系统环境和第三方依赖：

|软件名称|版本|作用|
|-|-|-|
|EulerOS 2.8/openEuler 20.03|-|运行yr-datasystem的操作系统|
|[CANN](#安装cann)|8.0.0或8.0.rc2|运行异构相关特性的依赖库|
|[Python](#安装python)|3.10-3.11|yr-datasystem的运行依赖Python环境|

下面给出第三方依赖的安装方法。

#### 安装CANN
在[Ascend官网](https://www.hiascend.com/hardware/firmware-drivers/community?product=1&model=30&cann=8.0.0.beta1&driver=Ascend+HDK+24.1.RC3)下载CANN run包，安装 run 包：
```bash
./Ascend-cann-toolkit_<version>_linux-<arch>.run --install
```
执行以上命令会打屏华为企业业务最终用户许可协议（EULA）的条款和条件，请输入Y或y同意协议，继续安装流程。

安装完成后，若显示如下信息，则说明软件安装成功：
```bash
xxx install success
```
xxx表示安装的实际软件包名。

如果用户未指定安装路径，则软件会安装到默认路径下，默认安装路径如下。root用户："/usr/local/Ascend"，非root用户："\${HOME}/Ascend"，${HOME}为当前用户目录。

配置环境变量，以非root用户安装后的默认路径为例，请用户根据set_env.sh的实际路径执行如下命令：
```bash
source ${HOME}/Ascend/ascend-toolkit/set_env.sh
```

#### 安装Python

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
conda create -n yr-datasystem_py311 python=3.11.4 -y
conda activate yr-datasystem_py311
```

可以通过以下命令查看Python版本。

```bash
python --version
```

### pip安装
安装PyPI上的版本：
```bash
pip install yr-datasystem
```

### 安装自定义版本
指定好yr_datasystem以及配套的Python版本，运行如下命令安装yr_datasystem包：
```bash
# 指定yr_datasystem版本为1.0.0
export version="1.0.0"
# 指定Python版本为3.11
export py_version="311"
pip install https://ms-release.obs.cn-north-4.myhuaweicloud.com/${version}/yr_datasystem/any/yr-datasystem-${version}-cp${py_version}-cp${py_version}-linux_x86_64.whl --trusted-host ms-release.obs.cn-north-4.myhuaweicloud.com -i https://pypi.tuna.tsinghua.edu.cn/simple
``` 

## 源码编译方式安装yr-datasystem版本

(源码环境准备)=
### 环境准备

下表列出了源码编译yr-datasystem所需的系统环境和第三方依赖：

|软件名称|版本|作用|
|-|-|-|
|EulerOS 2.8/openEuler 20.03|-|编译和运行yr-datasystem的操作系统|
|[CANN](#安装cann)|8.0.0或8.0.rc2|编译和运行异构相关特性的依赖库|
|[Python](#安装python)|3.10-3.11|yr-datasystem的使用依赖Python环境|
|[wheel](#安装wheel和setuptools)|0.32.0及以上|yr-datasystem使用的Python打包工具|
|[setuptools](#安装wheel和setuptools)|44.0及以上|yr-datasystem使用的Python包管理工具|
|[GCC](#安装gcc)|7.3.0|用于编译yr-datasystem的C编译器|
|[G++](#安装g)|7.3.0|用于编译yr-datasystem的C++编译器|
|[libtool](#安装git-patch-make-libtool)|-|编译构建yr-datasystem的工具|
|[git](#安装git-patch-make-libtool)|-|yr-datasystem使用的源代码管理工具|
|[Make](#安装git-patch-make-libtool)|-|yr-datasystem使用的源代码管理工具|
|[CMake](#安装cmake)|3.18.3及以上|编译构建yr-datasystem的工具|
|[patch](#安装git-patch-make-libtool)|2.5及以上|yr-datasystem使用的源代码补丁工具|

下面给出第三方依赖的安装方法。

#### 安装CANN

安装详细教程请参考：[安装CANN](#安装cann)

#### 安装Python

安装详细教程请参考：[安装Python](#安装python)

#### 安装wheel和setuptools

在安装完成Python后，使用以下命令安装。

```bash
pip install wheel
pip install -U setuptools
```

#### 安装GCC

使用以下命令安装。

```bash
sudo yum install gcc -y
```

#### 安装G++

使用以下命令安装。

```bash
sudo yum install gcc-c++ -y
```

#### 安装git patch make libtool

使用以下命令安装。

```bash
sudo yum install git patch make libtool
```

#### 安装CMake

根据系统架构选择不同的下载链接。

```bash
# x86使用
curl -O https://cmake.org/files/v3.19/cmake-3.19.8-Linux-x86_64.sh
# aarch64使用
curl -O https://cmake.org/files/v3.19/cmake-3.19.8-Linux-aarch64.sh
```

执行安装脚本安装CMake，默认安装到`/usr/local`目录下。

```bash
sudo mkdir /usr/local/cmake-3.19.8
sudo bash cmake-3.19.8-Linux-*.sh --prefix=/usr/local/cmake-3.19.8 --exclude-subdir
```

最后需要将CMake添加到`PATH`环境变量中。如果使用默认安装目录执行以下命令，其他安装目录需要做相应修改。

```bash
echo -e "export PATH=/usr/local/cmake-3.19.8/bin:\$PATH" >> ~/.bashrc
source ~/.bashrc
```

### 从代码仓下载源码

```bash
git clone https://gitee.com/openeuler/yuanrong-datasystem.git
```

### 初始化环境变量
在开始编译前需要初始化Ascend相关的环境变量：
```bash
source ${HOME}/Ascend/ascend-toolkit/set_env.sh
```
详细介绍请参考[安装CANN](#安装cann)章节。

### 编译yr-datasystem

进入yr-datasystem根目录，然后执行编译脚本。

```bash
cd yr-datasystem
bash build.sh
```

其中：

- `build.sh`中默认的编译线程数为8，如果编译机性能较差可能会出现编译错误，可在执行中增加-j{线程数}来减少线程数量。如`bash build.sh -j4`。
- 关于`build.sh`更多用法请执行`bash build.sh -h`获取帮助或者参看脚本头部的说明。

### 安装yr-datasystem

```bash
pip install output/yr_datasystem-*.whl
```
