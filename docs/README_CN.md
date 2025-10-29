# openYuanrong datasystem 文档

## 简介
此目录用于生成 openYuanrong datasystem 的中文文档以及英文文档。

## 目录结构说明
```text
docs
├── _ext // sphinx自定义扩展文件
├── source_en // 英文文档目录
├── source_zh_cn // 中文文档目录
├── _static // sphinx静态资源
├── Makefile // docs构建文件
├── README_CN.md // docs说明
└── requirements.txt // docs依赖项
```

## 文档构建

openYuanrong datasystem的教程和API文档均可由[Sphinx](https://www.sphinx-doc.org/en/master/)工具生成，操作前需完成openYuanrong datasystem的安装。

1. 使用pip安装openYuanrong datasystem模块，API文档需要根据安装后的openYuanrong datasystem模块生成，参考[安装](source_zh_cn/getting-started/install.md)。

   ```bash
   pip install openyuanrong_datasystem-*.whl
   ```

2. 安装Doxygen

   - 使用系统包管理器安装:
      ```bash
      sudo yum install doxygen
      ```
   - 源码安装：
      ```bash
      # 安装依赖
      sudo yum groupinstall "Development Tools"
      sudo yum install cmake git flex bison

      # 下载源码，推荐版本为1.9.6
      git clone -b Release_1_9_6 https://github.com/doxygen/doxygen.git
      cd doxygen
      mkdir build
      cd build
      cmake ..
      make -j$(nproc)
      sudo make install
      ```

3. 进入文档所在目录`yuanrong-datasystem/docs`，安装该目录下`requirements.txt`文件中的依赖项

   ```bash
   cd yuanrong-datasystem/docs
   pip install -r requirements.txt
   ```

4. 进入文档所在目录`yuanrong-datasystem/docs`下执行如下命令进行文档构建：

   - 构建中文文档：
      ```bash
      make html
      ```
      完成后会新建`build_zh_cn/html`目录，该目录中存放了生成后的中文文档网页，打开`build_zh_cn/html/index.html`即可查看文档内容。

   - 构建英文文档：
      ```bash
      make html SOURCEDIR=source_en
      ```
      完成后会新建`build_en/html`目录，该目录中存放了生成后的英文文档网页，打开`build_en/html/index.html`即可查看文档内容。