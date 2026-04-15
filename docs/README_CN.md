# openYuanrong datasystem 文档

## 简介
此目录用于生成 openYuanrong datasystem 的中文文档。

## 目录结构说明
```text
docs
├── _ext // sphinx自定义扩展文件
├── source_zh_cn // 中文文档目录
├── _static // sphinx静态资源
├── Makefile // docs构建文件
├── README_CN.md // docs说明
└── requirements.txt // docs依赖项
```

## 文档构建

openYuanrong datasystem 的教程和 API 文档均可由 [Sphinx](https://www.sphinx-doc.org/en/master/) 工具生成。

1. 安装 Doxygen

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

2. 进入文档所在目录 `yuanrong-datasystem/docs`，安装该目录下 `requirements.txt` 文件中的依赖项

   ```bash
   cd yuanrong-datasystem/docs
   pip install -r requirements.txt
   ```

3. 在文档所在目录 `yuanrong-datasystem/docs` 下执行如下命令进行文档构建：

   ```bash
   make html
   ```

   完成后会新建 `build_zh_cn/html` 目录，该目录中存放了生成后的中文文档网页，打开 `build_zh_cn/html/index.html` 即可查看文档内容。

## 在线文档更新

在线文档发布到 `https://gitcode.com/openeuler/yuanrong-datasystem` 仓库的 `doc_pages` 分支。刷新在线文档时，需要将本地构建产物 `build_zh_cn/html/` 下的全部内容覆盖到 `doc_pages/docs/zh-cn/latest/` 中。

推荐使用 `git worktree` 配合 `rsync` 同步，避免漏掉隐藏文件，同时清理目标目录中的旧文件：

1. 在仓库根目录准备 `doc_pages` 工作树

   ```bash
   git fetch origin doc_pages:doc_pages
   git worktree add ../doc_pages doc_pages
   ```

2. 将编译后的文档完整同步到 `doc_pages` 分支对应目录

   ```bash
   cd yuanrong-datasystem/docs
   mkdir -p ../../doc_pages/docs/zh-cn/latest
   rsync -a --delete build_zh_cn/html/ ../../doc_pages/docs/zh-cn/latest/
   ```

3. 提交并推送 `doc_pages` 分支更新

   ```bash
   cd ../../doc_pages
   git status
   git add docs/zh-cn/latest
   git commit -m "docs: refresh zh-cn latest pages"
   git push origin doc_pages
   ```

说明：`rsync -a --delete build_zh_cn/html/ ../../doc_pages/docs/zh-cn/latest/` 会将 `build_zh_cn/html/` 中的文件内容和目录结构（包含隐藏文件）同步到 `doc_pages/docs/zh-cn/latest/`，并删除目标目录中源目录已不存在的旧文件，从而避免在线文档出现遗漏或残留文件。
