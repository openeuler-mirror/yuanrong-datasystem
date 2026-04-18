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

在线文档发布到 `https://gitcode.com/openeuler/yuanrong-datasystem` 仓库的 `doc_pages` 分支。刷新在线文档时，必须先从上游仓 `https://gitcode.com/openeuler/yuanrong-datasystem.git` 或 `git@gitcode.com:openeuler/yuanrong-datasystem.git` 拉取最新的 `master` 分支代码，在该上游 `master` 工作树中重新构建 `build_zh_cn/html/`，再将构建产物的全部内容覆盖到 `doc_pages/docs/zh-cn/latest/` 中。

推荐使用 `git worktree` 配合 `rsync` 同步，避免漏掉隐藏文件，同时清理目标目录中的旧文件：

1. 在仓库根目录准备最新上游 `master` 的源码工作树，并在其中重新生成文档

   ```bash
   git remote add openeuler https://gitcode.com/openeuler/yuanrong-datasystem.git
   git fetch openeuler refs/heads/master:refs/remotes/openeuler/master
   git worktree add ../yuanrong-datasystem-docs-master openeuler/master
   cd ../yuanrong-datasystem-docs-master/docs
   make html
   ```

   如果环境要求使用 SSH，也可以将 `openeuler` 远端配置为 `git@gitcode.com:openeuler/yuanrong-datasystem.git`。

2. 在仓库根目录准备 `doc_pages` 工作树

   ```bash
   git fetch openeuler refs/heads/doc_pages:refs/remotes/openeuler/doc_pages
   git worktree add ../doc_pages openeuler/doc_pages
   ```

3. 将编译后的文档完整同步到 `doc_pages` 分支对应目录

   ```bash
   cd ../yuanrong-datasystem-docs-master/docs
   mkdir -p ../../doc_pages/docs/zh-cn/latest
   rsync -a --delete build_zh_cn/html/ ../../doc_pages/docs/zh-cn/latest/
   ```

4. 提交并推送基于最新上游 `doc_pages` 的刷新分支

   ```bash
   cd ../../doc_pages
   git checkout -b docs-refresh-zh-cn-latest
   git status
   git add docs/zh-cn/latest
   git commit -m "docs: refresh zh-cn latest pages"
   git push <your-fork-remote> docs-refresh-zh-cn-latest
   ```

   说明：不要将本地刷新分支直接推送到 `git@gitcode.com:openeuler/yuanrong-datasystem.git` 或 `https://gitcode.com/openeuler/yuanrong-datasystem.git`，应推送到个人 fork 或其他非上游远端后再发起 PR。

5. 提交 PR 时，PR 描述需要按 `.gitee/PULL_REQUEST_TEMPLATE/PULL_REQUEST_TEMPLATE.zh-cn.md` 模板填写本次文档更新说明、验证结果和接口影响说明，并明确写清楚本次在线文档是基于上游 `master` 的哪个 commit 生成，以及对应的文档生成时间。仓库内的 `ds-refresh-docs` skill 会在推送刷新分支到 fork 后自动调用 `ds-create-pr` 完成这一步。

说明：`rsync -a --delete build_zh_cn/html/ ../../doc_pages/docs/zh-cn/latest/` 会将 `build_zh_cn/html/` 中的文件内容和目录结构（包含隐藏文件）同步到 `doc_pages/docs/zh-cn/latest/`，并删除目标目录中源目录已不存在的旧文件，从而避免在线文档出现遗漏或残留文件。
