load("@rules_cc//cc:defs.bzl", "cc_library")

def _normalize_import_root(imp):
    """
    Normalize ProtoInfo transitive_imports entries to protoc -I roots.

    Some versions/toolchains provide entries as:
      - string (already a directory)
      - File (sometimes a .proto file path, especially under _virtual_imports)

    We must convert them into a directory root such that:
      root + "/" + <import string> points to an existing file.
    """

    # If it's already a string, assume it's a directory root
    if type(imp) == "string":
        return imp

    # If it's a File
    p = imp.path

    # Case 1: virtual imports layout:
    #   .../_virtual_imports/<target>/google/protobuf/any.proto
    # include root should be:
    #   .../_virtual_imports/<target>
    marker = "/_virtual_imports/"
    if marker in p:
        before, after = p.split(marker, 1)

        # after starts with "<target>/..."
        target = after.split("/", 1)[0]
        return before + marker + target

    # Case 2: protobuf repo layout often contains /src/...
    #   .../external/com_google_protobuf/src/google/protobuf/any.proto
    # include root should be:
    #   .../external/com_google_protobuf/src
    marker2 = "/src/"
    if marker2 in p:
        before, _ = p.split(marker2, 1)
        return before + marker2[:-1]  # keep "/src" (without trailing slash)

    # Fallback: use file dirname (best-effort)
    return imp.dirname

def _zmq_proto_gen_impl(ctx):
    proto = ctx.file.proto
    protoc = ctx.executable.protoc
    plugin = ctx.executable.plugin

    base = proto.basename[:-len(".proto")]

    # 声明输出文件
    out_srv_cc = ctx.actions.declare_file(base + ".service.rpc.pb.cc")
    out_srv_h = ctx.actions.declare_file(base + ".service.rpc.pb.h")
    out_stub_cc = ctx.actions.declare_file(base + ".stub.rpc.pb.cc")
    out_stub_h = ctx.actions.declare_file(base + ".stub.rpc.pb.h")
    outs = [out_srv_cc, out_srv_h, out_stub_cc, out_stub_h]

    # 获取当前 repo 的根目录 (如果是外部库，则为 external/yuanrong-datasystem)
    # 如果是本地库，则为空字符串
    workspace_root = proto.owner.workspace_root

    # 重新计算真正的 proto_root 路径
    # 假设项目约定的 proto 根目录是相对于 repo 的 ctx.attr.proto_root (比如 "src")
    real_proto_root = workspace_root + "/" + ctx.attr.proto_root if workspace_root else ctx.attr.proto_root

    # 计算 proto 文件相对于 real_proto_root 的相对路径
    # 这就是我们要传给 protoc 的最终输入路径
    if proto.path.startswith(real_proto_root):
        # +1 是为了去掉开头的斜杠
        proto_input_path = proto.path[len(real_proto_root):].lstrip("/")
    else:
        proto_input_path = proto.path

    args = ctx.actions.args()

    # 1. 添加基础搜索路径
    args.add("-I", ".")  # 允许相对于 workspace 根目录查找
    args.add("-I", real_proto_root)  # 允许相对于项目的 proto_root 查找

    # 2. 处理依赖的 proto 路径
    seen = {}
    for d in ctx.attr.extra_protos:
        pi = d[ProtoInfo]
        for imp in pi.transitive_imports.to_list():
            root = _normalize_import_root(imp)
            if root and root not in seen:
                seen[root] = True
                args.add("-I", root)

    # 3. 设置输出目录
    # 我们希望插件生成的路径直接落在 Bazel 声明的文件所在目录
    # 因为我们在上面已经把 proto_input_path 缩减为相对路径了
    # 所以这里 --zmq_out 应该指向 declare_file 所在的物理根目录
    out_dir = out_srv_cc.path[:-len(proto_input_path.replace(".proto", ".service.rpc.pb.cc"))].rstrip("/")
    args.add("--zmq_out", out_dir)

    # 4. 插件配置
    args.add("--plugin=protoc-gen-zmq=" + plugin.path)

    # 5. 输入文件
    args.add(proto_input_path)

    # 收集所有输入
    dep_srcs_transitive = [d[ProtoInfo].transitive_sources for d in ctx.attr.extra_protos]
    inputs = depset(direct = [proto], transitive = dep_srcs_transitive)

    ctx.actions.run(
        executable = protoc,
        arguments = [args],
        inputs = inputs,
        tools = [plugin],
        outputs = outs,
        mnemonic = "ZmqProtoGen",
        progress_message = "Generating zmq protos for %s" % proto.path,
        env = dict(ctx.attr.env),
    )

    cc_ctx = cc_common.create_compilation_context(
        headers = depset([out_srv_h, out_stub_h]),
        includes = depset([out_srv_h.dirname]),  # 简化处理，直接包含头文件所在目录
    )

    return [
        DefaultInfo(files = depset(outs)),
        OutputGroupInfo(
            headers = depset([out_srv_h, out_stub_h]),
            sources = depset([out_srv_cc, out_stub_cc]),
        ),
        CcInfo(compilation_context = cc_ctx),
    ]

zmq_proto_gen = rule(
    implementation = _zmq_proto_gen_impl,
    attrs = {
        "proto": attr.label(
            allow_single_file = [".proto"],
            mandatory = True,
        ),
        "proto_root": attr.string(mandatory = True),
        "protoc": attr.label(
            executable = True,
            cfg = "exec",
            allow_files = True,
            mandatory = True,
        ),
        "plugin": attr.label(
            executable = True,
            cfg = "exec",
            allow_files = True,
            mandatory = True,
        ),
        "extra_protos": attr.label_list(
            providers = [ProtoInfo],
            default = [],
        ),
        "env": attr.string_dict(default = {}),
    },
)

def zmq_cc_proto_library(
        name,
        proto,
        proto_root,
        deps = [],
        visibility = None,
        env = {},
        extra_protos = []):
    gen_name = name + "_gen"
    zmq_proto_gen(
        name = gen_name,
        proto = proto,
        proto_root = proto_root,
        protoc = "@com_google_protobuf//:protoc",
        plugin = "@yuanrong-datasystem//src/datasystem/common/rpc/plugin_generator:zmq_plugin",  # 修改成代码中生成的target
        env = env,
        extra_protos = extra_protos,
        visibility = visibility,
    )

    cc_library(
        name = name,
        srcs = [":" + gen_name],
        hdrs = [":" + gen_name],
        deps = deps + [
            "@com_google_protobuf//:protobuf",
            "@yuanrong-datasystem//src/datasystem/common/rpc:rpc_server_stream_base",
            "@yuanrong-datasystem//src/datasystem/common/rpc:rpc_server",
            #@yuanrong-datasystem"//src/datasystem/common/rdma/npu:remote_h2d_manager_header",
            #@yuanrong-datasystem"//src/datasystem/common/rdma:fast_transport_manager_wrapper",
            "@yuanrong-datasystem//src/datasystem/common/rpc:rpc_unary_client_impl",
            "@yuanrong-datasystem//src/datasystem/common/rpc/zmq:zmq_unary_client_impl",
            "@yuanrong-datasystem//src/datasystem/common/rpc/zmq:exclusive_conn_mgr",
            "@yuanrong-datasystem//src/datasystem/common/rpc/zmq:zmq_client_stream_base",
            "@yuanrong-datasystem//src/datasystem/common/shared_memory:common_shared_memory",
            "@yuanrong-datasystem//src/datasystem/common/rpc/zmq:zmq_stub_impl",
        ],
        strip_include_prefix = ".",
        include_prefix = "datasystem/protos",
        visibility = visibility,
    )
