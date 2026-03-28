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

    # out_pb_h = ctx.actions.declare_file(base + ".pb.h")
    out_srv_cc = ctx.actions.declare_file(base + ".service.rpc.pb.cc")
    out_srv_h = ctx.actions.declare_file(base + ".service.rpc.pb.h")
    out_stub_cc = ctx.actions.declare_file(base + ".stub.rpc.pb.cc")
    out_stub_h = ctx.actions.declare_file(base + ".stub.rpc.pb.h")

    outs = [out_srv_cc, out_srv_h, out_stub_cc, out_stub_h]

    # IMPORTANT: protoc output dir must match declare_file() directory
    out_dir = out_srv_cc.dirname

    # Collect sources/imports from ProtoInfo deps (for proto imports)
    dep_srcs_transitive = []
    dep_imports_transitive = []
    for d in ctx.attr.extra_protos:
        pi = d[ProtoInfo]
        dep_srcs_transitive.append(pi.transitive_sources)
        dep_imports_transitive.append(pi.transitive_imports)

    dep_srcs = depset(transitive = dep_srcs_transitive)
    dep_imports = depset(transitive = dep_imports_transitive)

    args = ctx.actions.args()

    # Proto file dir (like CMake -I ${file_dir})
    args.add("-I", proto.dirname)

    # Proto root (like CMake -I ${proto_src_directory})
    # Keeping it even if redundant is harmless
    args.add("-I", ctx.attr.proto_root)

    # Add normalized include roots for dependencies (google/protobuf/any.proto etc.)
    # Deduplicate with a dict
    seen = {}
    for imp in dep_imports.to_list():
        root = _normalize_import_root(imp)
        if root and root not in seen:
            seen[root] = True
            args.add("-I", root)

    # Outputs
    # args.add("--cpp_out", out_dir)
    new_out_dir = out_dir[:out_dir.find("/datasystem/protos")]
    proto_path = proto.path
    if proto_path.startswith("src/"):
        proto_input_path = proto_path[4:]
    else:
        proto_input_path = proto_path
    args.add("--zmq_out", new_out_dir)

    # Plugin for --zmq_out
    args.add("--plugin=protoc-gen-zmq=" + plugin.path)

    # Input proto
    args.add(proto_input_path)

    # Inputs must include proto itself + all imported protos (sandbox readable)
    inputs = depset(
        direct = [proto],
        transitive = [dep_srcs],
    )

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

    prefix_to_remove = "/datasystem/protos"

    if out_dir.endswith(prefix_to_remove):
        new_include_dir = out_dir[:-len(prefix_to_remove)]

    # Export headers + include dir to dependents
    cc_ctx = cc_common.create_compilation_context(
        headers = depset([out_srv_h, out_stub_h]),
        includes = depset([new_include_dir]),
    )

    return [
        DefaultInfo(files = depset([out_srv_cc, out_srv_h, out_stub_cc, out_stub_h])),
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
        plugin = "//src/datasystem/common/rpc/plugin_generator:zmq_plugin",  # 修改成代码中生成的target
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
            "//src/datasystem/common/rpc:rpc_server_stream_base",
            "//src/datasystem/common/rpc:rpc_server",
            #"//src/datasystem/common/rdma/npu:remote_h2d_manager_header",
            #"//src/datasystem/common/rdma:fast_transport_manager_wrapper",
            "//src/datasystem/common/rpc:rpc_unary_client_impl",
            "//src/datasystem/common/rpc/zmq:zmq_unary_client_impl",
            "//src/datasystem/common/rpc/zmq:exclusive_conn_mgr",
            "//src/datasystem/common/rpc/zmq:zmq_client_stream_base",
            "//src/datasystem/common/shared_memory:common_shared_memory",
            "//src/datasystem/common/rpc/zmq:zmq_stub_impl",
        ],
        strip_include_prefix = ".",
        include_prefix = "datasystem/protos",
        visibility = visibility,
    )
