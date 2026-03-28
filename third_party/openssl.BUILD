cc_library(
    name = "crypto",
    hdrs = glob(["include/openssl/*.h"]) + ["include/openssl/opensslconf.h"],
    srcs = ["libcrypto.a"],
    includes = ["include"],
    linkopts = ["-pthread", "-ldl"],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "ssl",
    deps = [":crypto"],
    hdrs = glob(["include/openssl/*.h"]) + ["include/openssl/opensslconf.h"],
    srcs = ["libssl.a"],
    includes = ["include"],
    visibility = ["//visibility:public"],
)

genrule(
    name = "openssl-compile",
    srcs = glob(["**/*"], exclude=["bazel-*"]),
    outs = [
        "libcrypto.a",
        "libssl.a",
        "include/openssl/opensslconf.h",
    ],
    cmd = select({
        "//conditions:default": "export openssl_config='config' &&",
    }) + " && ".join([
        "mkdir -p openssl-output",
        "cp -rL external/openssl/* openssl-output",
        "cd openssl-output",
        "./$${openssl_config} &>/dev/null",
        "make -j4 &>/dev/null",
        "cp -H libcrypto.a ../$(location libcrypto.a)",
        "cp -H libssl.a ../$(location libssl.a)",
        "cp -H include/openssl/opensslconf.h ../$(location include/openssl/opensslconf.h)",
        "cd -",
        "rm -rf openssl-output",
    ]),
    visibility = ["//visibility:public"],
)

filegroup(
    name = "openssl-src",
    srcs = glob(["**/*"]),
    visibility = ["//visibility:public"],
)