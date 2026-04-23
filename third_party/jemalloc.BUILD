#  Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.

genrule(
    name = "jemalloc-compile",
    srcs = glob(["**"]),
    outs = [
        "libjemalloc.a",
        "libjemalloc.so.2",
        "jemalloc/jemalloc.h",
    ],
    visibility = [
        "//visibility:public",
    ],
    cmd = " && ".join([
        "BASE_DIR=`pwd`",
        "EXTRA_CONF_OPTS=\"\"",
        # Use parameter expansion with default to avoid `unbound variable` when the build
        # environment sets `nounset` for genrule shells.
        "if [ -n \"$${DS_JEMALLOC_LG_PAGE-}\" ]; then EXTRA_CONF_OPTS=\"$$EXTRA_CONF_OPTS --with-lg-page=$${DS_JEMALLOC_LG_PAGE}\"; fi",
        "mkdir -p jemalloc_output",
        "cp -rL external/jemalloc_kvc/* jemalloc_output",
        "cd jemalloc_output",
        "CFLAGS=\"-fPIC -fdebug-prefix-map=$$BASE_DIR=.\" CXXFLAGS=\"-fPIC -fdebug-prefix-map=$$BASE_DIR=.\" ./autogen.sh &>/dev/null",
        "CFLAGS=\"-fPIC -fdebug-prefix-map=$$BASE_DIR=.\" CXXFLAGS=\"-fPIC -fdebug-prefix-map=$$BASE_DIR=.\" ./configure $$EXTRA_CONF_OPTS --with-pic --with-malloc-conf=narenas:1,background_thread:true,max_background_threads:100,oversize_threshold:107374182400,lg_extent_max_active_fit:63"
        + " --disable-cache-oblivious"
        + " --disable-zone-allocator"
        + " --enable-shared"
        + " --enable-static"
        + " --disable-cxx"
        + " --enable-stats"
        + " --disable-initial-exec-tls"
        + " --with-jemalloc-prefix=datasystem_"
        + " &>/dev/null",
        "make -j16 &>/dev/null",
        "cp -H lib/libjemalloc.a ../$(location libjemalloc.a)",
        "cp -H lib/libjemalloc.so.2 ../$(location libjemalloc.so.2)",
        "cp -H include/jemalloc/jemalloc.h ../$(location jemalloc/jemalloc.h)",
        "cd -",
        "rm -rf jemalloc_output",
    ]),
)

# Default use static library
cc_library(
    name = "jemalloc",
    srcs = [
        "libjemalloc.a",
    ],
    hdrs = [
        "jemalloc/jemalloc.h",
    ],
    includes = ["."],
    linkopts = [
        "-pthread",
        "-ldl",
        "-lm",
        "-lstdc++",
    ],
    visibility = [
        "//visibility:public",
    ],
    alwayslink = 1,
)

cc_library(
    name = "jemalloc_shared",
    srcs = [
        "libjemalloc.so.2",
    ],
    hdrs = [
        "jemalloc/jemalloc.h",
    ],
    includes = ["."],
    linkopts = [
        "-pthread",
        "-ldl",
        "-lm",
        "-lstdc++",
    ],
    visibility = [
        "//visibility:public",
    ],
)
