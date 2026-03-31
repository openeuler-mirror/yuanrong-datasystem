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
        "mkdir -p jemalloc_output",
        "cp -rL external/jemalloc/* jemalloc_output",
        "cd jemalloc_output",
        "CFLAGS=\"-fPIC -fdebug-prefix-map=$$BASE_DIR=.\" CXXFLAGS=\"-fPIC -fdebug-prefix-map=$$BASE_DIR=.\" ./autogen.sh &>/dev/null",
        "CFLAGS=\"-fPIC -fdebug-prefix-map=$$BASE_DIR=.\" CXXFLAGS=\"-fPIC -fdebug-prefix-map=$$BASE_DIR=.\" ./configure --with-pic --with-malloc-conf=narenas:1,background_thread:true,max_background_threads:100,oversize_threshold:107374182400,lg_extent_max_active_fit:63"
        + " --disable-cache-oblivious"
        + " --disable-zone-allocator"
        + " --without-export"
        + " --disable-shared"
        + " --enable-static"
        + " --disable-cxx"
        + " --enable-stats"
        #+ " --disable-initial-exec-tls"
        + " --with-jemalloc-prefix=datasystem_"
        + " &>/dev/null",
        "make -j16 &>/dev/null",
        "cp -H lib/libjemalloc.a ../$(location libjemalloc.a)",
        "cp -H include/jemalloc/jemalloc.h ../$(location jemalloc/jemalloc.h)",
        "CFLAGS=\"-fPIC -fdebug-prefix-map=$$BASE_DIR=.\" CXXFLAGS=\"-fPIC -fdebug-prefix-map=$$BASE_DIR=.\" ./configure --with-pic --with-malloc-conf=narenas:1,background_thread:true,max_background_threads:100,oversize_threshold:107374182400,lg_extent_max_active_fit:63"
        + " --enable-shared"
        + " --disable-cache-oblivious"
        + " --disable-zone-allocator"
        + " --disable-static"
        + " --disable-cxx"
        + " --enable-stats"
        + " --disable-initial-exec-tls"
        + " --with-jemalloc-prefix=datasystem_"
        + " &>/dev/null",
        "make -j16 &>/dev/null",
        "cp -H lib/libjemalloc.so.2 ../$(location libjemalloc.so.2)",
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
