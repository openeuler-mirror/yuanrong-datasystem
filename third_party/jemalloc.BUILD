#  Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.

genrule(
    name = "jemalloc-static-compile",
    srcs = glob(["**"]),
    outs = [
        "libjemalloc.a",
        "jemalloc/jemalloc.h",
    ],
    visibility = [
        "//visibility:public",
    ],
    cmd = " && ".join([
        "BASE_DIR=`pwd`",
        "COMMON_CONF_OPTS=\"\"",
        # Use parameter expansion with default to avoid `unbound variable` when the build
        # environment sets `nounset` for genrule shells.
        "if [ -n \"$${DS_JEMALLOC_LG_PAGE-}\" ]; then COMMON_CONF_OPTS=\"$$COMMON_CONF_OPTS --with-lg-page=$${DS_JEMALLOC_LG_PAGE}\"; fi",
        "mkdir -p jemalloc_static_output",
        "cp -rL $$(dirname $(location autogen.sh))/* jemalloc_static_output",
        "cd jemalloc_static_output",
        "CFLAGS=\"-fPIC -fdebug-prefix-map=$$BASE_DIR=.\" CXXFLAGS=\"-fPIC -fdebug-prefix-map=$$BASE_DIR=.\" ./autogen.sh &>/dev/null",
        "CFLAGS=\"-fPIC -fdebug-prefix-map=$$BASE_DIR=.\" CXXFLAGS=\"-fPIC -fdebug-prefix-map=$$BASE_DIR=.\" ./configure $$COMMON_CONF_OPTS --with-pic --with-malloc-conf=narenas:1,background_thread:true,max_background_threads:100,oversize_threshold:107374182400,lg_extent_max_active_fit:63"
        + " --disable-cache-oblivious"
        + " --disable-zone-allocator"
        + " --without-export"
        + " --disable-shared"
        + " --enable-static"
        + " --disable-cxx"
        + " --enable-stats"
        + " --disable-initial-exec-tls"
        + " --with-jemalloc-prefix=datasystem_"
        + " &>/dev/null",
        "make -j16 &>/dev/null",
        "cp -H lib/libjemalloc.a ../$(location libjemalloc.a)",
        "cp -H include/jemalloc/jemalloc.h ../$(location jemalloc/jemalloc.h)",
        "cd -",
        "rm -rf jemalloc_static_output",
    ]),
)

genrule(
    name = "jemalloc-shared-compile",
    srcs = glob(["**"]),
    outs = [
        "shared/libjemalloc.so.2",
        "shared/jemalloc/jemalloc.h",
    ],
    visibility = [
        "//visibility:public",
    ],
    cmd = " && ".join([
        "BASE_DIR=`pwd`",
        "COMMON_CONF_OPTS=\"\"",
        "if [ -n \"$${DS_JEMALLOC_LG_PAGE-}\" ]; then COMMON_CONF_OPTS=\"$$COMMON_CONF_OPTS --with-lg-page=$${DS_JEMALLOC_LG_PAGE}\"; fi",
        "mkdir -p jemalloc_shared_output",
        "cp -rL $$(dirname $(location autogen.sh))/* jemalloc_shared_output",
        "cd jemalloc_shared_output",
        "CFLAGS=\"-fPIC -fdebug-prefix-map=$$BASE_DIR=.\" CXXFLAGS=\"-fPIC -fdebug-prefix-map=$$BASE_DIR=.\" ./autogen.sh &>/dev/null",
        "CFLAGS=\"-fPIC -fdebug-prefix-map=$$BASE_DIR=.\" CXXFLAGS=\"-fPIC -fdebug-prefix-map=$$BASE_DIR=.\" LDFLAGS=\"-Wl,-z,now\" ./configure $$COMMON_CONF_OPTS --with-pic"
        + " --enable-shared"
        + " --disable-static"
        + " --disable-cxx"
        + " --enable-stats"
        + " &>/dev/null",
        "make -j16 &>/dev/null",
        "cp -H lib/libjemalloc.so.2 ../$(location shared/libjemalloc.so.2)",
        "cp -H include/jemalloc/jemalloc.h ../$(location shared/jemalloc/jemalloc.h)",
        "cd -",
        "rm -rf jemalloc_shared_output",
    ]),
)

genrule(
    name = "jemalloc-prof-shared-compile",
    srcs = glob(["**"]),
    outs = [
        "prof/libjemalloc.so.2",
        "prof/jemalloc/jemalloc.h",
    ],
    visibility = [
        "//visibility:public",
    ],
    cmd = " && ".join([
        "BASE_DIR=`pwd`",
        "COMMON_CONF_OPTS=\"\"",
        "if [ -n \"$${DS_JEMALLOC_LG_PAGE-}\" ]; then COMMON_CONF_OPTS=\"$$COMMON_CONF_OPTS --with-lg-page=$${DS_JEMALLOC_LG_PAGE}\"; fi",
        "mkdir -p jemalloc_prof_shared_output",
        "cp -rL $$(dirname $(location autogen.sh))/* jemalloc_prof_shared_output",
        "cd jemalloc_prof_shared_output",
        "CFLAGS=\"-fPIC -fdebug-prefix-map=$$BASE_DIR=.\" CXXFLAGS=\"-fPIC -fdebug-prefix-map=$$BASE_DIR=.\" ./autogen.sh &>/dev/null",
        "CFLAGS=\"-fPIC -fdebug-prefix-map=$$BASE_DIR=.\" CXXFLAGS=\"-fPIC -fdebug-prefix-map=$$BASE_DIR=.\" LDFLAGS=\"-Wl,-z,now\" ./configure $$COMMON_CONF_OPTS --with-pic"
        + " --enable-shared"
        + " --disable-static"
        + " --disable-cxx"
        + " --enable-stats"
        + " --enable-prof"
        + " &>/dev/null",
        "make -j16 &>/dev/null",
        "cp -H lib/libjemalloc.so.2 ../$(location prof/libjemalloc.so.2)",
        "cp -H include/jemalloc/jemalloc.h ../$(location prof/jemalloc/jemalloc.h)",
        "cd -",
        "rm -rf jemalloc_prof_shared_output",
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
        "shared/libjemalloc.so.2",
    ],
    hdrs = [
        "shared/jemalloc/jemalloc.h",
    ],
    includes = ["shared"],
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

cc_library(
    name = "jemalloc_prof_shared",
    srcs = [
        "prof/libjemalloc.so.2",
    ],
    hdrs = [
        "prof/jemalloc/jemalloc.h",
    ],
    includes = ["prof"],
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
