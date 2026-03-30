# BUILD file for tbb with sources
package(default_visibility = ["//visibility:public"])

load("@rules_cc//cc:defs.bzl", "cc_library")
genrule(
    name = "gen_version_string_ver",
    srcs = [
            "build/version_info_linux.sh",
        ],
    outs = ["version_string.ver"],
    cmd = '''
bash $(location build/version_info_linux.sh) bazel_tbb > "$@"
''',
)

cc_library(
    name = "tbb",
    srcs = glob([
        "src/**/*.cpp",
    ], exclude=[
        "src/old/**",
        "src/perf/**",
        "src/test/**",
        "src/rml/test/**",
        "src/rml/perfor/**",
        "src/rml/server/**",
        "src/tbb/tbb_bind.cpp",
        "src/tbbmalloc/**",
        "src/tbbproxy/**",
    ]),
    hdrs = (
            glob([
                "include/**/*.h",
                "include/**/*.hpp",
                "include/**/*.inl",
                "include/**/*.inc",

                "src/**/*.h",
                "src/**/*.hpp",
                "src/tbb/dynamic_link.cpp",
                "src/**/*.inl",
                "src/**/*.inc",
                ":gen_version_string_ver",
            ],
            exclude = [
                #"src/rml/client/*.h",
             	"src/rml/test/*.h",
            ])
            +
            glob(
                ["include/**"],
                exclude = [
                    "include/**/*.h",
                    "include/**/*.inl",
                    "include/**/*.inc",
                    "include/**/*.html",
                    "include/**/*.md",
                    "include/**/*.txt",
                    "include/**/index.html",
                ],
            )
        ),
    textual_hdrs = [":gen_version_string_ver"],
    copts = [
        "-std=c++17",
        "-fPIC",
        "-DUSE_PTHREAD",
        "-D__TBB_BUILD=1",
        "-Wno-error=deprecated-declarations",
        "-D_POSIX_C_SOURCE=200809L",
        "-D_XOPEN_SOURCE=700",
        "-include", "stdexcept",
        "-Wno-error=class-memaccess",
        "-Wno-error=strict-aliasing",
        "-Wno-error=deprecated-copy"
    ],
    includes = [
        ".",
        "include",
        "src",
        "src/rml/include",
    ],
    linkopts = [
        "-ldl",
        "-pthread",
        "-lrt",
    ],
    visibility = ["//visibility:public"],
)