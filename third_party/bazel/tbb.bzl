load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")

def setup_tbb():
    """Setup tbb library for Bazel builds."""
    maybe(
        http_archive,
        name = "tbb",
        sha256 = "ebc4f6aa47972daed1f7bf71d100ae5bf6931c2e3144cf299c8cc7d041dca2f3",
        strip_prefix = "oneTBB-2020.3",
        urls = [
            "https://github.com/uxlfoundation/oneTBB/archive/v2020.3.tar.gz",
        ],
        patches = [
            "//third_party/patches/tbb:2020.3/soft-link.patch",
        ],
        patch_args = ["-p1"],
        build_file_content = """
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
""",
    )
