load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("//third_party/bazel:grpc_upb_repository.bzl", "grpc_upb_repository")
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")


def preload_grpc():
    http_archive(
        name="utf8_range",
        strip_prefix="utf8_range-d863bc33e15cba6d873c878dcca9e6fe52b2f8cb",
        sha256="568988b5f7261ca181468dba38849fabf59dd9200fb2ed4b2823da187ef84d8c",
        urls=[
            "https://github.com/protocolbuffers/utf8_range/archive/d863bc33e15cba6d873c878dcca9e6fe52b2f8cb.zip"
        ],
    )

    http_archive(
        name="cython",
        url="https://github.com/cython/cython/archive/3.0.10.tar.gz",
        sha256="e7fd54afdfef123be52a63e17e46eec2942f6a8012c97030dc68e6e10ed16f13",
        strip_prefix="cython-3.0.10",
        build_file="@com_github_grpc_grpc//third_party:cython.BUILD",
    )

    http_archive(
        name="zlib",
        strip_prefix="zlib-1.3.1",
        urls=["https://github.com/madler/zlib/archive/v1.3.1.tar.gz"],
        sha256="17e88863f3600672ab49182f217281b6fc4d3c762bde361935e436a95214d05c",
        build_file="@com_google_protobuf//:third_party/zlib.BUILD",
    )

    http_archive(
        name="com_github_grpc_grpc",
        sha256="853b4ff0e1c3e1c4e19f8cc77bbab402981920997716003cea6db9970657f8c9",
        strip_prefix="grpc-1.65.4",
        urls=[
            "https://github.com/grpc/grpc/archive/v1.65.4.tar.gz",
        ],
    )

    http_archive(
        name="com_google_protobuf",
        sha256="4356e78744dfb2df3890282386c8568c85868116317d9b3ad80eb11c2aecf2ff",
        strip_prefix="protobuf-3.25.5",
        urls=[
            "https://github.com/protocolbuffers/protobuf/archive/v3.25.5.tar.gz",
        ],
        patches=[
            "//third_party/patches/protobuf/3.25.5:protobuf_support_gcc_7_3.patch"
        ],
        patch_args=["-p1"],
    )

    grpc_upb_repository(name="upb", path=Label("@com_github_grpc_grpc//:WORKSPACE"))

    http_archive(
        name="com_google_googleapis",
        url="https://github.com/googleapis/googleapis/archive/541b1ded4abadcc38e8178680b0677f65594ea6f.tar.gz",
        sha256="62c5a6c15f60c4abf4b45d612954094993d709f83e8e8844fd02f27ef86e7ee6",
        strip_prefix="googleapis-541b1ded4abadcc38e8178680b0677f65594ea6f",
    )

    http_archive(
        name="com_github_cares_cares",
        url="https://github.com/c-ares/c-ares/archive/cares-1_19_1.tar.gz",
        sha256="9eadec0b34015941abdf3eb6aead694c8d96a192a792131186a7e0a86f2ad6d9",
        strip_prefix="c-ares-cares-1_19_1",
    )
