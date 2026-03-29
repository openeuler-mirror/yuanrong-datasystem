load("@pybind11_bazel//:build_defs.bzl", "pybind_extension", "pybind_library")

def ds_cc_library(name, srcs = None, hdrs = None, copts = [], deps = None, **kwargs):
    native.cc_library(
        name = name,
        srcs = srcs or [],
        hdrs = hdrs or [],
        strip_include_prefix = "/src",
        deps = deps or [],
        copts = copts,
        **kwargs
    )

def _jemalloc_dynamic_transition_impl(settings, attr):
    return {
        "//:jemalloc_dynamic_internal": attr.jemalloc_dynamic,
    }

jemalloc_dynamic_transition = transition(
    implementation = _jemalloc_dynamic_transition_impl,
    inputs = [],
    outputs = ["//:jemalloc_dynamic_internal"],
)

def _jemalloc_pybind_extension_impl(ctx):
    actual = ctx.attr.actual_binary
    if type(actual) == "list":
        actual = actual[0]

    files = depset(transitive = [actual.files])
    runfiles = actual.default_runfiles

    providers = [
        DefaultInfo(
            files = files,
            runfiles = runfiles,
        ),
        actual[CcInfo],
    ]

    if PyInfo in actual:
        providers.append(actual[PyInfo])

    return providers

_jemalloc_pybind_extension = rule(
    implementation = _jemalloc_pybind_extension_impl,
    attrs = {
        "jemalloc_dynamic": attr.bool(),
        "actual_binary": attr.label(
            mandatory = True,
            cfg = jemalloc_dynamic_transition,  # 应用配置转换
            providers = [CcInfo],
        ),
        "_allowlist_function_transition": attr.label(
            default = "@bazel_tools//tools/allowlists/function_transition_allowlist",
        ),
    },
)

def pybind_extension_jemalloc(
        name,
        srcs = [],
        deps = [],
        jemalloc_dynamic = False,
        linkopts = [],
        additional_linker_inputs = [],
        **kwargs):
    actual_name = name + "_actual"

    pybind_extension(
        name = actual_name,
        srcs = srcs,
        deps = deps,
        linkopts = linkopts,
        additional_linker_inputs = additional_linker_inputs,
        visibility = ["//visibility:private"],
        **kwargs
    )

    _jemalloc_pybind_extension(
        name = name,
        jemalloc_dynamic = jemalloc_dynamic,
        #actual_binary = actual_name,
        actual_binary = ":" + actual_name + ".so",
        visibility = kwargs.get("visibility", ["//visibility:public"]),
    )
