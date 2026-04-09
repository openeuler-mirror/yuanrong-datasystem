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