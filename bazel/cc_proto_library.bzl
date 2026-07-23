"""Compatibility wrapper for Bazel's native C++ proto rule.

DataSystem consumes protobuf 3.25.5, which provides cc_proto_library as a
native Bazel rule rather than an exported Starlark symbol. Source consumers
load this stable shim so the DataSystem BUILD files do not depend on a
protobuf-version-specific .bzl path.
"""

def cc_proto_library(**kwargs):
    native.cc_proto_library(**kwargs)
