"""Creates a copy_file target when an optional repository artifact exists."""

load("@bazel_skylib//rules:copy_file.bzl", "copy_file")

def optional_copy_file(name, srcs, out, visibility = None):
    if len(srcs) > 1:
        fail("%s accepts at most one source, got %d" % (name, len(srcs)))
    if srcs:
        copy_file(
            name = name,
            src = srcs[0],
            out = out,
            visibility = visibility,
        )
    else:
        native.filegroup(
            name = name,
            srcs = [],
            visibility = visibility,
        )
