# Makefile for kvclient_standalone_test
#
# copy-sdk: Copy Bazel-built SDK .so files to third_party/sdk_lib/ for deployment.
#           Run `make copy-sdk` after `bash build.sh -b bazel` in the project root.

SDK_LIB_SRC ?= $(PWD)/../../output/datasystem/sdk/cpp/lib
SDK_LIB_DST := third_party/sdk_lib

.PHONY: copy-sdk clean-sdk

copy-sdk:
	@mkdir -p $(SDK_LIB_DST)
	@cp -u $(SDK_LIB_SRC)/*.so $(SDK_LIB_DST)/
	@echo "Copied $$(ls $(SDK_LIB_DST)/*.so | wc -l) .so files to $(SDK_LIB_DST)/"

clean-sdk:
	rm -rf $(SDK_LIB_DST)
