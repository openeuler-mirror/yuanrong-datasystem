# Makefile for kvclient_standalone_test
#
# copy-sdk: Copy Bazel-built SDK to third_party/sdk/ for compilation & deployment.
#           Point BAZEL_SDK_DIR to the Bazel output directory containing include/ and lib/.
#
# Usage:
#   make copy-sdk BAZEL_SDK_DIR=/path/to/output/cpp

BAZEL_SDK_DIR ?= $(PWD)/../../output/cpp
SDK_DST := third_party/sdk

.PHONY: copy-sdk clean-sdk

copy-sdk:
	@mkdir -p $(SDK_DST)
	@cp $(BAZEL_SDK_DIR)/lib/*.so $(SDK_DST)/
	@chmod u+w $(SDK_DST)/*.so
	@echo "Copied SDK libs to $(SDK_DST)/"
	@echo "  $$(ls $(SDK_DST)/*.so | sed 's|.*/||')"

clean-sdk:
	rm -rf $(SDK_DST)
