# Makefile for kvtest
#
# Targets:
#   copy-sdk  - Copy Bazel-built SDK .so to third_party/sdk/
#   package   - Assemble binary + libs + scripts + configs into output/
#   clean-sdk - Remove third_party/sdk/
#   clean     - Remove output/

BAZEL_SDK_DIR ?= $(PWD)/../../output/cpp
SDK_DST := third_party/sdk
OUTPUT := output

.PHONY: copy-sdk package clean-sdk clean

copy-sdk:
	@mkdir -p $(SDK_DST)
	@cp $(BAZEL_SDK_DIR)/lib/*.so* $(SDK_DST)/
	@chmod u+w $(SDK_DST)/*.so*
	@echo "Copied SDK libs to $(SDK_DST)/"
	@echo "  $$(ls $(SDK_DST)/*.so* | sed 's|.*/||')"

package:
	@if [ ! -f build/kvtest ]; then echo "ERROR: build/kvtest not found. Run cmake/make first."; exit 1; fi
	@mkdir -p $(OUTPUT)/lib $(OUTPUT)/config
	@cp build/kvtest $(OUTPUT)/
	@cp third_party/sdk/*.so* $(OUTPUT)/lib/
	@rm -f $(OUTPUT)/lib/libds_client_py.so*
	@cp deploy_client.py tools/procmon.py VERSION $(OUTPUT)/
	@cp tools/test_deploy.sh $(OUTPUT)/
	@cp config/config.json.example $(OUTPUT)/config/
	@cp config/deploy.json.example $(OUTPUT)/config/
	@echo "Packaged to $(OUTPUT)/:"
	@echo "  $$(ls -R $(OUTPUT) | head -20)"

clean-sdk:
	rm -rf $(SDK_DST)

clean:
	rm -rf $(OUTPUT)
