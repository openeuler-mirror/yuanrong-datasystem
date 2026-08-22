# Makefile for kvtest
#
# Targets:
#   package  - Assemble binary + scripts + configs into output/
#   clean    - Remove output/

BAZEL_SDK_DIR ?= $(PWD)/../../output/cpp
OUTPUT := output

.PHONY: package clean

package:
	@if [ ! -f build/kvtest ]; then echo "ERROR: build/kvtest not found. Run build.sh first."; exit 1; fi
	@mkdir -p $(OUTPUT)/lib $(OUTPUT)/config
	@cp build/kvtest $(OUTPUT)/
	@cp build/coordinator_test $(OUTPUT)/ 2>/dev/null || echo "WARNING: coordinator_test not found"
	@cp build/worker_test $(OUTPUT)/ 2>/dev/null || echo "WARNING: worker_test not found"
	@if [ -d build/lib ]; then cp -fL build/lib/*.so* $(OUTPUT)/lib/ 2>/dev/null || true; fi
	@cp deploy_client.py deploy_jf.py tools/procmon.py VERSION $(OUTPUT)/
	@cp src/mock_jf_server.py $(OUTPUT)/ 2>/dev/null || true
	@cp tests/test_deploy.sh tests/test_standalone_mode.sh $(OUTPUT)/ 2>/dev/null || true
	@cp config/config.json.example $(OUTPUT)/config/
	@cp config/deploy.json.example $(OUTPUT)/config/
	@echo "Packaged to $(OUTPUT)/:"
	@echo "  $$(ls -R $(OUTPUT) | head -20)"

clean:
	rm -rf $(OUTPUT)
