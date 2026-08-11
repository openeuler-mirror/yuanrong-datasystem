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
	@mkdir -p $(OUTPUT)/config
	@cp build/kvtest $(OUTPUT)/
# Rewrite RPATH to $ORIGIN/lib so the packaged binary can find .so in ./lib/
# if present. In container SDK mode, LD_LIBRARY_PATH (set by deploy_client.py
# to the container SDK path) takes precedence over RPATH. patchelf is
# optional — if unavailable, the binary still runs via LD_LIBRARY_PATH.
	@if command -v patchelf >/dev/null 2>&1; then \
		patchelf --set-rpath '$$ORIGIN/lib' $(OUTPUT)/kvtest; \
		echo "Rewrote kvtest RPATH to \$$ORIGIN/lib (via patchelf)"; \
	else \
		echo "  (patchelf not found — RPATH left as build-time paths; LD_LIBRARY_PATH will be used at runtime)"; \
	fi
	@cp deploy_client.py tools/procmon.py VERSION $(OUTPUT)/
	@cp tests/test_deploy.sh $(OUTPUT)/
	@cp config/config.json.example $(OUTPUT)/config/
	@cp config/deploy.json.example $(OUTPUT)/config/
	@echo "Packaged to $(OUTPUT)/:"
	@echo "  $$(ls -R $(OUTPUT) | head -20)"

clean:
	rm -rf $(OUTPUT)
