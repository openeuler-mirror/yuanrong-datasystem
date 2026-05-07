# Release Package Manifests

This directory stores file-level package shape baselines for generated release artifacts.

Current baseline:

- `cmake-release-xoff-python-tar.txt`: deployment tarball paths for the CMake release build with Python packaging enabled, Java/Go disabled, and hetero disabled with `-X off`.
- `cmake-release-xoff-python-wheel.txt`: Python wheel paths for the same build profile.

Validate a generated package after `build.sh` completes:

```bash
python3 scripts/verify_package_manifest.py \
  --install-dir output \
  --tar-manifest scripts/package_manifest/cmake-release-xoff-python-tar.txt \
  --wheel-manifest scripts/package_manifest/cmake-release-xoff-python-wheel.txt
```

The verifier is read-only. It does not create, remove, rewrite, or normalize files inside the release packages.
