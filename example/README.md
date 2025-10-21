This is an example for how to invoke the datasystem client api.

# Build the example
1. Modify the example/CMakeLists.txt line 10
```bash
set(DS_CLIENT_DIR ${DS_BUILD_OUTPUT_PATH})
```
Set the DS_BUILD_OUTPUT_PATH to the datasystem build output path.

2. Modify the example/CMakeLists.txt line 16
```bash
set(THIRD_DIR ${DEPENDENCE_DIR})
```
Set the DEPENDENCE_DIR to the log dependency.

3. Build
Build and compile the example.
```bash
# cd example
# mkdir build
# cd build
# cmake ..
# make
```
