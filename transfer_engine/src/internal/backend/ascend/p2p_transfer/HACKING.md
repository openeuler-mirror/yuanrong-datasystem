# Hacking

## Developer mode

### Configure, build and test

```sh
cmake -S . -B build -D p2p-transfer_DEVELOPER_MODE=ON
#cmake -S . -B build -D CMAKE_BUILD_TYPE=Debug -D p2p-transfer_DEVELOPER_MODE=ON
cmake --build build
./build/test/p2p-transfer_test_batch
```

### Developer mode targets

These are targets you may invoke using the build command from above, with an
additional `-t <target>` flag:

#### `format-check` and `format-fix`

These targets run the clang-format tool on the codebase to check errors and to
fix them respectively. Customization available using the `FORMAT_PATTERNS` and
`FORMAT_COMMAND` cache variables.

#### `spell-check` and `spell-fix`

These targets run the codespell tool on the codebase to check errors and to fix
them respectively. Customization available using the `SPELL_COMMAND` cache
variable.