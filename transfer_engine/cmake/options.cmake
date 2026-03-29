option(TRANSFER_ENGINE_ENABLE_P2P_THIRD_PARTY "Enable P2P-Transfer backend" ON)
option(TRANSFER_ENGINE_BUILD_TESTS "Build transfer_engine tests" ON)
option(TRANSFER_ENGINE_BUILD_BUNDLED_P2P_SO "Build bundled libp2p_transfer.so with transfer_engine" ON)
option(TRANSFER_ENGINE_BUILD_BUNDLED_PROTOBUF "Build bundled protobuf from source" ON)
option(TRANSFER_ENGINE_BUILD_PYTHON "Build transfer_engine python bindings" OFF)

set(TRANSFER_ENGINE_PYTHON_OUTPUT_DIR "" CACHE PATH "Output directory of python extension module")
