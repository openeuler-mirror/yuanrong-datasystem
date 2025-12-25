file(GLOB BENCH_SRC
    src/main.cpp
    src/args_base.cpp
    src/bench_base.cpp
    src/utils.cpp
    src/bench_perf.cpp
    src/kv/kv_args.cpp
    src/kv/kv_bench.cpp
)

add_executable(dsbench_cpp ${BENCH_SRC})
target_include_directories(dsbench_cpp PRIVATE src)

target_link_libraries(dsbench_cpp datasystem pthread common_util)
clean_build_rpath(dsbench_cpp)
