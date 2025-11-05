cmake_minimum_required(VERSION 3.14.1)

# When downloading and extracting files from a URL, the timestamp is handled by using the current system time as the timestamp for the extracted files.
if(POLICY CMP0135)
  cmake_policy(SET CMP0135 NEW)
endif()

# Enable _ROOT variable support (e.g., ZLIB_ROOT, etc.)
if(POLICY CMP0074)
  cmake_policy(SET CMP0074 NEW)
endif()

project(Datasystem DESCRIPTION "DataSystem for Distributed Data Cache" LANGUAGES CXX C)

# Version specified
file(STRINGS "${CMAKE_CURRENT_SOURCE_DIR}/VERSION" DATASYSTEM_VERSION)
add_compile_definitions(DATASYSTEM_VERSION="${DATASYSTEM_VERSION}")
message(STATUS "Datasystem version is ${DATASYSTEM_VERSION}")

include(CMakePackageConfigHelpers)

# Options for build.
include(cmake/options.cmake)
include(cmake/util.cmake)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Use C++17 standard.
set(CMAKE_CXX_STANDARD 17)

# compiler flags that are common across debug/release builds:
if (USE_SANITIZER)
    # use Google sanitizers to detect memory leak and thread race.
    string(TOUPPER ${USE_SANITIZER} USE_SANITIZER)
    if (${USE_SANITIZER} STREQUAL "ADDRESS")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fno-omit-frame-pointer -g3")
    elseif (${USE_SANITIZER} STREQUAL "THREAD")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread -g3")
    elseif (${USE_SANITIZER} STREQUAL "UNDEFINED")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=undefined -fno-sanitize=alignment -fsanitize=float-cast-overflow -fno-sanitize-recover=all -O0")
    else ()
        message(FATAL_ERROR "Unrecognized google sanitizers mode: ${USE_SANITIZER}")
    endif ()
endif ()
# -Wall: Enable all warnings.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall")
# -g3: Enable symbols for profiler tools.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g3")
# -Werror: Treat compile warnings as errors.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")
# -fsigned-char: Certain platforms such as ARM do not use signed chars by default.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsigned-char")
# -Wextra: Enables some extra warning.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wextra")
# -Wfloat-equal: Warn if floating-point values are used in equality comparisons
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wfloat-equal")
# -fno-common: Specifies that the compiler places uninitialized global variables in the BSS section of the object file.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-common")
# -rdynamic: Pass the flag -export-dynamic to the ELF linker, on targets that support it. 
# This option is needed to allow obtaining backtraces from within a program.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -rdynamic")

# -fPIC: Position Independent Code(PIC), is only necessary when producing shared objects.
add_definitions(-fPIC)

# debug build flags:
# -ggdb: Enable gdb debugging.
set(CMAKE_CXX_FLAGS_DEBUG "-ggdb")
# -O0: No optimize.
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0")
# -ftrapv: Generates traps for signed overflow on addition, subtraction, multiplication operations.
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -ftrapv")
# -fstack-check: Add stack check.
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fstack-check")
# -DDEBUG: Add DEBUG Marco.
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DDEBUG")

# release build flags:
# -O2: Turns on all optimization flags.
set(CMAKE_CXX_FLAGS_RELEASE "-O2")
# -fstack-protector-strong: Stack protect.
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -fstack-protector-strong")
# -Wl,-z,relro: Read-Only Relocations (RELRO).
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -Wl,-z,relro")
# -Wl,-z,now: Bind now.
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -Wl,-z,now")
# -Wl,-z,noexecstack: Non-Executable Memory.
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -Wl,-z,noexecstack")
# -D_FORTIFY_SOURCE=2: Add checks at compile-time and run-time.
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -D_FORTIFY_SOURCE=2")
# -fPIE -pie: Support address space layout randomization (ASLR).
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -fPIE -pie")
# -DNDEBUG: Turn off dchecks/asserts/debug only code.
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -DNDEBUG")
# -Wl,--build-id=none: for build consistency.
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -Wl,--build-id=none")

set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE "${CMAKE_COMMAND} -E time")
set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK "${CMAKE_COMMAND} -E time")

find_program(CCACHE "ccache")
if (CCACHE)
    message(STATUS "Use ccache to accelerate compilation")
    set(CMAKE_C_COMPILER_LAUNCHER ${CCACHE})
    set(CMAKE_CXX_COMPILER_LAUNCHER ${CCACHE})
endif ()

if (BUILD_COVERAGE)
    message(STATUS "Build code with coverage mode.")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fprofile-arcs -ftest-coverage")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fprofile-arcs -ftest-coverage -lgcov")
    add_compile_definitions(BUILD_COVERAGE)
endif ()

if (ENABLE_PERF)
    add_compile_definitions(ENABLE_PERF)
    message(STATUS "Enable perf point log")
endif ()

if (BUILD_HETERO)
    add_compile_definitions(BUILD_HETERO)
    message(STATUS "Enable hetero")
endif ()

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake/module")

set(GIT_HASH "Unknown")
set(GIT_BRANCH "Unknown")
get_git_hash(GIT_HASH)
get_git_branch(GIT_BRANCH)
message(STATUS "GIT COMMIT: ${GIT_HASH}; BRANCH: ${GIT_BRANCH}")
add_compile_options(-DGIT_HASH="${GIT_HASH}")
add_compile_options(-DGIT_BRANCH="${GIT_BRANCH}")

# Import the third party we depends.
include(cmake/dependency.cmake)

include_directories(include)
include_directories(src)

# include proto pb files
include_directories(${CMAKE_BINARY_DIR}/src)

if (WITH_TESTS)
    add_compile_definitions(WITH_TESTS)
endif ()

add_subdirectory(src/datasystem)

if (WITH_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif ()

include(cmake/package.cmake)

file(GLOB_RECURSE PROJECT_SOURCES "src/*.cpp" "include/*.h" "src/*.h")
foreach(SOURCE_FILE ${PROJECT_SOURCES})
    file(READ ${SOURCE_FILE} FILE_CONTENTS)
    string(FIND "${FILE_CONTENTS}" "#include <regex>" FOUND_REGEX)
    if (NOT ${FOUND_REGEX} EQUAL -1)
        message(FATAL_ERROR "${SOURCE_FILE} include <regex>, std::regex is not avaliale, use re2 to replace")
    endif()
endforeach()
