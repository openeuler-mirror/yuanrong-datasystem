# CMake script for adding googletest to CTest.
set(CTEST_FILE_CONTENTS "")

function (_COMMAND_APPEND CMD)
  set(_ARGS "")
  foreach(_ARG ${ARGN})
    if (_ARG MATCHES "[^-./:a-zA-Z0-9_]") 
      set(_ARGS "${_ARGS} [==[${_ARG}]==]")
    else()
      set(_ARGS "${_ARGS} ${_ARG}")
    endif()
  endforeach()
  set(CTEST_FILE_CONTENTS "${CTEST_FILE_CONTENTS}${CMD}(${_ARGS})\n" PARENT_SCOPE)
endfunction()

if (NOT EXISTS ${TEST_EXECUTABLE})
  message(FATAL_ERROR "Test executable not found: ${TEST_EXECUTABLE}!")
endif()

if (TEST_ENVIRONMENTS)
  string(REGEX REPLACE " " ";" TEST_ENVIRONMENTS ${TEST_ENVIRONMENTS})
endif()

# 1. Run test executable to get the test list.
set(_COMMAND_PREFIX)
if (TEST_ENVIRONMENTS)
  set(_COMMAND_PREFIX ${CMAKE_COMMAND} -E env ${TEST_ENVIRONMENTS})
endif()

execute_process(COMMAND ${_COMMAND_PREFIX} ${TEST_EXECUTABLE} --gtest_list_tests
                TIMEOUT 50
                RESULTS_VARIABLE _RESULT
                OUTPUT_VARIABLE  _OUTPUT)

if (NOT _RESULT EQUAL "0")
  message(FATAL_ERROR "Run ${TEST_EXECUTABLE} failed!\n"
                      "Command prefix: \n"
                      "  ${_COMMAND_PREFIX}\n"
                      "Return variable: \n"
                      "  ${_RESULT}\n"
                      "Output variable: \n"
                      "  ${_OUTPUT}")
endif()

# Convert to list
string(REGEX REPLACE "\n" ";" _OUTPUT ${_OUTPUT})

set(_LLT_LABEL "")
if (${TEST_EXECUTABLE} MATCHES ".*(object|kv).*")
  set(_LLT_LABEL "object")
elseif (${TEST_EXECUTABLE} MATCHES ".*stream.*")
  set(_LLT_LABEL "stream")
endif()

if (${TEST_EXECUTABLE} MATCHES ".*tests/st.*")
  set(_LLT_LABEL "${_LLT_LABEL} st")
elseif (${TEST_EXECUTABLE} MATCHES ".*tests/ut.*")
  set(_LLT_LABEL "${_LLT_LABEL} ut")
endif()

set(L0_CNT 0)
set(L1_CNT 0)
set(L2_CNT 0)

set(LEVEL_LABELS a b c d e f g h i j k l m n o p)
list(LENGTH LEVEL_LABELS LIST_LENGTH)
function(SET_LEVEL_SUFFIX NUM)
  math(EXPR RESULT "${NUM} % ${LIST_LENGTH}")
  list(GET LEVEL_LABELS ${RESULT} _LEVEL_SUFFIX)
  set(_LEVEL_SUFFIX "${_LEVEL_SUFFIX}" PARENT_SCOPE)
endfunction()


# 2. Parse the output variable and write to CTest file.
#
# Google test list testcases rule:
#   1. Test suite begin with letters and end with "."
#   2. Test name begin with 2 spaces
#   3. Test name with multi parameters end with "/${num}  # GetParam() = ..."
#   4. Test suite with multi parameters end with ".  # TypeParam() = ..."
# 
# Example:
#
# MultiGCSNodesTest.
#   TestNASWriter
#   DISABLED_TestNASTruncate
# FuseReadWriteParamTest/FuseFileSystemReadWriteTest.
#   TestOpenDir/0  # GetParam() = 4-byte object <00-00 00-00>
#   TestOpenDir/1000  # GetParam() = 4-byte object <E8-03 00-00>
#   TestReadFile/0  # GetParam() = 4-byte object <00-00 00-00>
# InodeStoreTest/0.  # TypeParam = datasystem::master::KvInodeStore
#   TestAddGetDeleteInode
#   TestAddGetDeleteChild
# InodeStoreTest/1.  # TypeParam = datasystem::master::KvHeapInodeStore
#   TestAddGetDeleteInode
#   TestAddGetDeleteChild
# ...
#
# What we need to do:
#   1. Get the test suite, remove invalid characters such as spaces and comments.
#   2. Get the test name, remove invalid characters such as spaces and comments.
#   3. Identify disabled cases amd add DISABLED property.
#   4. Generate test command such as `add_test` and `set_tests_properties`.

foreach(_TEST_LINE ${_OUTPUT})
  if (_TEST_LINE MATCHES "^  ")
    # Test name handle.
    string(REGEX REPLACE " +" "" _TEST_NAME ${_TEST_LINE})
    if (_TEST_NAME MATCHES "/[0-9]+#GetParam")
      string(REGEX REPLACE "#.*" "" _TEST_NAME ${_TEST_NAME})
    endif()

    string(REGEX REPLACE "^(DISABLED_|EXCLUSIVE_|LEVEL1_|LEVEL2_)+" "" _PRETTY_NAME "${_TEST_NAME}")

    # Add testcase to CTest, command: 
    #   add_test(NAME <name> COMMAND <command> [<arg>...] [WORKING_DIRECTORY <dir>])
    _command_append(add_test
      "${_PRETTY_SUITE}${_PRETTY_NAME}"
      ${_COMMAND_PREFIX} ${TEST_EXECUTABLE} "--gtest_filter=${_TEST_SUITE}${_TEST_NAME}" "--gtest_also_run_disabled_tests")

    if (NOT _TEST_SUITE MATCHES "^DISABLED_" AND NOT _TEST_NAME MATCHES "^DISABLED_")
      # If test suite or name contain "LEVEL1_"/"LEVEL2_", we need to set "level1"/"level2" label, otherwise, set "level0".
      if (_TEST_SUITE MATCHES "LEVEL1_" OR _TEST_NAME MATCHES "LEVEL1_")
        set(_LEVEL "1")
        math(EXPR L1_CNT "${L1_CNT} + 1")
        set(_COUNT ${L1_CNT})
        set_level_suffix(${L1_CNT})
      elseif (_TEST_SUITE MATCHES "LEVEL2_" OR _TEST_NAME MATCHES "LEVEL2_")
        set(_LEVEL "2")
        math(EXPR L2_CNT "${L2_CNT} + 1")
        set(_COUNT ${L2_CNT})
      else()
        set(_LEVEL "0")
        math(EXPR L0_CNT "${L0_CNT} + 1")
        set(_COUNT ${L0_CNT})
      endif()
      set_level_suffix(${_COUNT})
      set(_LABEL "${_LLT_LABEL} level${_LEVEL}${_LEVEL_SUFFIX}")
      _command_append(set_tests_properties
        "${_PRETTY_SUITE}${_PRETTY_NAME}"
        PROPERTIES LABELS "${_LABEL}"
      )
    endif()

    # If test suite or name begin with "DISABLED_", we need to set disable property.
    if (_TEST_SUITE MATCHES "^DISABLED_" OR _TEST_NAME MATCHES "^DISABLED_")
      _command_append(set_tests_properties
        "${_PRETTY_SUITE}${_PRETTY_NAME}"
        PROPERTIES DISABLED TRUE)
    elseif (_TEST_SUITE MATCHES "EXCLUSIVE_" OR _TEST_NAME MATCHES "EXCLUSIVE_")
      _command_append(set_tests_properties
        "${_PRETTY_SUITE}${_PRETTY_NAME}"
        PROPERTIES RUN_SERIAL TRUE)
    endif()
  else()
    # Test suite handle.
    if (_TEST_LINE MATCHES "#")
      string(REGEX REPLACE "\\. +#.*" "." _TEST_SUITE ${_TEST_LINE})
    else()
      set(_TEST_SUITE ${_TEST_LINE})
    endif()

    string(REGEX REPLACE "^(DISABLED_|EXCLUSIVE_|LEVEL1_|LEVEL2_)+" "" _PRETTY_SUITE "${_TEST_SUITE}")
  endif()
endforeach()

file(WRITE ${CTEST_FILE} "${CTEST_FILE_CONTENTS}")