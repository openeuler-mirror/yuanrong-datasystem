include(FetchContent)

set(DOWNLOAD_EXTRACT_TIMESTAMP TRUE)

set(THIRDPARTY_SAFE_FLAGS "-fPIC -D_FORTIFY_SOURCE=2 -O2 -fstack-protector-strong -ffunction-sections -fdata-sections -Wl,--gc-sections -Wl,--build-id=none -Wl,-z,relro,-z,noexecstack,-z,now ${EXT_FLAGS}")

if (CMAKE_HOST_SYSTEM_PROCESSOR STREQUAL "aarch64" AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER "9.1.0")
    set(EXT_FLAGS "-mtune=tsv110 -march=armv8-a")
    message("Add compiler flags ${EXT_FLAGS} for aarch64")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${EXT_FLAGS}")
endif()

list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake/modules)

# We provide a way to cache the third party libs to avoid the repeated third-party compilation.
# User can configure the cache by setting the environment variables or CMake configuration, and
# the priority of the CMake configuration is higher.
if(NOT DS_OPENSOURCE_DIR)
  if(DEFINED ENV{DS_OPENSOURCE_DIR})
    set(DS_OPENSOURCE_DIR $ENV{DS_OPENSOURCE_DIR})
  else()
    string(SHA256 _TEMP_PATH ${CMAKE_BINARY_DIR})
    set(DS_OPENSOURCE_DIR "/tmp/${_TEMP_PATH}")
  endif()
endif()
get_filename_component(DS_OPENSOURCE_DIR ${DS_OPENSOURCE_DIR} ABSOLUTE)

if (NOT BUILD_THREAD_NUM)
  set(BUILD_THREAD_NUM 8)
endif()

message(STATUS "Cache the third party libs to ${DS_OPENSOURCE_DIR}, "
               "build them with ${BUILD_THREAD_NUM} parallelism.")

find_program(Patch_EXECUTABLE patch)
set(Patch_FOUND ${Patch_EXECUTABLE})
find_program(Meson_EXECUTABLE meson)
set(Meson_FOUND ${Meson_EXECUTABLE})
find_program(Ninja_EXECUTABLE ninja)
set(Ninja_FOUND ${Ninja_EXECUTABLE})

function(__EXEC_COMMAND)
  set(options)
  set(one_value_args WORKING_DIRECTORY)
  set(multi_value_args COMMAND)
  cmake_parse_arguments(ARG "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})

  execute_process(COMMAND ${ARG_COMMAND}
      WORKING_DIRECTORY ${ARG_WORKING_DIRECTORY}
      RESULT_VARIABLE _RET)
    if(NOT _RET EQUAL "0")
      message(FATAL_ERROR "Fail execute command: ${ARG_COMMAND}, error: ${_RET}")
    endif()
endfunction()

function(DOWNLOAD_LIB_PKG LIB_NAME URL SHA256)
  # OpenEuler tiny package url end with "rpm" suffix, we need
  # to uncompress it and get the real source code package.
  if (URL MATCHES ".*\.src\.rpm$")
    FetchContent_Declare(
      "${LIB_NAME}_rpm"
      URL ${URL}
      URL_HASH SHA256=${SHA256}
    )
    FetchContent_GetProperties("${LIB_NAME}_rpm")
    FetchContent_Populate("${LIB_NAME}_rpm")

    # TODO: need to consider the end suffix with zip, tar and so on.
    file(GLOB _URL_LIST "${${LIB_NAME}_rpm_SOURCE_DIR}/${LIB_NAME}*\.tar\.gz" "${${LIB_NAME}_rpm_SOURCE_DIR}/${LIB_NAME}*\.tar\.xz")
    if (NOT _URL_LIST)
      message(FATAL_ERROR "Failed to find source package from ${${LIB_NAME}_rpm_SOURCE_DIR}")
    endif()
    list(GET _URL_LIST 0 URL)
    list(LENGTH _URL_LIST _URL_LIST_LEN)
    if (_URL_LIST_LEN GREATER 1)
      message(WARNING "Get source package is more than 1, but we only choose the first one: ${URL}")
    endif()

    file(SHA256 "${URL}" SHA256)
  endif()

  FetchContent_Declare(
    ${LIB_NAME}
    URL ${URL}
    URL_HASH SHA256=${SHA256}
  )
  FetchContent_GetProperties(${LIB_NAME})
  message(STATUS "Download ${LIB_NAME} from ${URL}")
  if(NOT ${LIB_NAME}_POPULATED)
    FetchContent_Populate(${LIB_NAME})
    set(${LIB_NAME}_SOURCE_DIR ${${LIB_NAME}_SOURCE_DIR} PARENT_SCOPE)
    set(${LIB_NAME}_BINARY_DIR ${${LIB_NAME}_BINARY_DIR} PARENT_SCOPE)
  endif()
endfunction()

# Generate fake third party tar package, it is all about trustworthiness.
#
# Arguments:
#   NAME <pkg-name>
#       Specify the package name of the third-party library.
#
#   URL <url-variable>
#       Specify an output variable, path of the tar package is assigned to this variable.
#
#   SHA256 <sha256-sum>
#       Specify an output variable, SHA256 sum code of the tar package is assigned to this variable.
#
#   FAKE_SHA256 <sha256-sum>
#       A fake sha256, useful when building third party components from source code.
#
#   VERSION <package-version>
#       Specify an output variable, the version of the package is assigned to this variable if version.txt exists.
#       The version.txt is provided by the user/CI to specify the source code's version.
#       The version.txt contains the version of package that must be equal to the version value provided by <PackageName>ConfigVersion.cmake,
#       otherwise cmake find_package may fail
#       The location of version.txt is ${DS_PACKAGE}/${PACKAGFE_NAME}/version.txt, like /usr1/third_party/zlib/version.txt
function(GEN_THIRDPARTY_PKG NAME URL SHA256 FAKE_SHA256 VERSION)
  get_filename_component(_THIRDPARTY_DIR "$ENV{DS_PACKAGE}" ABSOLUTE)
  set(_DIR "${_THIRDPARTY_DIR}/${NAME}")

  set(VERSION_TXT ${_DIR}/version.txt)
  if (EXISTS ${VERSION_TXT})
    file(READ ${VERSION_TXT} _VERSION)
    string(STRIP ${_VERSION} _VERSION)
  endif()

  if ("${_VERSION}" STREQUAL "")
    MESSAGE("The ${NAME} directory don't contain version.txt or it's empty")
  else()
    MESSAGE("Found thirdparty library ${NAME} version is ${_VERSION} in version.txt")
  endif()
  if (NOT EXISTS "${_DIR}" OR NOT IS_DIRECTORY "${_DIR}")
    message(FATAL_ERROR "Specify path: ${_DIR} not exist or is not a directory!")
  endif()

  set(_SUFFIX_LIST ".tar.gz" ".tar.xz" ".tar.bz2" ".zip")
  if (${NAME} STREQUAL "re2" OR ${NAME} STREQUAL "absl")
    list(TRANSFORM _SUFFIX_LIST PREPEND ${_DIR}/* OUTPUT_VARIABLE _DIR_SUFFIX_LIST)
  else()
    list(TRANSFORM _SUFFIX_LIST PREPEND ${_DIR}/${NAME}* OUTPUT_VARIABLE _DIR_SUFFIX_LIST)
  endif()
  file(GLOB _TAR_PKG_FILE ${_DIR_SUFFIX_LIST})

  if (_TAR_PKG_FILE)
    # OpenEuler would save the tar file in it's directory, unthinkable operation.
    list(GET _TAR_PKG_FILE 0 _DEST_PATH)
    list(LENGTH _TAR_PKG_FILE _TAR_PKG_LEN)
    if (_TAR_PKG_LEN GREATER 1)
      message(WARNING "Get tar file is more than 1, but we only choose the first one: ${_DEST_PATH}")
    endif()
    file(SHA256 "${_DEST_PATH}" _SHA256)
  else()
    # Step1: Generate sha256 based on source code.
    execute_process(COMMAND sh -c "find ${_DIR} -path ${_DIR}/.git -prune -o -type f -print0 | sort -z | xargs -0 cat | sha256sum"
                    OUTPUT_VARIABLE _FINAL_SHA256_VALUE
                    RESULT_VARIABLE _RET)
    if(NOT _RET EQUAL "0")
      message(FATAL_ERROR "Fail to find files in source code, error: ${_RET}")
    endif()
    string(REGEX REPLACE "\ .*$" "" _FINAL_SHA256_VALUE "${_FINAL_SHA256_VALUE}")
    # Step2: Generate fake *tar.gz
    find_program(_TAR_EXECUTABLE tar)
    if (NOT _TAR_EXECUTABLE)
      message(FATAL_ERROR "tar command not found!")
    endif()

    set(_DEST_PATH "${CMAKE_CURRENT_BINARY_DIR}/${NAME}.tar.gz")
    if (EXISTS "${_DEST_PATH}")
      file(REMOVE "${_DEST_PATH}")
    endif()
    __exec_command(COMMAND ${_TAR_EXECUTABLE} -zmcf "${_DEST_PATH}" -C "${_THIRDPARTY_DIR}" "${NAME}"
                   WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
    file(SHA256 "${_DEST_PATH}" _SHA256)
  endif()

  # Set output variables.
  set(${URL} "${_DEST_PATH}" PARENT_SCOPE)
  set(${SHA256} "${_SHA256}" PARENT_SCOPE)
  set(${FAKE_SHA256} "${_FINAL_SHA256_VALUE}" PARENT_SCOPE)
  if (NOT "${_VERSION}" STREQUAL "")
    set(${VERSION} "${_VERSION}" PARENT_SCOPE)
  endif()
endfunction()

# Add a third-party dependency library on which the Datasystem Depends.
#
# LIB_NAME is the name of the library.
#
# Additional optional arguments:
#
#   URL <download-url>
#       Specify download url, it can be a link or file path.
#
#   SHA256 <sha256-sum>
#       Specify package provided by URL sha256 sum for check purpose.
#
#   FAKE_SHA256 <sha256-sum>
#       A fake sha256, useful when building third party components from source code.
#
#   VERSION <version>
#       Specify library version.
#
#   TOOLCHAIN <toolchain>
#       Specify compile toolchain, support cmake, configure and so on.
#
#   CONF_PATH <configure-path>
#       Specify configure file path, it can be useful if the configure
#       file is not in root dir.
#
#   COMPONENTS <component1> <component2> ...
#       Specify the components we need from third-party library, if we
#       don't need all components, we can just specify the components
#       what we need by this argument.
#
#   CONF_OPTIONS <option1> <option2> ...
#       Specify the configure options, the value depends on which
#       toolchain we use.
#
#   PRE_CONFIGURE <cmd1> <cmd2> ...
#       Specify the pre-configure command before execute the configure,
#       e.g. sh autogen.sh .
#
#   PATHCES <patch1> <patch2> ...
#       Specify the patch files path, they would be apply before compile.
#
#   CXX_FLAGS <flag1> <flag2> ...
#       Specify the CXX compile flags.
#
#   C_FLAGS <flag1> <flag2> ...
#       Specify the C compile flags.
#
#   LINK_FLAGS <flag1> <flag2> ...
#       Specify the link flags.
#
#   EXTRA_MSGS <msg1> <msg2> ...
#       Specify the extra messages, it is helpful when third-party lib also
#       have dependent libraries. If dependent libraries changed, the lib
#       would be force to update.
function(ADD_THIRDPARTY_LIB LIB_NAME)
  set(options)
  set(one_value_args URL SHA256 FAKE_SHA256 VERSION TOOLCHAIN CONF_PATH)
  set(multi_value_args COMPONENTS CONF_OPTIONS PRE_CONFIGURE PATCHES CXX_FLAGS C_FLAGS LINK_FLAGS EXTRA_MSGS)
  cmake_parse_arguments(ARG "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})

  string(TOLOWER ${LIB_NAME} _LIB_NAME_LOWER)

  if(NOT ARG_TOOLCHAIN)
    set(ARG_TOOLCHAIN "cmake")
  endif()

  # Generate a unique install dir name, the impact factors are as follow:
  # Lib name:
  set(${LIB_NAME}_CONF_TXT "${_LIB_NAME_LOWER}")
  if(NOT ${ARG_FAKE_SHA256} STREQUAL "")
    set(${LIB_NAME}_CONF_TXT "${${LIB_NAME}_CONF_TXT}_${ARG_FAKE_SHA256}")
  else()
    set(${LIB_NAME}_CONF_TXT "${${LIB_NAME}_CONF_TXT}_${ARG_SHA256}")
  endif()
  # Version:
  set(${LIB_NAME}_CONF_TXT "${${LIB_NAME}_CONF_TXT}_${ARG_VERSION}")
  # Components:
  set(${LIB_NAME}_CONF_TXT "${${LIB_NAME}_CONF_TXT}_${ARG_COMPONENTS}")
  # Toolchain:
  set(${LIB_NAME}_CONF_TXT "${${LIB_NAME}_CONF_TXT}_${ARG_TOOLCHAIN}")
  # Configure options:
  set(${LIB_NAME}_CONF_TXT "${${LIB_NAME}_CONF_TXT}_${ARG_CONF_OPTIONS}")
  # CXX compiler version:
  set(${LIB_NAME}_CONF_TXT "${${LIB_NAME}_CONF_TXT}_${CMAKE_CXX_COMPILER_VERSION}")
  # C compiler version:
  set(${LIB_NAME}_CONF_TXT "${${LIB_NAME}_CONF_TXT}_${CMAKE_C_COMPILER_VERSION}")
  # CXX_FLAGS:
  set(${LIB_NAME}_CONF_TXT "${${LIB_NAME}_CONF_TXT}_${ARG_CXX_FLAGS}")
  # C_FLAGS:
  set(${LIB_NAME}_CONF_TXT "${${LIB_NAME}_CONF_TXT}_${ARG_C_FLAGS}")
  # LINK_FLAGS:
  set(${LIB_NAME}_CONF_TXT "${${LIB_NAME}_CONF_TXT}_${ARG_LINK_FLAGS}")
  # Patch files:
  foreach(_PATCH ${ARG_PATCHES})
    file(SHA256 ${_PATCH} _PATCH_SHA256)
    set(${LIB_NAME}_CONF_TXT "${${LIB_NAME}_CONF_TXT}}_${_PATCH_SHA256}")
  endforeach()
  # Extra messages:
  foreach(_MSG ${ARG_EXTRA_MSGS})
    set(${LIB_NAME}_CONF_TXT "${${LIB_NAME}_CONF_TXT}}_${_MSG}")
  endforeach()
  string(REPLACE ";" "_" ${LIB_NAME}_CONF_TXT ${${LIB_NAME}_CONF_TXT})
  string(SHA256 _ROOT_SUFFIX ${${LIB_NAME}_CONF_TXT})
  set(${LIB_NAME}_ROOT "${DS_OPENSOURCE_DIR}/${_LIB_NAME_LOWER}_${_ROOT_SUFFIX}")

  # Check if we have cache the lib, if true, reuse it directly.
  set(_VERIFY_FILE "${${LIB_NAME}_ROOT}/${LIB_NAME}_install.txt")
  if(EXISTS ${${LIB_NAME}_ROOT})
    if (EXISTS ${_VERIFY_FILE})
      set(${LIB_NAME}_FOUND TRUE)
    endif()

    if(${LIB_NAME}_FOUND)
      message(STATUS "${LIB_NAME} found in ${${LIB_NAME}_ROOT}...")
      if (EXISTS ${${LIB_NAME}_ROOT}/lib64)
        set(${LIB_NAME}_LIB_PATH ${${LIB_NAME}_ROOT}/lib64)
      else()
        set(${LIB_NAME}_LIB_PATH ${${LIB_NAME}_ROOT}/lib)
      endif()
      set(${LIB_NAME}_LIB_PATH ${${LIB_NAME}_LIB_PATH} PARENT_SCOPE)
      set(${LIB_NAME}_ROOT "${${LIB_NAME}_ROOT}" PARENT_SCOPE)
      if (DEFINED EXPORT_TO_USER_ENV_FILE)
        file(APPEND "${EXPORT_TO_USER_ENV_FILE}"
          "set(${LIB_NAME}_ROOT ${${LIB_NAME}_ROOT})" "\n"
          "set(${LIB_NAME}_LIB_PATH ${${LIB_NAME}_LIB_PATH})" "\n"
          )
      endif()
      return()
    else()
      message(STATUS "${LIB_NAME} not found in ${${LIB_NAME}_ROOT}, need recompile...")
      # Well, although the cache directory exists, it appears to be corrupted (because we can't find
      # it via find_package). So remove the directory directly and we will recompile the lib.
      file(REMOVE_RECURSE "${${LIB_NAME}_ROOT}")
    endif()
  endif()

  # Fetch the package first.
  download_lib_pkg(${_LIB_NAME_LOWER} ${ARG_URL} ${ARG_SHA256})

  # Apply the patches if need.
  foreach(_PATCH ${ARG_PATCHES})
    if (NOT Patch_FOUND)
      message(FATAL_ERROR "patch executable not found!")
    endif()
    execute_process(COMMAND ${Patch_EXECUTABLE} --verbose -p1 INPUT_FILE ${_PATCH}
                    WORKING_DIRECTORY ${${_LIB_NAME_LOWER}_SOURCE_DIR}
                    RESULT_VARIABLE _RET)
    if(NOT _RET EQUAL "0")
      message("Patch ${_PATCH} failed, error: ${_RET}")
    endif()
  endforeach()

  # Compile the source code and install.
  message(STATUS "Compiling ${LIB_NAME} in ${${_LIB_NAME_LOWER}_BINARY_DIR}")
  string(TOLOWER ${ARG_TOOLCHAIN} _TOOLCHAIN_LOWER)

  if(${_LIB_NAME_LOWER} STREQUAL "tbb" AND ${_TOOLCHAIN_LOWER} STREQUAL "make")
    find_program(MAKE_PROGRAM make)  # CMAKE_MAKE_PROGRAM may not be make.
    if (NOT MAKE_PROGRAM)
      message(FATAL_ERROR "make program not found! Please install make.")
    endif()
    if (ARG_CXX_FLAGS)
      list(APPEND ${LIB_NAME}_MAKE_CXXFLAGS "CXXFLAGS=${ARG_CXX_FLAGS}")
    endif()
    if (ARG_C_FLAGS)
      list(APPEND ${LIB_NAME}_MAKE_CFLAGS "CFLAGS=${ARG_C_FLAGS}")
    endif()
    if (ARG_LINK_FLAGS)
      list(APPEND ${LIB_NAME}_MAKE_LDFLAGS "LDFLAGS=${ARG_LINK_FLAGS}")
    endif()
    __exec_command(COMMAND ${MAKE_PROGRAM}
                          ${${LIB_NAME}_MAKE_CFLAGS}
                          ${${LIB_NAME}_MAKE_CXXFLAGS}
                          ${${LIB_NAME}_MAKE_LDFLAGS}
                          -j ${BUILD_THREAD_NUM}
                   WORKING_DIRECTORY ${${_LIB_NAME_LOWER}_SOURCE_DIR})
    # Copy headers
    file(COPY ${${_LIB_NAME_LOWER}_SOURCE_DIR}/include/tbb
         DESTINATION ${${LIB_NAME}_ROOT}/include)
    # Copy libs
    file(GLOB_RECURSE _LIBS_LIST "${${_LIB_NAME_LOWER}_SOURCE_DIR}/*lib*.so*")
    foreach(_LIB ${_LIBS_LIST})
      file(COPY "${_LIB}" DESTINATION ${${LIB_NAME}_ROOT}/lib)
    endforeach()
  elseif(${_LIB_NAME_LOWER} STREQUAL "ub")
    # libub could not install directly, copy its header files and libs by ourselves.
    if (NOT EXISTS ${${LIB_NAME}_ROOT})
      file(MAKE_DIRECTORY ${${LIB_NAME}_ROOT})
    endif()
    # extract files from rpm
    file(GLOB RPM_FILES "${${_LIB_NAME_LOWER}_SOURCE_DIR}/umdk-*.rpm")
    foreach(file ${RPM_FILES})
      message("process ${file}")
      execute_process(COMMAND rpm2cpio ${file}
                      COMMAND cpio -idmv
                      WORKING_DIRECTORY ${${_LIB_NAME_LOWER}_SOURCE_DIR}
                      OUTPUT_QUIET
                      RESULT_VARIABLE _RET)
      if(NOT _RET EQUAL "0")
        message(FATAL_ERROR "Fail to extract files from rpm, error: ${_RET}")
      endif()
    endforeach()
    # Copy headers
    if (URMA_OVER_UB)
      file(COPY ${${_LIB_NAME_LOWER}_SOURCE_DIR}/usr/include/ub/umdk/urma/urma_opcode.h
                ${${_LIB_NAME_LOWER}_SOURCE_DIR}/usr/include/ub/umdk/urma/urma_types.h
                ${${_LIB_NAME_LOWER}_SOURCE_DIR}/usr/include/ub/umdk/urma/urma_api.h
                ${${_LIB_NAME_LOWER}_SOURCE_DIR}/usr/include/ub/umdk/urma/urma_ubagg.h
          DESTINATION ${${LIB_NAME}_ROOT}/include)
    else()
      file(COPY ${${_LIB_NAME_LOWER}_SOURCE_DIR}/usr/include/umdk/urma_opcode.h
                ${${_LIB_NAME_LOWER}_SOURCE_DIR}/usr/include/umdk/urma_types.h
                ${${_LIB_NAME_LOWER}_SOURCE_DIR}/usr/include/umdk/urma_api.h
          DESTINATION ${${LIB_NAME}_ROOT}/include)
    endif()
    # Copy libs
    file(COPY ${${_LIB_NAME_LOWER}_SOURCE_DIR}/usr/lib64/liburma.so
              ${${_LIB_NAME_LOWER}_SOURCE_DIR}/usr/lib64/liburma.so.0
              ${${_LIB_NAME_LOWER}_SOURCE_DIR}/usr/lib64/liburma.so.0.0.1
              ${${_LIB_NAME_LOWER}_SOURCE_DIR}/usr/lib64/liburma_common.so
              ${${_LIB_NAME_LOWER}_SOURCE_DIR}/usr/lib64/liburma_common.so.0
              ${${_LIB_NAME_LOWER}_SOURCE_DIR}/usr/lib64/liburma_common.so.0.0.1
         DESTINATION ${${LIB_NAME}_ROOT}/lib64)
    # copy the ib/ip/ub libs to /urma if applicable
    if (EXISTS ${${_LIB_NAME_LOWER}_SOURCE_DIR}/usr/lib64/urma/liburma_ib.so)
      file(COPY ${${_LIB_NAME_LOWER}_SOURCE_DIR}/usr/lib64/urma/liburma_ib.so
          ${${_LIB_NAME_LOWER}_SOURCE_DIR}/usr/lib64/urma/liburma_ib.so.0
          ${${_LIB_NAME_LOWER}_SOURCE_DIR}/usr/lib64/urma/liburma_ib.so.0.0.1
        DESTINATION ${${LIB_NAME}_ROOT}/lib64/urma)
    endif()
    if (EXISTS ${${_LIB_NAME_LOWER}_SOURCE_DIR}/usr/lib64/urma/liburma_ip.so)
      file(COPY ${${_LIB_NAME_LOWER}_SOURCE_DIR}/usr/lib64/urma/liburma_ip.so
          ${${_LIB_NAME_LOWER}_SOURCE_DIR}/usr/lib64/urma/liburma_ip.so.0
          ${${_LIB_NAME_LOWER}_SOURCE_DIR}/usr/lib64/urma/liburma_ip.so.0.0.1
        DESTINATION ${${LIB_NAME}_ROOT}/lib64/urma)
    endif()
    if (EXISTS ${${_LIB_NAME_LOWER}_SOURCE_DIR}/usr/lib64/urma/liburma_ubagg.so)
      file(COPY ${${_LIB_NAME_LOWER}_SOURCE_DIR}/usr/lib64/urma/liburma_ubagg.so
          ${${_LIB_NAME_LOWER}_SOURCE_DIR}/usr/lib64/urma/liburma_ubagg.so.0
          ${${_LIB_NAME_LOWER}_SOURCE_DIR}/usr/lib64/urma/liburma_ubagg.so.0.0.1
        DESTINATION ${${LIB_NAME}_ROOT}/lib64/urma)
    endif()
    # copy libs and bins for URPC
    if (BUILD_WITH_URPC)
      file(COPY ${${_LIB_NAME_LOWER}_SOURCE_DIR}/usr/include/umdk/flowbuf/cpp/flowbuffer.h
                ${${_LIB_NAME_LOWER}_SOURCE_DIR}/usr/include/umdk/urpc/cpp
                ${${_LIB_NAME_LOWER}_SOURCE_DIR}/usr/include/umdk/urpc/urpc.h
            DESTINATION ${${LIB_NAME}_ROOT}/include)
      file(COPY ${${_LIB_NAME_LOWER}_SOURCE_DIR}/usr/lib64/libflowbuffer.so
                ${${_LIB_NAME_LOWER}_SOURCE_DIR}/usr/lib64/liburpc.so
                ${${_LIB_NAME_LOWER}_SOURCE_DIR}/usr/lib64/liburpc_cpp.so
          DESTINATION ${${LIB_NAME}_ROOT}/lib64)
      file(COPY ${${_LIB_NAME_LOWER}_SOURCE_DIR}/usr/bin/flowc
          DESTINATION ${${LIB_NAME}_ROOT}/bin)
    endif()
  elseif(${_TOOLCHAIN_LOWER} STREQUAL "cmake")
    if (ARG_CXX_FLAGS)
      list(APPEND ARG_CONF_OPTIONS "-DCMAKE_CXX_FLAGS=${ARG_CXX_FLAGS}")
    endif()
    if (ARG_C_FLAGS)
      list(APPEND ARG_CONF_OPTIONS "-DCMAKE_C_FLAGS=${ARG_C_FLAGS}")
    endif()
    if (ARG_LINK_FLAGS)
      list(APPEND ARG_CONF_OPTIONS "-DCMAKE_SHARED_LINKER_FLAGS=${ARG_LINK_FLAGS}")
    endif()
    list(APPEND ARG_CONF_OPTIONS "-DCMAKE_INSTALL_PREFIX=${${LIB_NAME}_ROOT}")

    __exec_command(COMMAND ${CMAKE_COMMAND} -G ${CMAKE_GENERATOR}
                            ${ARG_CONF_OPTIONS}
                            ${${_LIB_NAME_LOWER}_SOURCE_DIR}/${ARG_CONF_PATH}
                   WORKING_DIRECTORY ${${_LIB_NAME_LOWER}_BINARY_DIR})

    __exec_command(COMMAND ${CMAKE_COMMAND} --build . --target install -- -j${BUILD_THREAD_NUM}
                   WORKING_DIRECTORY ${${_LIB_NAME_LOWER}_BINARY_DIR})

  elseif(${_TOOLCHAIN_LOWER} STREQUAL "configure")
    # If we need to do something before run ./configure, just do it.
    if (ARG_PRE_CONFIGURE)
      __exec_command(COMMAND ${ARG_PRE_CONFIGURE}
                     WORKING_DIRECTORY ${${_LIB_NAME_LOWER}_SOURCE_DIR})
    endif()

    # Add compile flags, install prefix and run configure.
    if (ARG_CXX_FLAGS)
      list(APPEND ARG_CONF_OPTIONS "CXXFLAGS=${ARG_CXX_FLAGS}")
    endif()
    if (ARG_C_FLAGS)
      list(APPEND ARG_CONF_OPTIONS "CFLAGS=${ARG_C_FLAGS}")
    endif()
    if (ARG_LINK_FLAGS)
      list(APPEND ARG_CONF_OPTIONS "LDFLAGS=${ARG_LINK_FLAGS}")
    endif()
    list(APPEND ARG_CONF_OPTIONS "--prefix=${${LIB_NAME}_ROOT}")

    if (EXISTS ${${_LIB_NAME_LOWER}_SOURCE_DIR}/config AND NOT IS_DIRECTORY ${${_LIB_NAME_LOWER}_SOURCE_DIR}/config)
      set(_CONFIG_FILE ${${_LIB_NAME_LOWER}_SOURCE_DIR}/config)
    else()
      set(_CONFIG_FILE ${${_LIB_NAME_LOWER}_SOURCE_DIR}/configure)
    endif()

    __exec_command(COMMAND sh ${_CONFIG_FILE} ${ARG_CONF_OPTIONS}
                   WORKING_DIRECTORY ${${_LIB_NAME_LOWER}_SOURCE_DIR})

    # make -j && make install
    if (NOT CMAKE_MAKE_PROGRAM)
      message(FATAL_ERROR "make program not found!")
    endif()
    __exec_command(COMMAND ${CMAKE_MAKE_PROGRAM}
                           ${${LIB_NAME}_MAKE_CFLAGS}
                           ${${LIB_NAME}_MAKE_CXXFLAGS}
                           ${${LIB_NAME}_MAKE_LDFLAGS}
                           -j ${BUILD_THREAD_NUM}
                   WORKING_DIRECTORY ${${_LIB_NAME_LOWER}_SOURCE_DIR})
    __exec_command(COMMAND ${CMAKE_MAKE_PROGRAM} install
                   WORKING_DIRECTORY ${${_LIB_NAME_LOWER}_SOURCE_DIR})
  else()
    message(FATAL_ERROR "Unrecognized toolchain: ${ARG_TOOLCHAIN}")
  endif()

  # Write install text to root dir for verify purpose.
  file(WRITE "${_VERIFY_FILE}" "${${LIB_NAME}_CONF_TXT}")

  # For output package variables.
  if (EXISTS ${${LIB_NAME}_ROOT}/lib64)
    set(${LIB_NAME}_LIB_PATH ${${LIB_NAME}_ROOT}/lib64)
  else()
    set(${LIB_NAME}_LIB_PATH ${${LIB_NAME}_ROOT}/lib)
  endif()
  set(${LIB_NAME}_LIB_PATH ${${LIB_NAME}_LIB_PATH} PARENT_SCOPE)
  set(${LIB_NAME}_ROOT ${${LIB_NAME}_ROOT} PARENT_SCOPE)

  if (DEFINED EXPORT_TO_USER_ENV_FILE)
    file(APPEND "${EXPORT_TO_USER_ENV_FILE}"
      "set(${LIB_NAME}_ROOT ${${LIB_NAME}_ROOT})" "\n"
      "set(${LIB_NAME}_LIB_PATH ${${LIB_NAME}_LIB_PATH})" "\n"
      )
  endif()
endfunction()

# Adjuice third-party dependency library version.
#
# LIB_NAME is the name of the library.
#
# Output variables:
#   ${LIB_NAME}_VERSION
#       third-party library version.
#
#   ${LIB_NAME}_URL
#       third-party library download url.
#
#   ${LIB_NAME}_SHA256
#       third-party library SHA256 for verify.
function(ADJUICE_THIRDPARTY_VERSION LIB_NAME)
  if ("$ENV{DS_PACKAGE}" STREQUAL "")
    if (${LIB_NAME}_VERSION)
      list(FIND ${LIB_NAME}_VERSIONS "${${LIB_NAME}_VERSION}" ${LIB_NAME}_INDEX)
      if (${LIB_NAME}_INDEX EQUAL -1)
        message(FATAL_ERROR "Unsupported protobuf version: ${${LIB_NAME}_VERSION}, available versions are: ${${LIB_NAME}_VERSIONS}")
      endif()
    else()
      set(${LIB_NAME}_INDEX 0)
    endif()
    list(GET ${LIB_NAME}_VERSIONS ${${LIB_NAME}_INDEX} ${LIB_NAME}_VERSION)
    list(GET ${LIB_NAME}_URLS ${${LIB_NAME}_INDEX} ${LIB_NAME}_URL)
    list(GET ${LIB_NAME}_SHA256S ${${LIB_NAME}_INDEX} ${LIB_NAME}_SHA256)
  else()
    gen_thirdparty_pkg(${LIB_NAME} ${LIB_NAME}_URL ${LIB_NAME}_SHA256 ${LIB_NAME}_FAKE_SHA256 ${LIB_NAME}_VERSION)
  endif()
  set(${LIB_NAME}_VERSION ${${LIB_NAME}_VERSION} PARENT_SCOPE)
  set(${LIB_NAME}_URL ${${LIB_NAME}_URL} PARENT_SCOPE)
  set(${LIB_NAME}_SHA256 ${${LIB_NAME}_SHA256} PARENT_SCOPE)
  set(${LIB_NAME}_FAKE_SHA256 ${${LIB_NAME}_FAKE_SHA256} PARENT_SCOPE)
endfunction()

function(INSTALL_DATASYSTEM_TARGET TARGET)
  set(options)
  set(one_value_args EXPORT_NAME)
  set(multi_value_args)
  cmake_parse_arguments(ARG "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})
  if (NOT ${TARGET}_INSTALL_LIBPATH)
    set(${TARGET}_INSTALL_LIBPATH lib)
  endif()

  if (NOT ${TARGET}_INSTALL_BINPATH)
    set(${TARGET}_INSTALL_BINPATH bin)
  endif()

  if (ARG_EXPORT_NAME)
    install(TARGETS ${TARGET}
            EXPORT ${ARG_EXPORT_NAME}
            ARCHIVE DESTINATION ${${TARGET}_INSTALL_LIBPATH}
            LIBRARY DESTINATION ${${TARGET}_INSTALL_LIBPATH}
            RUNTIME DESTINATION ${${TARGET}_INSTALL_BINPATH})
  else()
    install(TARGETS ${TARGET}
            ARCHIVE DESTINATION ${${TARGET}_INSTALL_LIBPATH}
            LIBRARY DESTINATION ${${TARGET}_INSTALL_LIBPATH}
            RUNTIME DESTINATION ${${TARGET}_INSTALL_BINPATH})
  endif()
endfunction()

# for git information
macro(get_git_branch git_branch_out_var)
    find_package(Git QUIET)
    if (GIT_FOUND)
        execute_process(
                COMMAND ${GIT_EXECUTABLE} symbolic-ref --short -q HEAD
                WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
                ERROR_QUIET
                OUTPUT_VARIABLE ${git_branch_out_var}
                OUTPUT_STRIP_TRAILING_WHITESPACE
        )
    endif ()
endmacro()

macro(get_git_hash git_hash_out_var)
    find_package(Git QUIET)
    if (GIT_FOUND)
        execute_process(
                COMMAND ${GIT_EXECUTABLE} log -1 "--pretty=format:[%H] [%ai]"
                WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
                ERROR_QUIET
                OUTPUT_VARIABLE ${git_hash_out_var}
                OUTPUT_STRIP_TRAILING_WHITESPACE
        )
    endif ()
endmacro()

# Add datasystem GoogleTest to CTest.
#
# TARGET is the llt test executable.
#
# Additional optional arguments:
#
#   WORKING_DIR <working-directory>
#       Specify the working directory when running test executable.
#
#   TEST_ENVIRONMENTS <var1> <var2> ...
#       Specify the test environments variables when running test executable.
function(ADD_DATASYSTEM_TEST TARGET)
  set(options)
  set(one_value_args WORKING_DIR)
  set(multi_value_args TEST_ENVIRONMENTS)
  cmake_parse_arguments(ARG "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})

  if (NOT ARG_WORKING_DIR)
    set(ARG_WORKING_DIR ${CMAKE_CURRENT_BINARY_DIR})
  endif()

  if (ARG_TEST_ENVIRONMENTS)
    string(REGEX REPLACE ";" " " ARG_TEST_ENVIRONMENTS "${ARG_TEST_ENVIRONMENTS}")
  else()
    set(ARG_TEST_ENVIRONMENTS "")
  endif()

  set(_CTEST_INCLUDE_FILE "${CMAKE_CURRENT_BINARY_DIR}/${TARGET}_include.cmake")
  set(_CTEST_TEST_FILE "${CMAKE_CURRENT_BINARY_DIR}/${TARGET}_tests.cmake")
  set(_CTEST_GEN_SCRIPT "${CMAKE_SOURCE_DIR}/cmake/scripts/GoogleTestToCTest.cmake")

  add_custom_command(
    TARGET ${TARGET} POST_BUILD
    BYPRODUCTS "${_CTEST_TEST_FILE}"
    COMMAND "${CMAKE_COMMAND}"
            -D "TEST_TARGET=${TARGET}"
            -D "TEST_EXECUTABLE=$<TARGET_FILE:${TARGET}>"
            -D "TEST_WORKING_DIR=${ARG_TEST_ENVIRONMENTS}"
            -D "TEST_ENVIRONMENTS=${ARG_TEST_ENVIRONMENTS}"
            -D "CTEST_FILE=${_CTEST_TEST_FILE}"
            -P "${_CTEST_GEN_SCRIPT}"
    VERBATIM
  )

  file(WRITE "${_CTEST_INCLUDE_FILE}"
    "if(NOT EXISTS \"${_CTEST_TEST_FILE}\")\n"
    "  message(FATAL_ERROR \"${_CTEST_TEST_FILE} not found!\")\n"
    "else()\n"
    "  include(\"${_CTEST_TEST_FILE}\")\n"
    "endif()\n")

  # Add discovered tests to directory TEST_INCLUDE_FILES
  set_property(DIRECTORY APPEND PROPERTY TEST_INCLUDE_FILES "${_CTEST_INCLUDE_FILE}")
endfunction()

# Clean target build rpath. Some targets like python shared library
# would package as jar/wheel format file first then install. But there
# build rpath need to erase first.
#
# TARGET is CMake target, shared library or executable is available.
function(CLEAN_BUILD_RPATH TARGET)
  if (NOT TARGET)
    message(FATAL_ERROR "TARGET not specified!")
  endif()

  set(_BUILD_RPATH_REMOVE_SCRIPT "${CMAKE_SOURCE_DIR}/cmake/scripts/CleanBuildRpath.cmake")

  add_custom_command(
    TARGET ${TARGET} POST_BUILD
    COMMAND "${CMAKE_COMMAND}"
            -D "BIN_FILE=$<TARGET_FILE:${TARGET}>"
            -P "${_BUILD_RPATH_REMOVE_SCRIPT}"
    VERBATIM
  )
endfunction()

# Run StripAndGenHash.cmake to strip libraries in install stage.
#
# LIB_LIST <list-variable>
#    Specify the list of library path waiting to strip.
# DST_DIR <directory-path>
#    Specify the destination directory path of StripAndGenHash run.
function(STRIP_LIBS_IN_INSTALL_STAGE LIB_LIST DST_DIR)
  set(STRIP_GEN_HASH_SCRIPT ${CMAKE_SOURCE_DIR}/cmake/scripts/StripAndGenHash.cmake)
  foreach(_LIB ${${LIB_LIST}})
    # Use install code to pass variables to cmake script
    # Packaging python don't need to generate so.properties, set IGNORE_SO_PROPERTY to TRUE
    install(
      CODE "
      set(SRC_PATH_PATTERN \"${_LIB}\")
      set(DST_PATH \"${${DST_DIR}}\")
      set(STRIP_LIB \"${ENABLE_STRIP}\")
      set(IGNORE_SO_PROPERTY TRUE)
      ")
    install(SCRIPT ${STRIP_GEN_HASH_SCRIPT})
  endforeach()
endfunction()

# Package python whl file
#
# PACKAGE_NAME is the package name of python library
#
# Additional optional arguments:
#
#   CMAKE_INSTALL_PATH <output-path>
#       Specify the directory path where python whl file save.
#
#   DEPEND_TARGETS <target1> <target2> ...
#       Specify the datasystem targets which python library depend.
#
#   THIRDPARTY_LIBS_PATTERN <library-path1> <libary-path2> ...
#       Specify the third party library file path which python library depend, support match pattern.
#       Both soname and filename path is needed.
function(PACKAGE_DATASYSTEM_WHEEL PACKAGE_NAME)
  set(options)
  set(one_value_args CMAKE_INSTALL_PATH)
  set(multi_value_args DEPEND_TARGETS THIRDPATRY_LIBS_PATTERN)
  cmake_parse_arguments(ARG "${options}" "${one_value_args}"  "${multi_value_args}" ${ARGN})

  # Store all temporary package files, will be deleted after package finish.
  set(DATASYSTEM_WHEEL_PATH ${CMAKE_BINARY_DIR}/dist/datasystem)
  set(DATASYSTEM_SETUP_PATH ${CMAKE_BINARY_DIR}/dist)
  set(DATASYSTEM_PACKAGE_LIBPATH ${CMAKE_SOURCE_DIR})

  # Store helm chart
  set(HELM_CHART_PATH ${CMAKE_SOURCE_DIR}/k8s/helm_chart)
  set(SERVER_LIB ${CMAKE_INSTALL_PREFIX}/datasystem/service/lib)
  set(OUTPUT_INCLUDE_DIF ${CMAKE_INSTALL_PREFIX}/datasystem/sdk/cpp/include)
  set(SDK_LIB ${CMAKE_INSTALL_PREFIX}/datasystem/sdk/cpp/lib)
  set(PYTHON_SDK ${CMAKE_SOURCE_DIR}/python)
  set(PYTHON_LIBPATH ${CMAKE_BINARY_DIR}/python_lib)

  FILE(GLOB_RECURSE THIRDPARTY_LIBS_PATH "${PYTHON_LIBPATH}/*.so*")
  LIST(FILTER THIRDPARTY_LIBS_PATH EXCLUDE REGEX ".*sym.*")

  foreach(_PATTERN ${ARG_THIRDPATRY_LIBS_PATTERN})
    file(GLOB_RECURSE THIRDPARTY_LIB_LIST ${_PATTERN})
    foreach(_THIRDPARTY_LIB ${THIRDPARTY_LIB_LIST})
      install(FILES ${_THIRDPARTY_LIB} DESTINATION ${PYTHON_LIBPATH})
      if (NOT IS_SYMLINK ${_THIRDPARTY_LIB})
        get_filename_component(_LIB_NAME ${_THIRDPARTY_LIB} NAME)
        list(APPEND NEED_STRIP_LIBS ${PYTHON_LIBPATH}/${_LIB_NAME})
      endif()
    endforeach()
  endforeach()

  # Install datasystem target we need.
  foreach(_TARGET ${ARG_DEPEND_TARGETS})
    set(${_TARGET}_INSTALL_LIBPATH ${PYTHON_LIBPATH})
    install_datasystem_target(${_TARGET})
    list(APPEND NEED_STRIP_LIBS ${PYTHON_LIBPATH}/$<TARGET_FILE_NAME:${_TARGET}>)
  endforeach()

  # Strip libraries in PYTON_LIBPATH
  list(TRANSFORM ARG_NEED_STRIP_LIBS PREPEND "${PYTHON_LIBPATH}/")
  strip_libs_in_install_stage(NEED_STRIP_LIBS PYTHON_LIBPATH)

  # Copy chart files to package lib path
  install(DIRECTORY ${HELM_CHART_PATH}/
          DESTINATION ${DATASYSTEM_WHEEL_PATH}/helm_chart)

  # Copy cpp include files to package lib path
  install(DIRECTORY ${CMAKE_SOURCE_DIR}/include
          DESTINATION ${DATASYSTEM_WHEEL_PATH}/)

  # Copy service lib to package lib path
  install(DIRECTORY ${SERVER_LIB}/
          DESTINATION ${DATASYSTEM_WHEEL_PATH}/lib/)

  # Copy sdk lib
  install(DIRECTORY ${SDK_LIB}/
          DESTINATION ${DATASYSTEM_WHEEL_PATH}/lib/
          PATTERN "cmake" EXCLUDE)

  # Copy python sdk
  install(DIRECTORY ${PYTHON_SDK}/
          DESTINATION ${DATASYSTEM_WHEEL_PATH}/)

  # Copy ds cli source files to package lib path
  install(DIRECTORY ${CMAKE_SOURCE_DIR}/cli
    DESTINATION ${DATASYSTEM_WHEEL_PATH})

  #Copy setup.py
  install(FILES ${CMAKE_SOURCE_DIR}/setup.py DESTINATION ${DATASYSTEM_SETUP_PATH}/)

  # Copy VERSION and LICENSE to package lib path
  install(FILES ${CMAKE_SOURCE_DIR}/VERSION ${CMAKE_SOURCE_DIR}/LICENSE ${CMAKE_SOURCE_DIR}/README.md
          DESTINATION ${DATASYSTEM_WHEEL_PATH})
  # Copy cpp template to package lib path
  install(DIRECTORY ${CMAKE_SOURCE_DIR}/cli/cpp_template
          DESTINATION ${DATASYSTEM_WHEEL_PATH})
  # Copy worker and worker_config to package lib path
  install(FILES ${CMAKE_INSTALL_PREFIX}/datasystem/service/datasystem_worker ${CMAKE_SOURCE_DIR}/cli/deploy/conf/worker_config.json ${CMAKE_SOURCE_DIR}/cli/deploy/conf/cluster_config.json
          DESTINATION ${DATASYSTEM_WHEEL_PATH})

  find_package(Python3 COMPONENTS Interpreter Development)
  set(CONFIG_PACKAGE_SCRIPT ${CMAKE_BINARY_DIR}/PackageDatasystem.cmake)
  # Generate PackagePythonSDK.cmake to run setup.py
  configure_file(${CMAKE_SOURCE_DIR}/cmake/scripts/PackageDatasystem.cmake.in
                  ${CONFIG_PACKAGE_SCRIPT} @ONLY)
  install(SCRIPT ${CONFIG_PACKAGE_SCRIPT})
endfunction()

function(PACKAGE_PYTHON PACKAGE_NAME)
  set(options)
  set(one_value_args PYTHON_SRC_DIR CMAKE_INSTALL_PATH)
  set(multi_value_args DEPEND_TARGETS THIRDPATRY_LIBS_PATTERN)
  cmake_parse_arguments(ARG "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})

  # Store all temporary package files, will be deleted after package finish.
  set(PYTHON_PACKAGE_PATH ${CMAKE_BINARY_DIR}/python_api)
  # Store python source files and libraries.
  set(PYTHON_PACKAGE_LIBPATH ${PYTHON_PACKAGE_PATH}/${PACKAGE_NAME})
  # Store installed libraries, also store sym files if ENABLE_STRIP.
  set(PYTHON_LIBPATH ${CMAKE_BINARY_DIR}/python_lib)

  FILE(GLOB_RECURSE THIRDPARTY_LIBS_PATH "${PYTHON_LIBPATH}/*.so*")
  LIST(FILTER THIRDPARTY_LIBS_PATH EXCLUDE REGEX ".*sym.*")

  foreach(_PATTERN ${ARG_THIRDPATRY_LIBS_PATTERN})
    file(GLOB_RECURSE THIRDPARTY_LIB_LIST ${_PATTERN})
    foreach(_THIRDPARTY_LIB ${THIRDPARTY_LIB_LIST})
      install(FILES ${_THIRDPARTY_LIB} DESTINATION ${PYTHON_LIBPATH})
      if (NOT IS_SYMLINK ${_THIRDPARTY_LIB})
        get_filename_component(_LIB_NAME ${_THIRDPARTY_LIB} NAME)
        list(APPEND NEED_STRIP_LIBS ${PYTHON_LIBPATH}/${_LIB_NAME})
      endif()
    endforeach()
  endforeach()

  # Install datasystem target we need.
  foreach(_TARGET ${ARG_DEPEND_TARGETS})
    set(${_TARGET}_INSTALL_LIBPATH ${PYTHON_LIBPATH})
    install_datasystem_target(${_TARGET})
    list(APPEND NEED_STRIP_LIBS ${PYTHON_LIBPATH}/$<TARGET_FILE_NAME:${_TARGET}>)
  endforeach()

  # Strip libraries in PYTON_LIBPATH
  list(TRANSFORM ARG_NEED_STRIP_LIBS PREPEND "${PYTHON_LIBPATH}/")
  strip_libs_in_install_stage(NEED_STRIP_LIBS PYTHON_LIBPATH)

  # Copy python source files to package lib path
  install(DIRECTORY ${ARG_PYTHON_SRC_DIR}/
          DESTINATION ${PYTHON_PACKAGE_LIBPATH})
  # Copy VERSION and LICENSE to package lib path
  install(FILES ${CMAKE_SOURCE_DIR}/VERSION ${CMAKE_SOURCE_DIR}/LICENSE
          DESTINATION ${PYTHON_PACKAGE_LIBPATH})
  set(CONFIG_PACKAGE_SCRIPT ${CMAKE_BINARY_DIR}/PackagePythonSDK.cmake)
  # Generate PackagePythonSDK.cmake to run setup.py
  configure_file(${CMAKE_SOURCE_DIR}/cmake/scripts/PackagePython.cmake.in
          ${CONFIG_PACKAGE_SCRIPT} @ONLY)
  install(SCRIPT ${CONFIG_PACKAGE_SCRIPT})
endfunction()

function(ADD_THIRDPARTY_SO LIB_NAME)
  set(options)
  set(one_value_args URL SHA256 VERSION TOOLCHAIN CONF_PATH)
  set(multi_value_args COMPONENTS CONF_OPTIONS PRE_CONFIGURE PATCHES CXX_FLAGS C_FLAGS LINK_FLAGS EXTRA_MSGS)
  cmake_parse_arguments(ARG "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})

  string(TOLOWER ${LIB_NAME} _LIB_NAME_LOWER)

  # Generate a unique install dir name, the impact factors are as follow:
  # Lib name:
  set(${LIB_NAME}_CONF_TXT "${_LIB_NAME_LOWER}")
  # SHA256:
  set(${LIB_NAME}_CONF_TXT "${${LIB_NAME}_CONF_TXT}_${ARG_SHA256}")
  # Version:
  set(${LIB_NAME}_CONF_TXT "${${LIB_NAME}_CONF_TXT}_${ARG_VERSION}")
  # Components:
  set(${LIB_NAME}_CONF_TXT "${${LIB_NAME}_CONF_TXT}_${ARG_COMPONENTS}")

  string(REPLACE ";" "_" ${LIB_NAME}_CONF_TXT ${${LIB_NAME}_CONF_TXT})
  string(SHA256 _ROOT_SUFFIX ${${LIB_NAME}_CONF_TXT})
  set(${LIB_NAME}_ROOT "${DS_OPENSOURCE_DIR}/${_LIB_NAME_LOWER}_${_ROOT_SUFFIX}")

  # Check if we have cache the lib, if true, reuse it directly.
  set(_VERIFY_FILE "${${LIB_NAME}_ROOT}/${LIB_NAME}_install.txt")
  if(EXISTS ${${LIB_NAME}_ROOT})
    if (EXISTS ${_VERIFY_FILE})
      set(${LIB_NAME}_FOUND TRUE)
    endif()

    if(${LIB_NAME}_FOUND)
      message(STATUS "${LIB_NAME} found in ${${LIB_NAME}_ROOT}...")
      if (EXISTS ${${LIB_NAME}_ROOT}/lib64)
        set(${LIB_NAME}_LIB_PATH ${${LIB_NAME}_ROOT}/lib64 PARENT_SCOPE)
      else()
        set(${LIB_NAME}_LIB_PATH ${${LIB_NAME}_ROOT}/lib PARENT_SCOPE)
      endif()
      set(${LIB_NAME}_ROOT "${${LIB_NAME}_ROOT}" PARENT_SCOPE)
      return()
    else()
      message(STATUS "${LIB_NAME} not found in ${${LIB_NAME}_ROOT}, need recompile...")
      # Well, although the cache directory exists, it appears to be corrupted (because we can't find
      # it via find_package). So remove the directory directly and we will recompile the lib.
      file(REMOVE_RECURSE "${${LIB_NAME}_ROOT}")
    endif()
  endif()

  # Fetch the package first.
  download_lib_pkg(${_LIB_NAME_LOWER} ${ARG_URL} ${ARG_SHA256})

  file(COPY ${${_LIB_NAME_LOWER}_SOURCE_DIR}/lib/release/libStsSdk.so DESTINATION ${${LIB_NAME}_ROOT}/lib)
  file(COPY ${${_LIB_NAME_LOWER}_SOURCE_DIR}/include DESTINATION ${${LIB_NAME}_ROOT})

  # Write install text to root dir for verify purpose.
  file(WRITE "${_VERIFY_FILE}" "${${LIB_NAME}_CONF_TXT}")

  # For output package variables.
  if (EXISTS ${${LIB_NAME}_ROOT}/lib64)
    set(${LIB_NAME}_LIB_PATH ${${LIB_NAME}_ROOT}/lib64 PARENT_SCOPE)
  else()
    set(${LIB_NAME}_LIB_PATH ${${LIB_NAME}_ROOT}/lib PARENT_SCOPE)
  endif()

  set(${LIB_NAME}_ROOT ${${LIB_NAME}_ROOT} PARENT_SCOPE)
endfunction()

# Install file support pattern matching.
# Cmake's install(FILE) don't support pattern matching, and install(DIRECTORY) is hard to control details.
# So we add INSTALL_FILE_PATTERN to work like install(FILE) but support passing pattern.
# Make Sure the specific files can be found in configure stage.
#
# Additional optional arguments:
#
#   DEST_DIR <directory-path>
#       Specify the destination directory path of installing file.
#
#   PATH_PATTERN <pattern1> <pattern2> ...
#       Specify the library path pattern, like ${zlib_LIBRARY}/libz.so* .
#
#   PERMISSIONS <permission1> <permission2> ...
#       Specify permissions of copied file.
function(INSTALL_FILE_PATTERN)
  set(options)
  set(one_value_args DEST_DIR)
  set(multi_value_args PATH_PATTERN PERMISSIONS)
  cmake_parse_arguments(ARG "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})

  foreach(PATTERN ${ARG_PATH_PATTERN})
    file(GLOB_RECURSE PATH_LIST ${PATTERN})
    foreach(FILE_PATH ${PATH_LIST})
      if(ARG_PERMISSIONS)
        install(FILES ${FILE_PATH} DESTINATION ${ARG_DEST_DIR} PERMISSIONS ${ARG_PERMISSIONS})
      else()
        install(FILES ${FILE_PATH} DESTINATION ${ARG_DEST_DIR})
      endif()
    endforeach()
  endforeach()
endfunction()

# Generate protobuf cc files.
#
# SRCS is the output variable of the protobuf source files.
#
# HDRS is the output variable of the protobuf header files.
#
# TARGET_DIR is the generate cc files target directory.
#
# Additional optional arguments:
#
#   PROTO_FILES <file1> <file2> ...
#       Protobuf source files to be compiled.
#
#   SOURCE_ROOT <dir>
#       Protobuf source files root directory, default is ${CMAKE_CURRENT_SOURCE_DIR},
#       if protobuf source files are not in ${CMAKE_SOURCE_DIR}, this variable must
#       be set.
#
#   PROTO_DEPEND <target>
#       If the generated cc files need to depend some target this variable must be set.
function(GENERATE_PROTO_CPP SRCS HDRS TARGET_DIR)
  set(options)
  set(one_value_args SOURCE_ROOT PROTO_DEPEND)
  set(multi_value_args PROTO_FILES)
  cmake_parse_arguments(ARG "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})

  if (NOT ARG_PROTO_FILES)
    message(SEND_ERROR "GENERATE_PROTO_CPP() called without any proto files")
  endif ()

  if (NOT ARG_SOURCE_ROOT)
    set(ARG_SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR})
  endif()

  set(${SRCS})
  set(${HDRS})
  set(_PROTO_IMPORT_ARGS -I "${ARG_SOURCE_ROOT}")

  # Add protobuf import dir to avoid import report by protoc compiler.
  foreach (_PROTO_FILE ${ARG_PROTO_FILES})
    get_filename_component(_ABS_FILE ${_PROTO_FILE} ABSOLUTE)
    get_filename_component(_ABS_PATH ${_ABS_FILE} PATH)
    list(FIND _PROTO_IMPORT_ARGS ${_ABS_PATH} _IMPORT_EXIST)
    if (${_IMPORT_EXIST} EQUAL -1)
      list(APPEND _PROTO_IMPORT_ARGS -I ${_ABS_PATH})
    endif()
  endforeach()

  foreach (_PROTO_FILE ${ARG_PROTO_FILES})
    get_filename_component(_ABS_FILE   ${_PROTO_FILE} ABSOLUTE)
    get_filename_component(_ABS_DIR    ${_PROTO_FILE} DIRECTORY)
    get_filename_component(_PROTO_NAME ${_PROTO_FILE} NAME_WE)
    get_filename_component(_PROTO_DIR  ${_PROTO_FILE} PATH)
    file(RELATIVE_PATH _REL_DIR ${ARG_SOURCE_ROOT} ${_ABS_DIR})
    file(MAKE_DIRECTORY ${TARGET_DIR}/${_REL_DIR})
    list(APPEND ${SRCS} ${TARGET_DIR}/${_REL_DIR}/${_PROTO_NAME}.pb.cc)
    list(APPEND ${HDRS} ${TARGET_DIR}/${_REL_DIR}/${_PROTO_NAME}.pb.h)
    add_custom_command(
            OUTPUT "${TARGET_DIR}/${_REL_DIR}/${_PROTO_NAME}.pb.cc" "${TARGET_DIR}/${_REL_DIR}/${_PROTO_NAME}.pb.h"
            COMMAND ${CMAKE_COMMAND} -E env LD_LIBRARY_PATH=${Protobuf_LIB_PATH}:$ENV{LD_LIBRARY_PATH}
            $<TARGET_FILE:protobuf::protoc>
            ARGS ${_PROTO_IMPORT_ARGS} --cpp_out=${TARGET_DIR} ${_ABS_FILE}
            DEPENDS ${_ABS_FILE}
            COMMENT "Running c++ protocol buffer compiler on ${_PROTO_FILE}" VERBATIM)

    if (ARG_PROTO_DEPEND)
      add_custom_target(PROTO_LIB_DEPEND_${_PROTO_NAME} DEPENDS
              "${TARGET_DIR}/${_PROTO_NAME}.pb.cc"
              "${TARGET_DIR}/${_PROTO_NAME}.pb.h")
      add_dependencies(${ARG_PROTO_DEPEND} PROTO_LIB_DEPEND_${_PROTO_NAME})
    endif()
  endforeach ()

  set_source_files_properties(${${SRCS}} ${${HDRS}} PROPERTIES GENERATED TRUE)
  set(${SRCS} ${${SRCS}} PARENT_SCOPE)
  set(${HDRS} ${${HDRS}} PARENT_SCOPE)
endfunction()
