# Rules to install header files and (shared library) on the system

if(PROJECT_IS_TOP_LEVEL)
  set(
      CMAKE_INSTALL_INCLUDEDIR "include/p2p_transfer-${PROJECT_VERSION}"
      CACHE STRING ""
  )
  set_property(CACHE CMAKE_INSTALL_INCLUDEDIR PROPERTY TYPE PATH)
endif()

include(CMakePackageConfigHelpers)
include(GNUInstallDirs)

# find_package(<package>) call for consumers to find this project
set(package p2p_transfer)

install(
    FILES
    include/p2p.h
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
    COMPONENT p2p_transfer_Development
)

file(MAKE_DIRECTORY ${CMAKE_INSTALL_INCLUDEDIR}/p2p_transfer)

install(
    FILES
    ${CMAKE_CURRENT_BINARY_DIR}/export/p2p_transfer/p2p_transfer_export.h
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/p2p_transfer"
    COMPONENT p2p_transfer_Development
)

install(
    TARGETS p2p_transfer
    EXPORT p2p_transferTargets
    RUNTIME #
    COMPONENT p2p_transfer_Runtime
    LIBRARY #
    COMPONENT p2p_transfer_Runtime
    NAMELINK_COMPONENT p2p_transfer_Development
    ARCHIVE #
    COMPONENT p2p_transfer_Development
    INCLUDES #
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
)

write_basic_package_version_file(
    "${package}ConfigVersion.cmake"
    COMPATIBILITY SameMajorVersion
)

# Allow package maintainers to freely override the path for the configs
set(
    p2p_transfer_INSTALL_CMAKEDIR "${CMAKE_INSTALL_LIBDIR}/cmake/${package}"
    CACHE STRING "CMake package config location relative to the install prefix"
)
set_property(CACHE p2p_transfer_INSTALL_CMAKEDIR PROPERTY TYPE PATH)
mark_as_advanced(p2p_transfer_INSTALL_CMAKEDIR)

install(
    FILES cmake/install-config.cmake
    DESTINATION "${p2p_transfer_INSTALL_CMAKEDIR}"
    RENAME "${package}Config.cmake"
    COMPONENT p2p_transfer_Development
)

install(
    FILES "${PROJECT_BINARY_DIR}/${package}ConfigVersion.cmake"
    DESTINATION "${p2p_transfer_INSTALL_CMAKEDIR}"
    COMPONENT p2p_transfer_Development
)

install(
    EXPORT p2p_transferTargets
    NAMESPACE p2p_transfer::
    DESTINATION "${p2p_transfer_INSTALL_CMAKEDIR}"
    COMPONENT p2p_transfer_Development
)

if(PROJECT_IS_TOP_LEVEL)
  include(CPack)
endif()
