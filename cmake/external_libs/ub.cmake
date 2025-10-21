if (DOWNLOAD_UB)
    # set the url and archive name to be downloaded
    set(UB_ARCHIVE_DATETIME 20240124)                     # format YYYYMMDDhhmmss
    if ("${UB_URL}" STREQUAL "" AND "${UB_SHA256}" STREQUAL "")
        if (CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64")
            set(UB_URL "https://cmc-nkg-artifactory.cmc.tools.huawei.com/artifactory/cmc-software-release/UBUS%20BaseSoftware%20TD/umdk_master/V100R005C00B304/Software/x86_64/EulerOS-V2R10/01.InstallationPackage/UMDK-V100R005C00B304-EulerOS-V2R10-mlx.tar.gz")
            set(UB_SHA256 "d78a214da24b709fc21f3c20b73583f24e87a943a92faefd5d90d5edd31d557f")
        elseif (CMAKE_SYSTEM_PROCESSOR STREQUAL "aarch64")
            set(UB_URL "https://cmc-nkg-artifactory.cmc.tools.huawei.com/artifactory/cmc-software-release/UBUS%20BaseSoftware%20TD/umdk_master/V100R005C00B304/Software/aarch64/EulerOS-V2R10/01.InstallationPackage/UMDK-V100R005C00B304-EulerOS-V2R10-mlx.tar.gz")
            set(UB_SHA256 "2ae8d1f00ffbd3a7edf68375ee20439de8627597f2efced290ddeed6525caef6")
        else()
            message(FATAL_ERROR "Unsupported system processor: ${CMAKE_SYSTEM_PROCESSOR}")
        endif()
    endif()

    ADD_THIRDPARTY_LIB(UB
        URL ${UB_URL}
        SHA256 ${UB_SHA256}
        VERSION ${UB_ARCHIVE_DATETIME})
endif()