if (DOWNLOAD_UB)
    # set the url and archive name to be downloaded
    if (URMA_OVER_UB)
        set(UB_ARCHIVE_DATETIME 20251015)                     # format YYYYMMDDhhmmss
        if ("${UB_URL}" STREQUAL "" OR "${UB_SHA256}" STREQUAL "")
            message(FATAL_ERROR "UMDK package download paths need to be specified for URMA over UB scenario")
        endif()
        if (NOT CMAKE_SYSTEM_PROCESSOR STREQUAL "aarch64")
            # Note that only openeuler2403 is supported
            message(FATAL_ERROR "Unsupported system processor: ${CMAKE_SYSTEM_PROCESSOR}")
        endif()
    else()
        set(UB_ARCHIVE_DATETIME 20240124)                     # format YYYYMMDDhhmmss
        if ("${UB_URL}" STREQUAL "" AND "${UB_SHA256}" STREQUAL "")
            if (CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64")
                set(UB_URL "xxx")
                set(UB_SHA256 "xxx")
            elseif (CMAKE_SYSTEM_PROCESSOR STREQUAL "aarch64")
                set(UB_URL "xxx")
                set(UB_SHA256 "xxx")
            else()
                message(FATAL_ERROR "Unsupported system processor: ${CMAKE_SYSTEM_PROCESSOR}")
            endif()
        endif()
    endif()

    ADD_THIRDPARTY_LIB(UB
        URL ${UB_URL}
        SHA256 ${UB_SHA256}
        VERSION ${UB_ARCHIVE_DATETIME})
endif()
