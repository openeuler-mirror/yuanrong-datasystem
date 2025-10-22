if (DOWNLOAD_UB)
    # set the url and archive name to be downloaded
    set(UB_ARCHIVE_DATETIME 20240124)                     # format YYYYMMDDhhmmss
    if ("${UB_URL}" STREQUAL "" AND "${UB_SHA256}" STREQUAL "")
        if (CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64")
            # wait for modify
            set(UB_URL "xxx")
            set(UB_SHA256 "xxx")
        elseif (CMAKE_SYSTEM_PROCESSOR STREQUAL "aarch64")
            # wait for modify
            set(UB_URL "xxx")
            set(UB_SHA256 "xxx")
        else()
            message(FATAL_ERROR "Unsupported system processor: ${CMAKE_SYSTEM_PROCESSOR}")
        endif()
    endif()

    ADD_THIRDPARTY_LIB(UB
        URL ${UB_URL}
        SHA256 ${UB_SHA256}
        VERSION ${UB_ARCHIVE_DATETIME})
endif()