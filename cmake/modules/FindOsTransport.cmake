# - Find header For libos_transport
# This module defines
#  OS_TRANSPORT_INCLUDE_DIR, directory containing os-transport headers


# need os transport
find_path(OS_TRANSPORT_INCLUDE_DIR os-transport/os_transport.h
    DOC   "Path to the os-transport header file"
    HINTS /usr/local/ /usr/include
)
