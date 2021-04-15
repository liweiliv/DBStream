#util
aux_source_directory(. utilDir)
add_library(util SHARED ${utilDir})
TARGET_LINK_LIBRARIES(util ${GLOG} ${GFLAGS})
target_include_directories(util PUBLIC ${BASE_INCLUDE_PATH} ${GLOG_INCLUDE_PATH} ${GLFAG_INCLUDE_PATH})

IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
	TARGET_LINK_LIBRARIES(util pthread)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")

ADD_SUBDIRECTORY(unittest)
