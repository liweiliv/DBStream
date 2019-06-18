cmake_minimum_required (VERSION 2.8)
set(CMAKE_BUILD_TYPE "Debug")
project (DBStream)
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
	ADD_DEFINITIONS(-DOS_LINUX)
	set(CMAKE_CXX_FLAGS "  -Wall -Wno-unused-function -Wno-invalid-offsetof -msse4.2 -std=c++11 ${CMAKE_CXX_FLAGS}")
ELSEIF (CMAKE_SYSTEM_NAME MATCHES "Windows")
        ADD_DEFINITIONS(-DOS_WIN)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")
SET(LIBRARY_OUTPUT_PATH lib)
include (GenerateExportHeader)
#util
aux_source_directory(util utilDir)
add_library(util SHARED ${utilDir})
#meta
aux_source_directory(meta metaDir)
add_library(meta SHARED ${metaDir})
TARGET_LINK_LIBRARIES(meta util sqlParser)

#sql parser
aux_source_directory(sqlParser sqlparserDir)
add_library(sqlParser SHARED ${sqlparserDir})
TARGET_LINK_LIBRARIES(sqlParser util)

add_library(mysqlParserFuncs SHARED sqlParser/mysqlParseFunc/mysqlSqlParserFuncs.cpp)
TARGET_LINK_LIBRARIES(mysqlParserFuncs meta)
#memory
aux_source_directory(memory memoryDir)
add_library(memory SHARED ${memoryDir})
TARGET_LINK_LIBRARIES(memory util)
#store
aux_source_directory(store storeDir)
add_library(store SHARED ${storeDir})
TARGET_LINK_LIBRARIES(store  util memory meta)


#unit test
SET(EXECUTABLE_OUTPUT_PATH test)
ADD_EXECUTABLE(testMysqlSql  sqlParser/mysqlParseFunc/unittest/testMysqlSql.cpp)
TARGET_LINK_LIBRARIES(testMysqlSql util sqlParser)
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
	TARGET_LINK_LIBRARIES(testMysqlSql dl)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")

ADD_EXECUTABLE(testMetaDataCollection meta/unittest/testMetaDataCollection.cpp)
TARGET_LINK_LIBRARIES(testMetaDataCollection util sqlParser meta)
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
	TARGET_LINK_LIBRARIES(testMetaDataCollection dl)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")

ADD_EXECUTABLE(testThreadLocal  util/unittest/testThreadLocal.cpp)
TARGET_LINK_LIBRARIES(testThreadLocal util)
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
        TARGET_LINK_LIBRARIES(testThreadLocal pthread)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")

ADD_EXECUTABLE(testpageTable  util/unittest/testpageTable.cpp)
TARGET_LINK_LIBRARIES(testpageTable util)
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
        TARGET_LINK_LIBRARIES(testpageTable pthread)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")

ADD_EXECUTABLE(testbasciBufferPool  memory/unittest/testbasciBufferPool.cpp)
TARGET_LINK_LIBRARIES(testbasciBufferPool memory util)
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
        TARGET_LINK_LIBRARIES(testbasciBufferPool pthread)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")

ADD_EXECUTABLE(testLinkList  util/unittest/testLinkList.cpp)
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
        TARGET_LINK_LIBRARIES(testLinkList pthread)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")

ADD_EXECUTABLE(testDualLinkList  util/unittest/testDualLinkList.cpp)
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
        TARGET_LINK_LIBRARIES(testDualLinkList pthread)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")
