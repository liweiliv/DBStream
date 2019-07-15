cmake_minimum_required (VERSION 2.8)
set(CMAKE_BUILD_TYPE "Debug")
project (DBStream)
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
	ADD_DEFINITIONS(-DOS_LINUX)
	set(CMAKE_CXX_FLAGS "-g  -Wall -Wno-unused-function -Wno-invalid-offsetof -msse4.2 -std=c++11 ${CMAKE_CXX_FLAGS}")
ELSEIF (CMAKE_SYSTEM_NAME MATCHES "Windows")
        ADD_DEFINITIONS(-DOS_WIN)
		include_directories(C:\\Program Files\\MySQL\\MySQL Server 8.0\\include)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")
include (GenerateExportHeader)

#lz4
find_library(LZ4 lz4 lz4/lib)
if(NOT LZ4)
        message(FATAL_ERROR "lz4 library not found")
endif()
#glog
find_library(GLOG glog /usr/local/lib)
if(NOT GLOG)
        message(FATAL_ERROR "glog library not found")
endif()
#mysqlclient
find_library(MYSQL_CLIENT libmysqlclient.a /usr/lib64/mysql/)
if(NOT MYSQL_CLIENT)
        message(FATAL_ERROR "libmysqlclient.a  not found")
endif()




#util
SET(LIBRARY_OUTPUT_PATH lib)
aux_source_directory(util utilDir)
add_library(util SHARED ${utilDir})
TARGET_LINK_LIBRARIES(util ${GLOG})
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
	TARGET_LINK_LIBRARIES(util pthread)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")
#meta
SET(LIBRARY_OUTPUT_PATH lib)
aux_source_directory(meta metaDir)
add_library(meta SHARED ${metaDir})
TARGET_LINK_LIBRARIES(meta util sqlParser ${GLOG})

#sql parser
SET(LIBRARY_OUTPUT_PATH lib)
aux_source_directory(sqlParser sqlparserDir)
add_library(sqlParser SHARED ${sqlparserDir})
TARGET_LINK_LIBRARIES(sqlParser util ${GLOG})

add_library(mysqlParserFuncs SHARED sqlParser/mysqlParseFunc/mysqlSqlParserFuncs.cpp)
TARGET_LINK_LIBRARIES(mysqlParserFuncs meta ${GLOG})
#memory
SET(LIBRARY_OUTPUT_PATH lib)
aux_source_directory(memory memoryDir)
add_library(memory SHARED ${memoryDir})
TARGET_LINK_LIBRARIES(memory util ${GLOG})
#store
SET(LIBRARY_OUTPUT_PATH lib)
aux_source_directory(store storeDir)
add_library(store SHARED ${storeDir})
TARGET_LINK_LIBRARIES(store  util memory meta ${LZ4} ${GLOG})

#datasource
SET(LIBRARY_OUTPUT_PATH lib)
aux_source_directory(dataSource dataSourceDir)
add_library(dataSource SHARED ${dataSourceDir})
TARGET_LINK_LIBRARIES(dataSource  util store meta ${GLOG})
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
	TARGET_LINK_LIBRARIES(dataSource dl)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")
#mysqlDataSource
SET(LIBRARY_OUTPUT_PATH lib)
include_directories(/usr/include/mysql)
aux_source_directory(dataSource/mysqlDataSource mysqlDataSourceDir)
add_library(mysqlDataSource SHARED ${mysqlDataSourceDir})
TARGET_LINK_LIBRARIES(mysqlDataSource  util store meta sqlParser ${GLOG} ${MYSQL_CLIENT})
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
	TARGET_LINK_LIBRARIES(mysqlDataSource pthread ssl crypto rt z)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")
#main
SET(EXECUTABLE_OUTPUT_PATH bin)
ADD_EXECUTABLE(DBStream  DBStream.cpp)
TARGET_LINK_LIBRARIES(DBStream util sqlParser dataSource store meta memory)
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
	TARGET_LINK_LIBRARIES(DBStream dl pthread)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")
#-------------------------------------------------------------------------------









#------------------------------------------------------------------------------
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

ADD_EXECUTABLE(testRingBuffer  memory/unittest/testRingBuffer.cpp)
TARGET_LINK_LIBRARIES(testRingBuffer memory util)

ADD_EXECUTABLE(testLinkList  util/unittest/testLinkList.cpp)
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
        TARGET_LINK_LIBRARIES(testLinkList pthread)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")

ADD_EXECUTABLE(testDualLinkList  util/unittest/testDualLinkList.cpp)
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
        TARGET_LINK_LIBRARIES(testDualLinkList pthread)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")

ADD_EXECUTABLE(testConfig  util/unittest/testConfig.cpp)
TARGET_LINK_LIBRARIES(testConfig util)
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
        TARGET_LINK_LIBRARIES(testConfig pthread)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")

ADD_EXECUTABLE(testTrieTree  util/unittest/testTrieTree.cpp)
TARGET_LINK_LIBRARIES(testTrieTree util)

ADD_EXECUTABLE(testSparsepp util/unittest/testSparsepp.cpp)

