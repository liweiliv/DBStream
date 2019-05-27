cmake_minimum_required (VERSION 2.8)
set(CMAKE_BUILD_TYPE "Debug")
project (DBStream)
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
	ADD_DEFINITIONS(-DOS_LINUX)
	set(CMAKE_CXX_FLAGS "-Wall -Wno-unused-function  -std=c++11 ${CMAKE_CXX_FLAGS}")
ELSEIF (CMAKE_SYSTEM_NAME MATCHES "Windows")
        ADD_DEFINITIONS(-DOS_WIN)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")
SET(LIBRARY_OUTPUT_PATH lib)
#util
aux_source_directory(util utilDir)
add_library(util SHARED ${utilDir})
#meta
aux_source_directory(meta metaDir)
add_library(meta SHARED ${metaDir})
#sql parser
aux_source_directory(sqlParser sqlparserDir)
add_library(sqlParser SHARED ${sqlparserDir})
add_library(mysqlParserFuncs SHARED sqlParser/mysqlParseFunc/mysqlSqlParserFuncs.cpp)
#store
aux_source_directory(store storeDir)
add_library(store SHARED ${storeDir})


#unit test
SET(EXECUTABLE_OUTPUT_PATH test)
ADD_EXECUTABLE(testMysqlSql  sqlParser/mysqlParseFunc/unittest/testMysqlSql.cpp)
TARGET_LINK_LIBRARIES(testMysqlSql util sqlParser meta dl)

ADD_EXECUTABLE(testMetaDataCollection meta/unittest/testMetaDataCollection.cpp)
TARGET_LINK_LIBRARIES(testMetaDataCollection util sqlParser meta dl)
