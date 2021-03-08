cmake_minimum_required (VERSION 2.8)
set(CMAKE_BUILD_TYPE "Debug")
project (DBStream)
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
	ADD_DEFINITIONS(-DOS_LINUX)
	ADD_DEFINITIONS(-D_GLIBCXX_USE_CXX11_ABI=0)
	set(CMAKE_CXX_FLAGS "-g -Wall -Wno-unused-function -Wno-invalid-offsetof -msse4.2 -std=c++17 ${CMAKE_CXX_FLAGS}")
	SET(LIBRARY_OUTPUT_PATH lib)
ELSEIF (CMAKE_SYSTEM_NAME MATCHES "Windows")
        ADD_DEFINITIONS(-DOS_WIN)
		ADD_DEFINITIONS(-DWIN32_LEAN_AND_MEAN)
		SET(LIBRARY_OUTPUT_PATH bin)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")
include (GenerateExportHeader) 
STRING( REGEX REPLACE ".*/(.*)" "\\1" CURRENT_FOLDER ${CMAKE_CURRENT_SOURCE_DIR} ) 
SET(BASE_INCLUDE_PATH "${CMAKE_CURRENT_SOURCE_DIR}")
message(STATUS "pwd: ${BASE_INCLUDE_PATH}")
SET(EXECUTABLE_OUTPUT_PATH bin)
ADD_DEFINITIONS(-DASIO_STANDALONE)

#ADD_DEFINITIONS(-DDEBUG)

#lz4
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
	find_library(LZ4 lz4 lz4/lib)
	if(NOT LZ4)
        message(FATAL_ERROR "lz4 library not found")
	endif()
ELSEIF (CMAKE_SYSTEM_NAME MATCHES "Windows")
	find_library(LZ4 liblz4 ./lz4/dll)
	if(NOT LZ4)
        message(FATAL_ERROR "lz4 library not found")
	endif()
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")

find_package(GFLAGS REQUIRED)

#glog
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
	#	find_package(GLOG REQUIRED)
	SET(GLOG_INCLUDE_PATH "/usr/local/include/glog/")
	find_library(GLOG glog)
	if(NOT GLOG)
		message(FATAL_ERROR "glog library not found")
	endif()
ELSEIF (CMAKE_SYSTEM_NAME MATCHES "Windows")
	ADD_DEFINITIONS(-DGLOG_NO_ABBREVIATED_SEVERITIES)
	SET(GLOG_INCLUDE_PATH "D:\\code\\glog-master\\out\\install\\x64-Debug\\include")
	find_library(GLOG glogd "D:\\code\\glog-master\\out\\install\\x64-Debug\\lib")
	if(NOT GLOG)
        message(FATAL_ERROR "glog library not found")
	endif() 
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")
#libevent
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
	SET(LIBEVENT_INCLUDE_PATH "/usr/local/include/event2")
	find_library(LIBEVENT event "/usr/local/lib/")
	if(NOT LIBEVENT)
        message(FATAL_ERROR "libevent library not found")
	endif() 
	#find_package(libevent REQUIRED)
ELSEIF (CMAKE_SYSTEM_NAME MATCHES "Windows")
	SET(LIBEVENT_INCLUDE_PATH "D:\\git\\libevent-2.1.10-stable\\out\\install\\x64-Debug\\include")
	find_library(LIBEVENT event "D:\\git\\libevent-2.1.10-stable\\out\\install\\x64-Debug\\lib")
	if(NOT LIBEVENT)
        message(FATAL_ERROR "libevent library not found")
	endif() 
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")

#tbb
IF (CMAKE_SYSTEM_NAME MATCHES "Windows")
	SET(TBB_INCLUDE_PATH "D:\\git\\tbb\\include")
	find_library(TBB tbb_debug "D:\\git\\tbb\\lib")
	if(NOT TBB)
        message(FATAL_ERROR "tbb not found")
	endif() 
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Windows")


#mysqlclient
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
	SET(MYSQL_INCLUDE_PATH "/usr/include/mysql")
	find_library(MYSQL_CLIENT libmysqlclient.a /usr/lib64/mysql/)
	if(NOT MYSQL_CLIENT)
        message(FATAL_ERROR "libmysqlclient.a  not found")
	endif()
ELSEIF (CMAKE_SYSTEM_NAME MATCHES "Windows")
	SET(MYSQL_INCLUDE_PATH "C:\\Program Files\\mysql-8.0.17-winx64\\include")
	find_library(MYSQL_CLIENT mysqlclient "C:\\Program Files\\mysql-8.0.17-winx64\\lib")
	#find_library(MYSQL_CLIENT mysqlclient  "D:\\code\\mysql-8.0.19\\out\\install\\x64-Debug\\lib")
	if(NOT MYSQL_CLIENT)
        message(FATAL_ERROR "mysqlclient library not found")
	endif()
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")


#oracleClient
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
	SET(ORACLE_INCLUDE_PATH "thirdParty/oracle/linux/include")
	find_library(ORACLE_CLIENT NAMES ociei clntsh clntshcore nnz21 occi HINTS "thirdParty/oracle/linux/lib")
	if(NOT ORACLE_CLIENT)
	message(FATAL_ERROR "occi not found")
	endif()
	message(STATUS "pwd: ${ORACLE_CLIENT}")
ELSEIF (CMAKE_SYSTEM_NAME MATCHES "Windows")
	SET(ORACLE_INCLUDE_PATH "thirdparty/oracle/windows/include")
	find_library(ORACLE_CLIENT names oci oraocci19d "thirdParty/oracle/windows/lib/")
	if(NOT ORACLE_CLIENT)
        message(FATAL_ERROR "occi not found")
	endif()
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")

#boost
IF (CMAKE_SYSTEM_NAME MATCHES "Windows")
	SET(BOOST_INCLUDE_PATH "D:\\boost\\boost_1_73_0")
	LINK_DIRECTORIES("D:\\boost\\boost_1_73_0\\stage\\lib")
ELSEIF (CMAKE_SYSTEM_NAME MATCHES "Linux")
	SET(BOOST_INCLUDE_PATH "../boost_1_73_0")
	LINK_DIRECTORIES("../boost_1_73_0/stage/lib")
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Windows")



#util
aux_source_directory(util utilDir)
add_library(util SHARED ${utilDir})
TARGET_LINK_LIBRARIES(util ${GLOG} ${GFLAGS})
target_include_directories(util PUBLIC ${BASE_INCLUDE_PATH} ${GLOG_INCLUDE_PATH} ${GLFAG_INCLUDE_PATH})

IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
	TARGET_LINK_LIBRARIES(util pthread)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")


#global 
aux_source_directory(globalInfo globalDir)
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
	add_library(globalInfo SHARED ${globalDir})
ELSEIF (CMAKE_SYSTEM_NAME MATCHES "Windows")
	add_library(globalInfo  ${globalDir})
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")
TARGET_LINK_LIBRARIES(globalInfo util)

#thread
aux_source_directory(thread threadDir)
add_library(thread SHARED ${threadDir})
TARGET_LINK_LIBRARIES(thread ${GLOG} ${GFLAGS})
target_include_directories(thread PUBLIC ${BASE_INCLUDE_PATH} ${GLOG_INCLUDE_PATH} ${GLFAG_INCLUDE_PATH})

IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
	TARGET_LINK_LIBRARIES(thread pthread)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")

#meta
#aux_source_directory(meta metaDir)
#add_library(meta SHARED ${metaDir})
add_library(meta SHARED meta/metaData.cpp meta/charset.cpp meta/columnType.cpp)
TARGET_LINK_LIBRARIES(meta util thread sqlParser ${GLOG})

#sql parser
aux_source_directory(sqlParser sqlparserDir)
add_library(sqlParser SHARED ${sqlparserDir})
TARGET_LINK_LIBRARIES(sqlParser util ${GLOG} ${GLFAGS})
target_include_directories(sqlParser PUBLIC ${BASE_INCLUDE_PATH} ${GLOG_INCLUDE_PATH} ${GLFAG_INCLUDE_PATH})

add_library(mysqlParserFuncs SHARED sqlParser/mysqlParseFunc/mysqlSqlParserFuncs.cpp)
TARGET_LINK_LIBRARIES(mysqlParserFuncs meta ${GLOG} ${GLFAGS})
target_include_directories(mysqlParserFuncs PUBLIC ${MYSQL_INCLUDE_PATH})

#sql parser v2
aux_source_directory(sqlParserV2 sqlparserV2Dir)
add_library(sqlParserV2 SHARED ${sqlparserV2Dir})
TARGET_LINK_LIBRARIES(sqlParserV2 util thread ${GLOG} ${GLFAGS})
target_include_directories(sqlParserV2 PUBLIC ${BASE_INCLUDE_PATH} ${GLOG_INCLUDE_PATH} ${GLFAG_INCLUDE_PATH})

#memory
aux_source_directory(memory memoryDir)
add_library(memory SHARED ${memoryDir})
TARGET_LINK_LIBRARIES(memory util thread ${GLOG} ${GFLAGS})
target_include_directories(memory PUBLIC ${BASE_INCLUDE_PATH} ${GLOG_INCLUDE_PATH} ${GLFAG_INCLUDE_PATH})

#net
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
	add_library(net SHARED net/net.cpp)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")

IF (CMAKE_SYSTEM_NAME MATCHES "Windows")
	add_library(net SHARED net/net.cpp net/windowsNetServer.cpp)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Windows")

TARGET_LINK_LIBRARIES(net util thread ${GLOG} ${GFLAGS})
target_include_directories(net PUBLIC ${BASE_INCLUDE_PATH} ${GLOG_INCLUDE_PATH} ${GLFAG_INCLUDE_PATH} ${LIBEVENT_INCLUDE_PATH})

#cluster
aux_source_directory(cluster clusterDir)
add_library(cluster SHARED ${clusterDir})
TARGET_LINK_LIBRARIES(cluster util thread memory ${GLOG} ${GLFAGS} )
target_include_directories(cluster PUBLIC ${BASE_INCLUDE_PATH} ${GLOG_INCLUDE_PATH} ${GLFAG_INCLUDE_PATH} ${LIBEVENT_INCLUDE_PATH})
target_include_directories(cluster PUBLIC ${BOOST_INCLUDE_PATH})
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
        TARGET_LINK_LIBRARIES(cluster boost_coroutine)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")



#database
aux_source_directory(database databaseDir)
add_library(database SHARED ${databaseDir})
TARGET_LINK_LIBRARIES(database  util globalInfo thread memory meta ${LZ4} ${GLOG} ${GFLAGS})
target_include_directories(database PUBLIC ${BASE_INCLUDE_PATH} ${GLOG_INCLUDE_PATH} ${GLFAG_INCLUDE_PATH})

#kvdb
aux_source_directory(memKvDatabase memKvDatabaseDir)
add_library(memKvDatabase SHARED ${memKvDatabaseDir})
IF (CMAKE_SYSTEM_NAME MATCHES "Windows")
	target_include_directories(memKvDatabase PUBLIC ${TBB_INCLUDE_PATH})
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Windows")
TARGET_LINK_LIBRARIES(memKvDatabase  util globalInfo thread memory meta ${LZ4} ${GLOG} ${GFLAGS} ${TBB})
target_include_directories(memKvDatabase PUBLIC ${BASE_INCLUDE_PATH} ${GLOG_INCLUDE_PATH} ${GLFAG_INCLUDE_PATH})

#metaCollection
add_library(metaCollection SHARED meta/metaDataCollection.cpp)
TARGET_LINK_LIBRARIES(metaCollection meta database util thread sqlParser ${GLOG})
#store
aux_source_directory(store storeDir)
add_library(store SHARED ${storeDir})
TARGET_LINK_LIBRARIES(store  util globalInfo thread memory meta metaCollection database ${LZ4} ${GLOG} ${GFLAGS})
target_include_directories(store PUBLIC ${BASE_INCLUDE_PATH} ${GLOG_INCLUDE_PATH} ${GLFAG_INCLUDE_PATH})

#shell
aux_source_directory(shell shellDir)
add_library(shell SHARED ${shellDir})
TARGET_LINK_LIBRARIES(shell store util memory meta ${LZ4} ${GLOG} ${GFLAGS})
target_include_directories(shell PUBLIC ${BASE_INCLUDE_PATH} ${GLOG_INCLUDE_PATH} ${GLFAG_INCLUDE_PATH})

#datasource
aux_source_directory(dataSource dataSourceDir)
add_library(dataSource SHARED ${dataSourceDir})
TARGET_LINK_LIBRARIES(dataSource  util store meta ${GLOG} ${GFLAGS})
target_include_directories(dataSource PUBLIC ${BASE_INCLUDE_PATH} ${GLOG_INCLUDE_PATH} ${GLFAG_INCLUDE_PATH})

IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
	TARGET_LINK_LIBRARIES(dataSource dl)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")
#mysqlDataSource
include_directories(/usr/include/mysql)
aux_source_directory(dataSource/mysqlDataSource mysqlDataSourceDir)
add_library(mysqlDataSource SHARED ${mysqlDataSourceDir})
target_include_directories(mysqlDataSource PUBLIC ${BASE_INCLUDE_PATH} ${GLOG_INCLUDE_PATH} ${GLFAG_INCLUDE_PATH} ${MYSQL_INCLUDE_PATH})
TARGET_LINK_LIBRARIES(mysqlDataSource  util store meta sqlParser ${GLOG} ${MYSQL_CLIENT})
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
	TARGET_LINK_LIBRARIES(mysqlDataSource pthread ssl crypto rt z)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")

#oracleDataSource
aux_source_directory(dataSource/oracleDataSource oracleDataSourceDir)
add_library(oracleDataSource SHARED ${oracleDataSourceDir})
target_include_directories(oracleDataSource PUBLIC ${BASE_INCLUDE_PATH} ${GLOG_INCLUDE_PATH} ${GLFAG_INCLUDE_PATH})
target_include_directories(oracleDataSource PRIVATE ${ORACLE_INCLUDE_PATH})
TARGET_LINK_LIBRARIES(oracleDataSource util thread store meta ${ORACLE_CLIENT} ${GLOG} ${GFLAGS})
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
	TARGET_LINK_LIBRARIES(oracleDataSource pthread)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")



IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
	TARGET_LINK_LIBRARIES(dataSource dl)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")



#mysqlApplier
aux_source_directory(replicator/mysqlApplier mysqlApplierDir)
add_library(mysqlApplier SHARED ${mysqlApplierDir})
TARGET_LINK_LIBRARIES(mysqlApplier  util  meta  ${GLOG} ${MYSQL_CLIENT})
target_include_directories(mysqlApplier PUBLIC ${BASE_INCLUDE_PATH} ${GLOG_INCLUDE_PATH} ${GLFAG_INCLUDE_PATH} ${MYSQL_INCLUDE_PATH})

IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
	TARGET_LINK_LIBRARIES(mysqlApplier pthread ssl crypto rt z)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")
#main
ADD_EXECUTABLE(DBStream  DBStream.cpp)
TARGET_LINK_LIBRARIES(DBStream util globalInfo sqlParser dataSource store meta metaCollection memory)
target_include_directories(DBStream PUBLIC ${BASE_INCLUDE_PATH} ${GLOG_INCLUDE_PATH} ${GLFAG_INCLUDE_PATH})

IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
	TARGET_LINK_LIBRARIES(DBStream dl pthread)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")



#crypt
aux_source_directory(crypt/aes aesDir)
aux_source_directory(crypt/sha-2 sha2Dir)

add_library(crypt SHARED ${aesDir} ${sha2Dir})



#-------------------------------------------------------------------------------









#------------------------------------------------------------------------------
#unit test
ADD_EXECUTABLE(testMysqlSql  sqlParser/mysqlParseFunc/unittest/testMysqlSql.cpp)
TARGET_LINK_LIBRARIES(testMysqlSql util sqlParser)
target_include_directories(testMysqlSql PUBLIC ${BASE_INCLUDE_PATH})
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
	TARGET_LINK_LIBRARIES(testMysqlSql dl)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")

ADD_EXECUTABLE(testSqlWord  sqlParser/unittest/testSqlWord.cpp)
TARGET_LINK_LIBRARIES(testSqlWord util sqlParser)
target_include_directories(testSqlWord PUBLIC ${BASE_INCLUDE_PATH})
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
	TARGET_LINK_LIBRARIES(testSqlWord dl)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")

ADD_EXECUTABLE(testMetaDataCollection meta/unittest/testMetaDataCollection.cpp)
TARGET_LINK_LIBRARIES(testMetaDataCollection util sqlParser meta metaCollection)
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
	TARGET_LINK_LIBRARIES(testMetaDataCollection dl)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")

ADD_EXECUTABLE(testColumnType meta/unittest/testColumnType.cpp)
TARGET_LINK_LIBRARIES(testColumnType  meta )
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
        TARGET_LINK_LIBRARIES(testColumnType dl pthread)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")

ADD_EXECUTABLE(testAppendingIndex database/unittest/testAppendingIndex.cpp)
TARGET_LINK_LIBRARIES(testAppendingIndex util database meta metaCollection sqlParser)
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
	TARGET_LINK_LIBRARIES(testAppendingIndex dl)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")

ADD_EXECUTABLE(testAppendDatabase database/unittest/testAppendDatabase.cpp)
TARGET_LINK_LIBRARIES(testAppendDatabase util database meta metaCollection sqlParser)
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
	TARGET_LINK_LIBRARIES(testAppendDatabase dl)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")

ADD_EXECUTABLE(testThreadLocal  util/unittest/testThreadLocal.cpp)
TARGET_LINK_LIBRARIES(testThreadLocal util)
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
        TARGET_LINK_LIBRARIES(testThreadLocal thread)
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
target_include_directories(testDualLinkList PUBLIC ${BASE_INCLUDE_PATH})
TARGET_LINK_LIBRARIES(testDualLinkList thread)
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

ADD_EXECUTABLE(testString util/unittest/testString.cpp)
TARGET_LINK_LIBRARIES(testString util)



ADD_EXECUTABLE(testHeap util/unittest/testHeap.cpp)
TARGET_LINK_LIBRARIES(testHeap util)

ADD_EXECUTABLE(testFixedQueue util/unittest/testFixedQueue.cpp)
target_include_directories(testFixedQueue PUBLIC ${BASE_INCLUDE_PATH})
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
        TARGET_LINK_LIBRARIES(testFixedQueue pthread)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")


ADD_EXECUTABLE(testBitUtil util/unittest/testBitUtil.cpp)
TARGET_LINK_LIBRARIES(testBitUtil util)

ADD_EXECUTABLE(testItoa util/unittest/testItoa.cpp)
TARGET_LINK_LIBRARIES(testItoa util)

ADD_EXECUTABLE(testRingFixedQueue  util/unittest/testRingFixedQueue.cpp)
TARGET_LINK_LIBRARIES(testRingFixedQueue util)
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
        TARGET_LINK_LIBRARIES(testRingFixedQueue pthread)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")

ADD_EXECUTABLE(testBuddy  memory/unittest/testBuddy.cpp)
TARGET_LINK_LIBRARIES(testBuddy util memory)

ADD_EXECUTABLE(testSpinlock  thread/unittest/testSpinlock.cpp)
TARGET_LINK_LIBRARIES(testSpinlock util thread)
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
        TARGET_LINK_LIBRARIES(testSpinlock pthread)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")




ADD_EXECUTABLE(testMysqlDataCreater dataSource/mysqlDataSource/unittest/mysqlDataCreater.cpp)
target_include_directories(testMysqlDataCreater PUBLIC ${BASE_INCLUDE_PATH} ${GLOG_INCLUDE_PATH} ${GLFAG_INCLUDE_PATH} ${MYSQL_INCLUDE_PATH})
TARGET_LINK_LIBRARIES(testMysqlDataCreater  util ${GLOG} ${MYSQL_CLIENT})
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
	TARGET_LINK_LIBRARIES(testMysqlDataCreater pthread ssl crypto rt z dl)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")

ADD_EXECUTABLE(testMemChannel  cluster/unittest/testMemChannel.cpp)
TARGET_LINK_LIBRARIES(testMemChannel util thread memory)
target_include_directories(testMemChannel PUBLIC ${BOOST_INCLUDE_PATH})
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
        TARGET_LINK_LIBRARIES(testMemChannel pthread)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")

ADD_EXECUTABLE(testClusterLogFile  cluster/unittest/testClusterLogFile.cpp)
TARGET_LINK_LIBRARIES(testClusterLogFile util thread memory cluster)
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
        TARGET_LINK_LIBRARIES(testClusterLogFile pthread)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")

ADD_EXECUTABLE(testClusterLog  cluster/unittest/testClusterLog.cpp)
TARGET_LINK_LIBRARIES(testClusterLog util thread memory cluster)
target_include_directories(testClusterLog PUBLIC ${BOOST_INCLUDE_PATH})
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
        TARGET_LINK_LIBRARIES(testClusterLog pthread)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")

ADD_EXECUTABLE(testKvTable  memKvDatabase/unittest/testTable.cpp)
TARGET_LINK_LIBRARIES(testKvTable util  ${TBB} memory thread)
target_include_directories(testKvTable PUBLIC ${TBB_INCLUDE_PATH})
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
        TARGET_LINK_LIBRARIES(testKvTable pthread tbb)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")


ADD_EXECUTABLE(testSqlWordV2  sqlParserV2/unittest/testSqlWord.cpp)
TARGET_LINK_LIBRARIES(testSqlWordV2 util sqlParserV2 thread)
target_include_directories(testSqlWordV2 PUBLIC)

ADD_EXECUTABLE(testOracleConect  dataSource/oracleDataSource/unittest/testOracleConnect.cpp)
TARGET_LINK_LIBRARIES(testOracleConect util oracleDataSource ${ORACLE_CLIENT} ${GLOG})
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
	TARGET_LINK_LIBRARIES(testOracleConect pthread)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")
target_include_directories(testOracleConect PUBLIC)
target_include_directories(testOracleConect PRIVATE ${ORACLE_INCLUDE_PATH})
