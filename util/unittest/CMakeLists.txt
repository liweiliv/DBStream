ADD_EXECUTABLE(testLinkList  testLinkList.cpp)
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
        TARGET_LINK_LIBRARIES(testLinkList pthread)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")

ADD_EXECUTABLE(testDualLinkList  testDualLinkList.cpp)
target_include_directories(testDualLinkList PUBLIC ${BASE_INCLUDE_PATH})
TARGET_LINK_LIBRARIES(testDualLinkList thread)
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
        TARGET_LINK_LIBRARIES(testDualLinkList pthread)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")

ADD_EXECUTABLE(testConfig  testConfig.cpp)
TARGET_LINK_LIBRARIES(testConfig util)
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
        TARGET_LINK_LIBRARIES(testConfig pthread)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")

ADD_EXECUTABLE(testTrieTree  testTrieTree.cpp)
TARGET_LINK_LIBRARIES(testTrieTree util)

ADD_EXECUTABLE(testSparsepp testSparsepp.cpp)

ADD_EXECUTABLE(testString testString.cpp)
TARGET_LINK_LIBRARIES(testString util)



ADD_EXECUTABLE(testHeap testHeap.cpp)
TARGET_LINK_LIBRARIES(testHeap util)

ADD_EXECUTABLE(testFixedQueue testFixedQueue.cpp)
target_include_directories(testFixedQueue PUBLIC ${BASE_INCLUDE_PATH})
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
        TARGET_LINK_LIBRARIES(testFixedQueue pthread)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")


ADD_EXECUTABLE(testBitUtil testBitUtil.cpp)
TARGET_LINK_LIBRARIES(testBitUtil util)

ADD_EXECUTABLE(testItoa testItoa.cpp)
TARGET_LINK_LIBRARIES(testItoa util)

ADD_EXECUTABLE(testRingFixedQueue  testRingFixedQueue.cpp)
TARGET_LINK_LIBRARIES(testRingFixedQueue util)
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
        TARGET_LINK_LIBRARIES(testRingFixedQueue pthread)
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")