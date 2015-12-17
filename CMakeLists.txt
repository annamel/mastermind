cmake_minimum_required(VERSION 2.6)
project(mastermind)

add_definitions("-D_GNU_SOURCE -std=c++0x -Wall -O2 -g")

find_package(Boost REQUIRED system program_options)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake")

find_package(elliptics REQUIRED)
find_package(CocaineNative REQUIRED)
find_package(Cocaine REQUIRED)
find_package(RapidJSON REQUIRED)
find_package(CURL REQUIRED)

include(cmake/locate_library.cmake)
locate_library(MSGPACK "msgpack.hpp" "msgpack")
locate_library(MONGOCLIENT "mongo/version.h" "mongoclient")

include_directories(${elliptics_INCLUDE_DIRS} ${CocaineNative_INCLUDE_DIRS}
    ${RAPIDJSON_INCLUDE_DIRS} ${CURL_INCLUDE_DIRS})

set(COLLECTOR_LIBRARIES
    ${elliptics_cpp_LIBRARY}
    ${elliptics_client_LIBRARY}
    ${CocaineNative_LIBRARIES}
    ${Cocaine_LIBRARY}
    ${CURL_LIBRARIES}
    boost_system
    boost_thread
    msgpack
    rt
    dispatch
    pthread_workqueue
    kqueue
    ev
    mongoclient
    )

add_subdirectory(src/collector)

enable_testing()
add_subdirectory(tests/collector)

install(FILES ${CMAKE_BINARY_DIR}/src/collector/mastermind-collector.tar.gz
    src/collector/mastermind-collector.manifest
    src/collector/mastermind-collector.profile
    DESTINATION lib/mastermind/cocaine-app/)
