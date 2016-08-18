cmake_minimum_required(VERSION 2.8.8)
project(mastermind)

add_definitions("-D_GNU_SOURCE -std=c++0x -Wall -O2 -g")

find_package(Boost REQUIRED system program_options)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake")

find_package(Threads)
find_package(elliptics REQUIRED)
find_package(CocaineNative REQUIRED)
find_package(Cocaine REQUIRED)
find_package(RapidJSON REQUIRED)
find_package(CURL REQUIRED)

include(cmake/locate_library.cmake)
locate_library(MSGPACK "msgpack.hpp" "msgpack")
locate_library(MONGOCLIENT "mongo/version.h" "mongoclient")
locate_library(BLACKHOLE "blackhole/logger.hpp" "blackhole")

include_directories(${elliptics_INCLUDE_DIRS} ${CocaineNative_INCLUDE_DIRS}
    ${RAPIDJSON_INCLUDE_DIRS} ${CURL_INCLUDE_DIRS})

set(COLLECTOR_LIBRARIES
    ${CMAKE_THREAD_LIBS_INIT}
    ${elliptics_client_LIBRARY}
    ${elliptics_cpp_LIBRARY}
    ${CocaineNative_LIBRARIES}
    ${Cocaine_LIBRARIES}
    ${CURL_LIBRARIES}
    ${BLACKHOLE_LIBRARIES}
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
