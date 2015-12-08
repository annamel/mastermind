find_package(Threads REQUIRED)
include_directories(${Threads_INCLUDE_DIR})

add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/googletest/googletest EXCLUDE_FROM_ALL)

include_directories(${GTEST_INCLUDE_DIRS})
set(GTEST_TARGETS gtest gtest_main)

set(GTEST_BOTH_LIBRARIES gtest gtest_main)

# Show test printouts
set(CMAKE_CTEST_COMMAND ctest --output-on-failure)
add_custom_target(run_tests ${CMAKE_CTEST_COMMAND})

include_directories(
    ${gtest_SOURCE_DIR}/include
    ${gtest_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}
    ${mastermind_SOURCE_DIR}/src/collector
    util)

add_subdirectory(util)

set(TEST_LIBRARIES
    ${CMAKE_THREAD_LIBS_INIT}
    ${GTEST_BOTH_LIBRARIES}
    collector_common
    util)

add_subdirectory(config_parser)
add_subdirectory(filter_parser)
add_subdirectory(stats_parser)
