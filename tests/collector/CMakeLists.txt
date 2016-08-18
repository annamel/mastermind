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

# Find all files test_<name>.cpp and add target <name>
file(GLOB_RECURSE TEST_SOURCE_FILES RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "test_*.cpp")

foreach(TEST_SOURCE_FILE IN LISTS TEST_SOURCE_FILES)
    string(REGEX REPLACE ".*test_(.*)[.]cpp$" "\\1" TEST_EXECUTABLE ${TEST_SOURCE_FILE})
    add_executable(${TEST_EXECUTABLE} EXCLUDE_FROM_ALL
        ${CMAKE_CURRENT_SOURCE_DIR}/${TEST_SOURCE_FILE})
    target_link_libraries(${TEST_EXECUTABLE} ${TEST_LIBRARIES})
    add_test(${TEST_EXECUTABLE} ${TEST_EXECUTABLE})
    add_dependencies(run_tests ${TEST_EXECUTABLE})
endforeach()
