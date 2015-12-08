add_executable(stats_parser stats_parser.cpp)

target_link_libraries(stats_parser ${TEST_LIBRARIES})

add_dependencies(stats_parser ${GTEST_TARGETS})

add_test(stats_parser stats_parser)
add_dependencies(run_tests stats_parser)
