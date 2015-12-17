add_executable(filter_parser filter_parser.cpp)

target_link_libraries(filter_parser ${TEST_LIBRARIES})

add_dependencies(filter_parser ${GTEST_TARGETS})

add_test(filter_parser filter_parser)
add_dependencies(run_tests config_parser)
