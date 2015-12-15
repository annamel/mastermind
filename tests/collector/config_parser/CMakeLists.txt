add_executable(config_parser config_parser.cpp)

target_link_libraries(config_parser ${TEST_LIBRARIES})

add_dependencies(config_parser ${GTEST_TARGETS})

add_test(config_parser config_parser)
add_dependencies(run_tests config_parser)
