add_executable(group group.cpp)

target_link_libraries(group ${TEST_LIBRARIES})

add_dependencies(group ${GTEST_TARGETS})

add_test(group group)
add_dependencies(run_tests group)
