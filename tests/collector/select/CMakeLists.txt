add_executable(select select.cpp)

target_link_libraries(select ${TEST_LIBRARIES})

add_dependencies(select ${GTEST_TARGETS})

add_test(select select)
add_dependencies(run_tests select)
