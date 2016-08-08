add_executable(history history.cpp)

target_link_libraries(history ${TEST_LIBRARIES})

add_dependencies(history ${GTEST_TARGETS})

add_test(history history)
add_dependencies(run_tests history)
