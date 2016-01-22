add_executable(backend backend.cpp)

target_link_libraries(backend ${TEST_LIBRARIES})

add_dependencies(backend ${GTEST_TARGETS})

add_test(backend backend)
add_dependencies(run_tests backend)
