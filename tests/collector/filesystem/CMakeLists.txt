add_executable(filesystem filesystem.cpp)

target_link_libraries(filesystem ${TEST_LIBRARIES})

add_dependencies(filesystem ${GTEST_TARGETS})

add_test(filesystem filesystem)
add_dependencies(run_tests filesystem)

