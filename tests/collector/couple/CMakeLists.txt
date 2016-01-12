add_executable(couple couple.cpp)

target_link_libraries(couple ${TEST_LIBRARIES})

add_dependencies(couple ${GTEST_TARGETS})

add_test(couple couple)
add_dependencies(run_tests couple)

