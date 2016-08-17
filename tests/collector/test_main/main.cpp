#include <Logger.h>

#include <gtest/gtest.h>

int main(int argc, char **argv)
{
    app::logging::init_logger("/dev/stdout", "/dev/stdout", 0);

    app::logging::DefaultAttributes attr;

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
