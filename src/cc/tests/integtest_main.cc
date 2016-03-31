#include <gtest/gtest.h>
#include <iostream>

#include "tests/environments/metaserver_environment.cc"

GTEST_API_ int main(int argc, char **argv) {
    std::cout << "Running main() from tests/integtest_main" << std::endl;
    testing::InitGoogleTest(&argc, argv);
    testing::AddGlobalTestEnvironment(new KFS::Test::Metaserver);
    return RUN_ALL_TESTS();
}
