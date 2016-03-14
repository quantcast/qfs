#ifndef TESTS_INTEGTEST
#define TESTS_INTEGTEST

#include <gtest/gtest.h>

namespace KFS {
namespace Test {

class QFSTestEnvironment : public ::testing::Environment
{
public:
    virtual void SetUp();
    virtual void TearDown();

protected:
};

} // namespace Test
} // namespace KFS

#endif
