#include <gtest/gtest.h>

#include "common/StrUtils.h"

namespace KFS {
namespace Test {

TEST(StrUtils_T, printf) {
    EXPECT_EQ("hi", StrUtils::str_printf("%s", "hi"));
    EXPECT_EQ("hi 2 there", StrUtils::str_printf("%s %d %s", "hi", 2, "there"));
}

TEST(StrUtils_T, sprintf) {
    std::string result = "hi";
    StrUtils::str_sprintf(&result, "%s %s", "hello", "world!");

    EXPECT_EQ("hello world!", result);
}

TEST(StrUtils_T, appendf) {
    std::string result = "hello ";
    StrUtils::str_appendf(&result, "%s!", "world");

    EXPECT_EQ("hello world!", result);
}

}
} // namespace KFS
