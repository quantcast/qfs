#ifndef COMMON_STRINGUTILS
#define COMMON_STRINGUTILS

#include <string>
#include <stdarg.h>

namespace KFS {

using namespace std;

class StrUtils
{
public:
    /**
     * str_printf is analogous to printf(3). It takes a format string and a list
     * of arguments as parameters. It formats the string and returns it as an
     * std::string. If there is an error in formatting the string, an empty
     * string is returned.
     */
    static string str_printf(const char* format, ...);

    /**
     * str_sprintf is analogous to sprintf(3). It takes a destination string, a
     * format string, and a list of arguments as parameters. It formats the
     * string and returns it as an std::string.
     */
    static void str_sprintf(string* dest, const char* format, ...);

    /**
     * str_appendf is analogous to sprintf(3). It takes a destination string, a
     * format string, and a list of arguments as parameters. It formats the
     * string and appends the result to the destination std::string.
     */
    static void str_appendf(string* dest, const char* format, ...);

private:
    static void str_printf_helper(string* dest, const char* format,
            va_list args);
};

} // namespace KFS

#endif
