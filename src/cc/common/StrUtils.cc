#include "common/StrUtils.h"

#include <stdio.h>
#include <stdlib.h>

namespace KFS {

void
StrUtils::str_printf_helper(string* dest, const char* format,
        va_list args)
{
    char* ptr = NULL;
    int size = vasprintf(&ptr, format, args);

    if (size == -1) {
        return;
    }

    dest->append(ptr, size);
    free(ptr);
}

string
StrUtils::str_printf(const char* format, ...)
{
    va_list args;
    va_start(args, format);

    string result;
    str_printf_helper(&result, format, args);

    va_end(args);
    return result;
}

void
StrUtils::str_sprintf(string* dest, const char* format, ...)
{
    va_list args;
    va_start(args, format);

    dest->clear();
    str_printf_helper(dest, format, args);

    va_end(args);
}

void
StrUtils::str_appendf(string* dest, const char* format, ...)
{
    va_list args;
    va_start(args, format);

    str_printf_helper(dest, format, args);

    va_end(args);
}

} // namespace KFS
