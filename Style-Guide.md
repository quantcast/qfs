QFS is written in C++. We try to follow the style guide below as much as
possible. QFS has evolved over time, so there may be existing source files that
do not adhere to this style guide, but all new code follows it and we're in the
process of cleaning up the old files.

Any new C++ code added should conform to the style and rules outlined below.

**Golden Rule**: functions, variables, and file names should be descriptive.

Typedefs and Class Names
------------------------
- Typedefs : Upper case (eg: `MyTimeType`)
- Class/Struct names : Upper case (eg: `class ChunkServer`)

File names
----------
- Class-implementation files : UpperCase  (eg: `KfsClient.cc/h`)
- Non-class-implementation files, with main: lower\_case (eg: `server_main.cc`)
- Non-class-implementation files, w/o main : lower_case (eg: `kfsops.cc/h`)

Methods
-------
- Global          : Please do not add global methods.
- Namespace Scope : Upper case (eg: `GetSampleSeq()`)
- Class method    : Upper case (eg: `GetClassName()`)

Variables
---------
- Local variables : `camelCase`
- Class member    : `mMemberName`
- Static variable : `sReaderCount`
- Const variable  : `kPrevRef`

General guidlines
-----------------
- Be consistent with 80-char line length (or not) within a file.
- No trailing white spaces.
- No tabs; use spaces. indent 4 spaces at a time.
- Header files must have a `#ifndef #define` guard.
- Function parameter ordering should be (input, output).
- Place function variables in the narrowest scope possible.
- Parameters passed by reference must be const. use pointers for output arguments.
- Do not perform complex initialization in constructors; use an Init method.
- Use the `explicit` keyword for constructors with one argument.
- Do explicit initialization of all member variables. define a default constructor if there is no constructor.
- Minimize the use of function overloading, default function arguments, friend classes, and friend functions.
- Use C++ casts (eg: `static_cast<>()`) and `const`ing whenever it makes sense.
- Be 64-bit and 32-bit friendly.
- Prefer inline functions to macros.
- Use `sizeof(variableName)` instead of `sizeof(typename)`.
- Be consistent with `//` or `/* */` commenting style.
- Each file SHOULD have a description of the file at the top.
- Each class and function SHOULD have a comment saying its purpose.
- Variable names should be self descriptive.
- Function declaration: return type and function name on different lines.
- Classes SHOULD have public, protected, and private sections in that order.

![Quantcast](//pixel.quantserve.com/pixel/p-9fYuixa7g_Hm2.gif?labels=opensource.qfs.wiki)
