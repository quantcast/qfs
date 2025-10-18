# Code Contribution policy

Thank you for your interest in contributing to qfs! We welcome code
contributions to qfs. The following is a set of guidelines and suggestions to
make it easier for you to contribute to qfs.Please make them in the form of
 from your fork, and bear in mind the following guidelines
and suggestions.

## Before Starting Work

Before starting your work, make sure to check our [issue tracker][it] for
tickets regarding your suggested change. It's possible someone could have
already reported your bug or suggestion and the community has already discussed
it in detail.

If your code change is complex, please consider communicating with QFS community
at <qfs-devel@googlegroups.com> to get feedback before submitting the pull
request. This will save you time and effort in case there are design discussions
to be had.

## Submitting Changes

Please submit code changes as [pull requests][pr]. A separate pull request
should be submitted for each separate issue. In addition, for each commit you
make, ensure the following:

- Each commit deals with only a single idea.
- Commit messages have a summary line that begins with its scope (eg: `fuse:
    fix compiler warning`). Depending on the complexity of the change, a
    detailed description of the commit should follow in the next lines.
- If you have multiple small commits, consider [rebasing][rebase] to
    consolidate them into a single commit.

Furthermore, when making your pull request, ensure the following:

- Your change is based on the current master branch (rebase if necessary).
- The code builds properly and tests all pass
- You have added appropriate unit and integration tests for your
    changes/fixes. See the [[Developer Documentation]] for more information on
    writing unit and integration tests.
- Code changes are well documented and readable. You should update the
    documentation within the wiki directory if necessary.
- Your changes conform to the style guide listed below.
- In the pull request comments, please describe the code change and any tests
    you’ve done.

Pull requests will be reviewed and accepted (or responded to) by Quantcast as
soon as possible.

## Style Guide

QFS is written in C++. We try to follow the style guide below as much as
possible. QFS has evolved over time, so there may be existing source files that
do not adhere to this style guide, but all new code follows it and we're in the
process of cleaning up the old files. If you find one of these old files, please
feel free to submit a [pull request][pr] to fix it.

Any new C++ code added should conform to the style and rules outlined below.

**Golden Rule**: functions, variables, and file names should be descriptive.

### General Guidelines

- Files should have line lengths of 80 characters.
- No trailing white spaces.
- No tabs; use spaces. Indent 4 spaces at a time.
- Header files must have a `#ifndef #define` guard.
- Function parameter ordering should be input, output.
- Place function variables in the narrowest scope possible.
- Parameters passed by reference must be const. Use pointers for output
  arguments.
- Do not perform complex initialization in constructors; instead use an Init
  method.
- Use the `explicit` keyword for constructors with only one argument.
- Do explicit initialization of all member variables. Define a default
  constructor if there is no constructor.
- Minimize the use of function overloading, default function arguments, friend
  classes, and friend functions.
- Use C++ casts (eg: `static_cast<>()`) and `const`ing whenever it makes sense.
- Be 64-bit and 32-bit friendly.
- Prefer inline functions to macros.
- Use `sizeof(variableName)` instead of `sizeof(typename)`.
- Be consistent with `//` or `/* */` commenting style.
- Each file should have a description of the file at the top.
- Each class and function should have a comment saying its purpose.
- Variable names should be self descriptive.
- Function declaration: return type and function name on different lines.
- Classes should have public, protected, and private sections in that order.

### Typedefs and Class Names

- Typedefs: Camel case (eg: `MyTimeType`)
- Class/Struct names: Camel case (eg: `class ChunkServer`)

### File names

- Class-implementation files: UpperCase  (eg: `KfsClient.cc/h`)
- Non-class-implementation files, with main: lower\_case (eg: `server_main.cc`)
- Non-class-implementation files, w/o main: lower_case (eg: `kfsops.cc/h`)

### Methods

- Global: Please do not add global methods.
- Namespace Scope: Camel case (eg: `GetSampleSeq()`)
- Class method: Camel case (eg: `GetClassName()`)

### Variables

- Local variables: Use camel case, e.g. `camelCase`
- Class member: Use camel case with an `m` prefix, e.g. `mMemberName`
- Static variable: Use camel case with an `s` prefix, e.g. `sReaderCount`
- Const variable: Use camel case with a `k` prefix, e.g. `kPrevRef`

![Quantcast](//pixel.quantserve.com/pixel/p-9fYuixa7g_Hm2.gif?labels=opensource.qfs.wiki)

[it]: https://quantcast.atlassian.net
[pr]: https://help.github.com/articles/using-pull-requests
[rebase]: https://help.github.com/articles/interactive-rebase
