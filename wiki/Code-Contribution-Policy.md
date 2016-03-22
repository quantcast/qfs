We welcome code contributions. Please make them in the form of [pull
requests](https://help.github.com/articles/using-pull-requests) from your fork,
and bear in mind the following guidelines:

- If your code change is complex, please consider communicating with QFS project
  maintainer at **qfs@quantcast.com** to get feedback before submitting the pull
  request.
- Please submit separate pull requests for separate issues.
- Typically you would create a branch per issue, with a branch name like
  `iss42_python_build_fix` where 42 is the issue number and
  python_build_fix is a short, delimited description.

- When committing code, please make sure:
  - Each commit deals with one type of change.
  - Commit messages have a summary line that begins with its scope (eg: `fuse:
    fix compiler warning`). Depending on the complexity of the change,
    detailed description of the commit could follow in the next lines.
  - Commit messages include author information.
  - If you have multiple small commits, consider
  [rebasing](https://help.github.com/articles/interactive-rebase) to consolidate
  them into a single commit.

- Before making the pull request, please ensure that:
  - Your change is based on the current state of the QFS tree (rebase if
    necessary).
  - Builds still run and tests still pass.
  - It includes appropriate unit tests for your changes/fixes.
  - Code changes are well documented and readable.
  - Your changes conform to the
    [[QFS coding style|Style-Guide]]
- In the pull request comments, please describe the code change and any tests
  you’ve done.
- Pull requests will be reviewed and accepted (or responded to) by Quantcast as
  soon as possible.

![Quantcast](//pixel.quantserve.com/pixel/p-9fYuixa7g_Hm2.gif?labels=opensource.qfs.wiki)
