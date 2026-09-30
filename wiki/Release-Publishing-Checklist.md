# Release Publishing Checklist

This checklist describes the steps used to publish a QFS release, based on the
2.2.x releases (2.2.8 in particular). In the examples below `X.Y.Z` is the new
release version, and `P.Q.R` is the previous release version.

## How release versions and binaries are produced

- A release is a git tag named `X.Y.Z` on the `master` branch. Release tags are
  lightweight tags, typically placed on the last commit of the release
  preparation (usually a wiki / release notes commit).
- `src/cc/common/buildversgit.sh --release` reports the tag name when the
  checked out commit matches a tag, `master` when it matches the head of the
  local `master` branch, and the short commit hash otherwise. The build embeds
  this version, and `make tarball` uses it in the tarball name:
  `qfs-<os flavor>-<version>-<arch>.tgz`, for example
  `qfs-debian-13-2.2.8-x86_64.tgz`.
- `qfs_no_git_version` in `src/cc/common/buildversgit.sh` is used only when
  git is not available, for example when building from a source archive. It
  must be updated to the new release version.
- The GitHub Actions `Build` workflow (`.github/workflows/build.yml`) runs on
  every push. For pushes to `master` and to tags it uploads release build
  tarballs to the `quantcast-qfs` S3 bucket. Pushing the `X.Y.Z` tag therefore
  produces the `X.Y.Z` binaries referenced by the
  [[Binary Distributions|Binary-Distributions]] page.
- The GitHub wiki is not edited directly: it mirrors the `wiki` directory of
  this repository (see `wiki/README`).

## 1. Pre-release checks

- [ ] All changes intended for the release are merged into `master`.
- [ ] The latest `Build` workflow run on `master` succeeded for all jobs:
      `rat`, all Linux docker builds (including the Kerberos test job), and
      both macOS builds (x86_64 and arm64).
- [ ] `make rat clean` passes locally, or in the CI `rat` job.
- [ ] Decide on platform support changes for the release: add builds for new
      distributions and architectures, and remove end of life ones, in
      `.github/workflows/build.yml` and `travis/script.sh`. Update the build
      status badges in `README.md` accordingly.
- [ ] Optionally update build tool versions used by the build scripts, for
      example the Apache Maven download URL / version.

## 2. Release preparation commit

Prior releases used a single "Prepare for X.Y.Z release" commit (for example
`bbb3c844` for 2.2.8), optionally followed by separate README and wiki commits.

- [ ] `src/cc/common/buildversgit.sh`: set `qfs_no_git_version="X.Y.Z"`.
- [ ] Create `wiki/Release-Notes-X.Y.Z.md` with detailed release notes. Use the
      commit history since the previous release as the source:

      ```sh
      git log --no-merges --format='%h %s' P.Q.R..master
      ```

      Group changes by component (meta server, chunk server, client library,
      Java / Python / Go bindings, build system, platform support), and list
      added and removed platforms.
- [ ] `wiki/Release-Notes.md`: add a `## [[QFS version X.Y.Z|Release-Notes-X.Y.Z]]`
      section at the top with release highlights, key components updated, and
      platform changes. Make sure that the page link resolves: the page name
      after `|` must match the file name without the `.md` extension.
- [ ] `README.md`: add a `Release X.Y.Z` section at the top with a one line
      summary and a link to the
      [release notes](https://github.com/quantcast/qfs/wiki/Release-Notes).
- [ ] `wiki/Binary-Distributions.md`:
  - [ ] Add a `[X.Y.Z][X.Y.Z]` row below the `master` row. Copy the `master`
        row and replace `master` with `X.Y.Z`; leave cells empty for platforms
        not built for the release, and use `End of life` where appropriate.
  - [ ] Add the `[X.Y.Z]: https://github.com/quantcast/qfs/tree/X.Y.Z` link
        definition.
  - [ ] Add the `[X.Y.Z,<platform>]` link definitions, one per platform, for
        example:

        ```text
        [X.Y.Z,debian13]: https://s3.amazonaws.com/quantcast-qfs/qfs-debian-13-X.Y.Z-x86_64.tgz
        ```

        The file names must match the tarball names that CI builds and uploads
        (see `make tarball` in the top level `Makefile`). The `master`
        tarball names for the same platforms are a good reference.
  - [ ] For newly added platforms, add the columns, and the `master` and
        `X.Y.Z` links.
- [ ] Push the changes to `master` and wait for the `Build` workflow to
      succeed.

## 3. Tag the release

- [ ] Make sure the local `master` is up to date with `origin/master`, and that
      the commit to be tagged has a successful `Build` workflow run.
- [ ] Create and push the tag:

      ```sh
      git checkout master
      git pull
      git tag X.Y.Z
      git push origin X.Y.Z
      ```

- [ ] Wait for the `Build` workflow run for the `X.Y.Z` tag to complete
      successfully. This run builds and uploads the release tarballs.
- [ ] Verify that every `X.Y.Z` download link on the
      [[Binary Distributions|Binary-Distributions]] page exists, for example:

      ```sh
      sed -n 's/^\[X\.Y\.Z,[^]]*\]: *//p' wiki/Binary-Distributions.md |
          while read url; do
              printf '%s %s\n' \
                  "$(curl -sIo /dev/null -w '%{http_code}' "$url")" "$url"
          done
      ```

      All links should report `200`. Fix the links, or the build for the
      missing platforms, before announcing the release.

## 4. Publish the GitHub release

- [ ] Create a GitHub release from the `X.Y.Z` tag with the title `X.Y.Z`, and
      mark it as the latest release. Prior releases used the following
      description:

      ```text
      Release X.Y.Z contains <summary of the release>.
      Release notes:
      https://github.com/quantcast/qfs/wiki/Release-Notes
      Binaries are available at:
      https://github.com/quantcast/qfs/wiki/Binary-Distributions
      ```

      With the GitHub CLI:

      ```sh
      gh release create X.Y.Z --repo quantcast/qfs --title X.Y.Z \
          --latest --notes-file release-notes.txt
      ```

## 5. Update the GitHub wiki

- [ ] In a clone of the GitHub wiki repository
      (`git@github.com:quantcast/qfs.wiki.git`) that includes the subtree merge
      of the `wiki` directory, run `make`. This pulls the `wiki` directory of
      `master` and pushes it to the GitHub wiki (see `wiki/makefile`).
- [ ] Check the published [Release Notes][release notes],
      Release-Notes-X.Y.Z, and [Binary Distributions][binary distributions]
      pages, and verify that the links between them work.
- [ ] Wiki only fixes after the tag (for example `62866cf5`, which fixed the
      2.2.8 release notes link) do not require a new tag or new binaries:
      commit to `master` and repeat this step.

![Quantcast](//pixel.quantserve.com/pixel/p-9fYuixa7g_Hm2.gif?labels=opensource.qfs.wiki)

[release notes]: https://github.com/quantcast/qfs/wiki/Release-Notes
[binary distributions]: https://github.com/quantcast/qfs/wiki/Binary-Distributions
