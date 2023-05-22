# Release

This document is primarily targeted towards Deephaven CSV release managers.
It is meant to contain the necessary instructions for executing a Deephaven CSV release.

Please be sure to read and understand this document in full before proceeding with a release.
If you have any questions or concerns about any of the commands, please reach out.

## Jar artifacts

The deephaven-csv release process consists of releasing two jars to [Maven Central](https://repo1.maven.org/maven2/io/deephaven/).

* [io.deephaven:deephaven-csv](https://repo1.maven.org/maven2/io/deephaven/deephaven-csv/)
* [io.deephaven:deephaven-csv-fast-double-parser](https://repo1.maven.org/maven2/io/deephaven/deephaven-csv-fast-double-parser/)

## Release process

The majority of the release procedure is executed by the [publish.yml workflow](./.github/workflows/publish.yml).
It is kicked off by a push to a branch name that matches `release/v*`.
Please familiarize yourself with the steps in that workflow.

### 0. Poll Deephaven CSV team

Ensure you are proceeding with a known release, and there aren't any blockers.

### 1. Repository prerequisites

These release notes assume that the Deephaven CSV repository `git@github.com:deephaven/deephaven-csv.git` is referenced as the remote named `upstream`.
Please ensure your local repository is set up as such, or that you replace any commands with the appropriately named remote:

```shell
$ git remote get-url upstream
git@github.com:deephaven/deephaven-csv.git
```

### 2. Create release branch and commit version bump

Ensure you are up-to-date with `upstream/main`, or at the commit that you want to start a new release from.
If you are unsure what commit to start from, please ask.
Most of the time, the main branch will be referencing a `-SNAPSHOT` version, and we'll want to update this to a non-snapshot version.
Please double-check you are on the version you expect to be releasing.
The releases have so far proceeded with branches named `releave/vX.Y.Z`, where `X.Y.Z` is the version number (this isn't a technical requirement), please replace `X.Y.Z` with the appropriate version.

```shell
$ git fetch upstream
$ git checkout upstream/main
$ git checkout -b release/vX.Y.Z
$ # edit gradle.properties, remove -SNAPSHOT
$ git add gradle.properties
$ git commit -m "Bump to version X.Y.Z"
```

### 3. Push to upstream

Triple-check things look correct, the release is a "GO", and then start the release process by pushing the release branch to upstream:

```shell
$ git show release/vX.Y.Z
$ git push -u upstream release/vX.Y.Z
```

### 4. Monitor release

The release will proceed with [GitHub Actions](https://github.com/deephaven/deephaven-csv/actions/workflows/publish.yml).

### 5. Maven Central jars

The jars are put into a [Maven Central Repository Manager](https://s01.oss.sonatype.org) staging repository.
You'll need your own username and password to sign in (to ensure auditability).

If any late-breaking issues are found during the release process, but the Maven Central jars have not been released from staging, the release process can be re-done.
Once the jars are officially released from the staging repository, they are released "forever".

When ready, the staging repository will need to be "Closed" and then "Released".
Once the staging repository has been "Released", there is no going back.

The jars will be visible after release at [https://repo1.maven.org/maven2/io/deephaven/](https://repo1.maven.org/maven2/io/deephaven/).
Sometimes it takes a little bit of time for the jars to appear.

### 6. Tag upstream

The `vX.Y.Z` tag is primarily meant for an immutable reference point in the future.
It does not kick off any additional jobs.
The release should only be tagged _after_ the Maven Central staging repository has been "Released".

```shell
$ git tag vX.Y.Z release/vX.Y.Z
$ git show vX.Y.Z
$ git push upstream vX.Y.Z
```

### 7. Prepare next development iteration

The main branch is explicitly updated (instead of going through a squash and merge PR process):

```shell
$ git checkout main
$ git merge --ff-only vX.Y.Z
# edit gradle.properties, bump version, add -SNAPSHOT
# Do a find and replace for old version, replace with new version (across READMEs)
$ ...
$ git add .
$ git status -uno
$ git commit -m "Version A.B.C-SNAPSHOT"
$ git push -u upstream main
```

The release branch can be deleted locally and remotely.

### 8. GitHub release

Create a new [GitHub release](https://github.com/deephaven/deephaven-csv/releases/new) and use the `vX.Y.Z` tag as reference.

The convention is to have the Release title of the form `vX.Y.Z` and to autogenerate the release notes in comparison to the previous release tag.

Hit the GitHub "Publish release" button.
