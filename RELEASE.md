# Release process

The release process starts with one or more tagged release candidates,
when no more blocking issues needs to be fixed a final tag is created
and the final release is rolled.

confluent-kafka-dotnet uses semver versioning and loosely follows
librdkafka's version, e.g. v0.11.x for the final release and
v0.11.x-RC3 for the 3rd v0.11.x release candidate.

With the addition of prebuilt binary packages we make use of travis-ci.org.
These artifacts are downloaded from artifacts url of each ci build (for 
example: https://ci.appveyor.com/project/ConfluentClientEngineering/confluent-kafka-dotnet/builds/39236976/artifacts),
and then uploaded manually to https://www.nuget.org/.

**Note**: Dotnet package versions use the same tag with librdkafka.
          That is to say that while the librdkafka RC is named `v0.11.x-RC3`,
          a dotnet client RC with the same version is also named `v0.11.x-RC3`.


The following guide uses `v0.11.x-RC1` as version for a release candidate,
replace as necessary with your version or remove `-RC..` suffix for the
final release.

To give you an idea what this process looks like we have this fabricated
time line:

 * Time to prepare for a release.
 * A librdkafka release candidate is available.
 * Check if any open PRs needs merging, do that.
 * Feature freeze - don't merge any more PRs.
 * Create a release candidate branch.
 * Update versions, dependencies (librdkafka), etc.
 * Create test tag to trigger CI builds, test.
 * Work out any release build issues.
 * librdkafka is released, update librdkafka version.
 * Create release candidate tag, wait for CI builds, deploy to nuget.org.
 * Create PR, get it reviewed.
 * When it is time for final release, merge the PR.
 * On master, create release tag, wait for CI builds, deploy to nuget.org.
 * Update confluent.io docs.
 * Announce release.


## FEATURE FREEZE

During the release process no PRs must be merged, unless they're fixing
something that needs to go into the release.
If that's the case the rc branch needs to be rebased on latest master,
or master merged into it, to pick up the new changes.


## 1. Create a RC branch

Make sure your local git clone is up to date and clean:

    $ git checkout master
    $ git pull --rebase --tags origin master

Create an RC branch: If there isn't yet a {MAJOR}.{MINOR}.x branch
corresponding to the release (e.g. 1.7.x), create one from master.

    $ git checkout -b 1.7.x


## 2. Update CHANGELOG.md

Make sure the top of [CHANGELOG.md](../CHANGELOG.md) has a section
for the upcoming release. See previous releases in CHANGELOG.md for how
to structure and format the release notes.

For our fix and enhancement PRs we typically write the corresponding changelog
entry as part of that PR, but for PRs that have been submitted from the
community we typically need to add changelog entries afterwards.

To find all changes since the last release, do:

    $ git log v0.11.3..    # the last release

Go through the printed git history and identify and changes that are not
already in the changelog and that are relevant to the end-users of the client.
For example, a bug fix for a bug that users would be seeing should have a
corresponding changelog entry, but a fix to the build system is typically not
interesting and can be skipped.

For commits provided by the community we need to provide proper attribution
by looking up the GitHub username for each such change and then mention
the user from the changelog entry, e.g.:

` * Added OAuth support to AdminClient ([someghuser](https://github.com/someghuser))`

The changelog must also say which librdkafka version is bundled, a link to the
librdkafka release notes, and should also mention some of the most important
changes to librdkafka in that release.

*Note*: `CHANGELOG.md` is in MarkDown format.


## 3. Download librdkafka config file

    $ cd src/ConfigGen
    $ dotnet run v0.11.x-RC1
    $ mv out/Config_gen.cs ../Confluent.Kafka


## 4. Update librdkafka version

Search / replace to the old/new version number in:
    - C# comments
    - The change log
    - The reademe
    - csproj files (librdkafka.redist version and .net library versions)

*Note*: Make sure librdkafka.redist is use the latest version.


## 5. Run tests

*Note*: Make sure .net core 2.1 installed.  

    - Confluent.Kafka.IntegrationTests
    - Confluent.Kafka.UnitTests
    - Confluent.SchemaRegistry.IntegrationTests
    - Confluent.SchemaRegistry.Serdes.IntegrationTests
    - Confluent.SchemaRegistry.UnitTests
    - Confluent.SchemaRegistry.Serdes.UnitTests

Cd to the above folders and run:
    $ dotnet test -f netcoreapp2.1

Start a single broker Kafka cluster on localhost and an
instance of Schema Registry (default configs are fine). There are some
tests for auth, and a docker file which is helpful for that, but verification
for these is usually not needed.


## 6. Tag, CI build, wheel verification, upload

The following sections are performed in multiple iterations:

 - **TEST ITERATION** - a number of test/release-candidate iterations where
   test tags are used to trigger CI builds. As many of these as required
   to get fully functional and verified.
 - **CANDIDATE ITERATION** - when the builds test reliable, a release candidate
   RC tag is pushed. The resulting artifacts should be uploaded to nuget.org.
 - **RELEASE ITERATION** - after all the update and tests looks good, a release
   tag is created on rc branch. The resulting artifacts are uploaded to
   nuget.org. Merge the rc branch to master.


Repeat the following chapters' TEST ITERATIONs, then submit a PR, get it
reviewed by team mates, perform one or more CANDIDATE ITERATION, and then do a
final RELEASE ITERATION.


### 6.1. Create a tag

Packaging is fragile and is only triggered when a tag is pushed. To avoid
finding out about packaging problems on the RC tag, it is strongly recommended
to first push a test tag to trigger the packaging builds. This tag should
be removed after the build passes.

**Note**: Dotnet package versions use the same tag with librdkafka.
          That is to say that while the librdkafka RC is named `v0.11.x-RC3`,
          a Python client RC with the same version is also named `v0.11.x-RC3`.

**TEST ITERATION**:

    # Repeat with new tags until all build issues are solved.
    $ git tag v0.11.x-RC1-test2

    # Delete any previous test tag you've created.
    $ git tag -d v0.11.x-RC1-test1


**CANDIDATE ITERATION**:

    $ git tag v0.11.x-RC1


**RELEASE ITERATION**:

    $ git tag v0.11.x


### 6.2. Push tag and commits

Perform a dry-run push first to make sure the correct branch and only our tag
is pushed.

    $ git push --dry-run --tags origin v0.11.x  # tags and branch

An alternative is to push branch and tags separately:

    $ git push --dry-run origin v0.11.x  # the branch
    $ git push --dry-run origin v0.11.x-RC1-test1  # the tag


Verify that the output corresponds to what you actually wanted to push;
the correct branch and tag, etc.

Remove `--dry-run` when you're happy with the results.


### 6.3. Wait for CI builds to complete

Monitor travis-ci builds by looking at the *tag* build at
[travis-ci](https://ci.appveyor.com/project/ConfluentClientEngineering/confluent-kafka-dotnet/history)

CI jobs are flaky and may fail temporarily. If you see a temporary build error,
e.g., a timeout, restart the specific job.

If there are permanent errors, fix them and then go back to 6.1. to create
and push a new test tag. Don't forget to delete your previous test tag.


### 6.4. Download build artifacts 

When all CI builds are successful it is time to download the resulting
artifacts from artifacts url under the travis-ci job. 


### 6.5.1. TEST ITERATION: Tidy up and prepare for RC

When all things are looking good it is time to clean up
the git history to look tidy, remove any test tags, and then go back to
6.1 and perform the CANDIDATE ITERATION.


### 6.5.2. CANDIDATE ITERATION: Create PR

Once all test and RC builds are successful and have been verified and you're
ready to go ahead with the release, it is time create a PR to have the
release changes reviewed.

If librdkafka has had a new RC or the final release it is time to update
the librdkafka versions now. Follow the steps in chapter 3.

Do not squash/rebase the RC branch since that would invalidate the RC tags,
so try to keep the commit history tidy from the start in the RC branch.

Create a PR for the RC branch and add team mates as reviewers and wait for
review approval.

Once the PR has been approved and there are no further changes for the release,
go back to 6.1 and start the final RELEASE ITERATION.


### 6.5.3. RELEASE ITERATION: Merge PR

Once the PR has been approved and there are no further changes for the release,
create the final release tag, push it to the rc branch and merge the PR to
master.

Make sure to **Merge** the PR - don't squash, don't rebase - we need the commit
history to stay intact for the RC tags to stay relevant.

With the PR merged to master, check out and update master:

    $ git checkout master
    $ git pull --rebase --tags origin master

    # Make sure your master is clean and identical to origin/master
    $ git status


### 6.6. Upload packages to nuget.org

Upload binary packages to nuget.org with confluent org account.


## 7. RELEASE ITERATION: Create github release

When the final release is tagged, built and uploaded, go to
[github releases](https://github.com/confluentinc/confluent-kafka-dotnet/releases/)
and create a new release with the same name as the final release tag (`v0.11.x`).

Copy the section for this release from `CHANGELOG.md` to the GitHub release.
Use Preview to check that links work as expected.

Create the release.


### 7.1. Announcement

Write a tweet to announce the new release, something like:

    #Apache #Kafka #Dotnet client confluent-kafka-dotnet v0.11.x released!
    Adds support for <mainline feature> or something about maintenance release.
    <link-to-release-notes-on-github>


### 7.2. Update docs.confluent.io API docs

Create a PR to update the confluent-kafka-dotnet version tag for the
dotnet API docs on docs.confluent.io for the 2 docs repo.

confluentinc/docs.git
confluentinc/docs-platform.git


### 7.3. Done!

That's it, back to the coal mine.
