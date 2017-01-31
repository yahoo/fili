How to Cut a Stable Release
===========================

While Fili development generally happens in a roll-forward kind of way, we need to periodically draw a line in the sand
and mark groups of changes as "stable" and supported. Typically, it's a good idea to do this just before large,
codebase-wide refactorings or changes are about to happen. To make it easier to do, since more frequent releases make
upgrading hurt less, here are the general steps in the process.

Clean up Stable Release Changelog
---------------------------------

As development happens for Fili, changes build up in the development line, and descriptions of those changes build up in
the "current" section of the [CHANGELOG](/CHANGELOG.md). As part of declaring the release stable, the changelog needs to
be cleaned up so that the changes are easy to digest for those upgrading, flag them for the version they apply to, and
move them into an "immutable" section so that new changes don't continue to be added. Here are the steps:

1. Update [README.md](/README.md) link to "stable" version on Bintray to be the about-to-be-released stable version.
   Make sure to update both the button text and the link address.
2. Update the CHANGELOG file by retitling the "current" section with the about-to-be-released stable version and release
   date.
3. Clean up the changelog contents for the about-to-be-released stable version.
4. Add a new "current" section to the changelog with empty sections for `Changed`, `Deprecated`, `Fixed`,
   `Known Issues`, and `Removed`.
5. Raise (and get merged) a PR for the release.

Start Next Development Line
---------------------------

After the stable version is released, we need to get the next development line set up so that new work and changes are
on the new version line, rather than continuing to be added to the just-released stable version. Here are the steps for
that.

1. Update the `version.fili` and `version` properties in the root [`pom.xml`](/pom.xml) file to the next development
   version snapshot
2. Update the `parent`->`version` property in the all of the module `pom.xml` files to the next development version
   snapshot

   <sub>Note: There's probably a "maven-y" way to do this, but search-replace has generally done well for me.</sub>
