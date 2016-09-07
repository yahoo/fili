#!/bin/bash

travis/git_credentials.bash

echo "Publishing for tag: $TRAVIS_TAG"

# Build the new tag to push
echo "Rewrite CHANGELOG.md with version prefix: $TRAVIS_TAG"
echo "IMPLEMENT THIS THING!"

echo "Commit change"
git add CHANGELOG.md

echo "git commit -m 'Updating to version $TRAVIS_TAG'"

# Push the new CHANGELOG to origin
echo "Pushing to origin"
git push origin

echo "Maven versions set: "
mvn versions:set -DnewVersion=${TRAVIS_TAG} -DgenerateBackupPoms=false

echo "Deploying: "
mvn deploy --settings travis/bintray-settings.xml
