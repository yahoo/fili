#!/bin/bash
PUBLISH_WHITELIST="master"
RELEASE_TAG_REGEX="^[0-9]+\\.[0-9]+\\.[0-9]+"

# Deploy if this has a travis tag and is on an approved branch
MATCHING_TAG=$(echo $TRAVIS_TAG | egrep ${RELEASE_TAG_REGEX})

if [[ ${MATCHING_TAG} != "" ]]; then
    travis/release.bash
    exit $?
fi

mvn -Dmaven.javadoc.skip=true verify

