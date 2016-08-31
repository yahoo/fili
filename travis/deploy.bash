#!/bin/bash
PUBLISH_WHITELIST="master"
RELEASE_TAG_REGEX="^[0-9]+\\.[0-9]+\\.[0-9]+"

# Deploy if this has a travis tag and is on an approved branch
MATCHING_TAG=$(echo $TRAVIS_TAG | egrep ${RELEASE_TAG_REGEX})

if [[ ${MATCHING_TAG} != "" ]]; then
    # This is a tag event with a version style tag
    echo "Publishing, this is a tag event: ${TRAVIS_TAG}"

    echo "Maven versions set : "
    mvn versions:set -DnewVersion=$(git describe) -DgenerateBackupPoms=false

    echo "Deploying: "
    mvn deploy --settings travis/bintray-settings.xml
    exit 0
fi

mvn verify
MAVEN_RETURN_CODE=$?

if [[ ${MAVEN_RETURN_CODE} -ne 0 ]]; then
    echo "Maven did not succeed. Not flagging for publication."
    exit ${MAVEN_RETURN_CODE}
fi

WHITELISTED=$(echo ${PUBLISH_WHITELIST} | grep ${TRAVIS_BRANCH})
if [[ "${WHITELISTED}" == "" ]]; then
    echo "Do not flag for publication, this is not a whitelisted branch: ${TRAVIS_BRANCH}"
    exit 0
fi

if [[ "${TRAVIS_PULL_REQUEST}" != "false" ]]; then
    echo "Do not flag for publication, this is a PR: ${TRAVIS_PULL_REQUEST}"
   exit 0
fi

# This is a deployable push, update the tag to trigger a deploy
echo "Ci tagging: "
travis/ci-tag.bash
