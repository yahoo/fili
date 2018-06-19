#!/bin/bash
PUBLISH_WHITELIST="master"
RELEASE_TAG_REGEX="^[0-9]+\\.[0-9]+\\.[0-9]+"

export MAVEN_OPTS="-Xmx3000m"

# Deploy if this has a travis tag and is on an approved branch
MATCHING_TAG=$(echo $TRAVIS_TAG | egrep ${RELEASE_TAG_REGEX})
if [[ ${MATCHING_TAG} != "" ]]; then
    # This is a tag event with a version style tag
    echo "INFO Publishing, this is a tag event: ${TRAVIS_TAG}"

    echo "INFO Maven versions set : "
    mvn versions:set -DnewVersion=$(git describe) -DgenerateBackupPoms=false
    MAVEN_RETURN_CODE=$?
    if [[ ${MAVEN_RETURN_CODE} -ne 0 ]]; then
        echo "ERROR Unable to set version via Maven. Aborting publication."
        exit ${MAVEN_RETURN_CODE}
    fi
    mvn versions:update-property -Dproperty=version.fili -DnewVersion=$(git describe) -DgenerateBackupPoms=false
    MAVEN_RETURN_CODE=$?
    if [[ ${MAVEN_RETURN_CODE} -ne 0 ]]; then
        echo "ERROR Unable to update property via Maven. Aborting publication."
        exit ${MAVEN_RETURN_CODE}
    fi

    echo "INFO Deploying: "
    mvn deploy --settings travis/bintray-settings.xml
    MAVEN_RETURN_CODE=$?
    if [[ ${MAVEN_RETURN_CODE} -ne 0 ]]; then
        echo "ERROR Publication failed."
        exit ${MAVEN_RETURN_CODE}
    fi

    # Publication succeeded. We're done here
    exit 0
fi

# Set environment variable for testing purposes
export FILI_TEST_LIST=a,2,bc,234

# We're not on a release tag, so build and test the code
mvn verify
MAVEN_RETURN_CODE=$?
if [[ ${MAVEN_RETURN_CODE} -ne 0 ]]; then
    echo "ERROR Maven verify did not succeed."
    exit ${MAVEN_RETURN_CODE}
fi

# Only publish whitelisted branches
WHITELISTED=$(for a in ${PUBLISH_WHITELIST}; do if [ ${TRAVIS_BRANCH} = ${a} ]; then echo true; fi; done;)
if [[ "${WHITELISTED}" != "true" ]]; then
    echo "INFO Do not flag for publication, this is not a whitelisted branch: ${TRAVIS_BRANCH}"
    exit 0
fi

# Don't publish PR builds
if [[ "${TRAVIS_PULL_REQUEST}" != "false" ]]; then
    echo "INFO Do not flag for publication, this is a PR: ${TRAVIS_PULL_REQUEST}"
    exit 0
fi

# This is a publishable push, update the tag to trigger a deploy
echo "Ci tagging: "
travis/ci-tag.bash
