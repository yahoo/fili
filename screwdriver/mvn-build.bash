#!/bin/bash
PUBLISH_WHITELIST="master,sd-master"
RELEASE_TAG_REGEX="^[0-9]+\\.[0-9]+\\.[0-9]+"

export MAVEN_OPTS="-Xmx3000m"

# Set environment variable for testing purposes
export FILI_TEST_LIST=a,2,bc,234

mvn -version

# We're not on a release tag, so build and test the code
echo "INFO mvn Install"
mvn -B -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn install -Pcoverage.build
#capture the maven return code
MAVEN_RETURN_CODE=$?
if [[ ${MAVEN_RETURN_CODE} -ne 0 ]]; then
    echo "ERROR Maven verify did not succeed."
    exit ${MAVEN_RETURN_CODE}
fi

echo "INFO mvn Site"
mvn -B -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn post-site -Pcoverage.report,sonar

#echo "INFO mvn sonar"
#mvn -B -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn sonar:sonar -Dsonar.projectKey=yahoo_fili -Dsonar.organization=yahoo -Dsonar.login=${SONAR_TOKEN} -Pcoverage.report,sonar

# Only publish whitelisted branches
WHITELISTED=$(for a in ${PUBLISH_WHITELIST}; do if [[ ${TRAVIS_BRANCH} == ${a} ]]; then echo true; fi; done;)
if [[ "${WHITELISTED}" != "true" ]]; then
    echo "INFO Do not flag for publication, this is not a whitelisted branch: ${TRAVIS_BRANCH}"
    exit 0
fi

# Don't publish PR builds.
if [[ "${TRAVIS_PULL_REQUEST}" != "false" ]]; then
    echo "INFO Do not flag for publication, this is a PR: ${TRAVIS_PULL_REQUEST}"
    exit 0
fi
