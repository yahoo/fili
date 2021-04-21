#!/usr/bin/env bash

set -e


export GPG_TTY=$(tty)
export MAVEN_OPTS="-Xmx3000m"
VERSION=$(git describe)

# This is a tag event with a version style tag
echo "INFO Publishing, this is a github release event"

#get the last tag on this branch
echo "INFO Last tag: $VERSION"

echo "INFO Maven versions set : "
mvn versions:set -DnewVersion=$VERSION -DgenerateBackupPoms=false
MAVEN_RETURN_CODE=$?

if [[ ${MAVEN_RETURN_CODE} -ne 0 ]]; then
    echo "ERROR Unable to set version via Maven. Aborting publication."
    exit ${MAVEN_RETURN_CODE}
fi

mvn versions:update-property -Dproperty=version.fili -DnewVersion=$VERSION -DgenerateBackupPoms=false
MAVEN_RETURN_CODE=$?

if [[ ${MAVEN_RETURN_CODE} -ne 0 ]]; then
    echo "ERROR Unable to update property via Maven. Aborting publication."
    exit ${MAVEN_RETURN_CODE}
fi

mkdir -p screwdriver/deploy
chmod 0700 screwdriver/deploy

openssl aes-256-cbc -pass pass:$GPG_ENCPHRASE -in screwdriver/pubring.gpg.enc -out screwdriver/deploy/pubring.gpg -pbkdf2 -d
openssl aes-256-cbc -pass pass:$GPG_ENCPHRASE -in screwdriver/secring.gpg.enc -out screwdriver/deploy/secring.gpg -pbkdf2 -d

echo "INFO Deploying to maven central: "
mvn -B deploy -P ossrh -DskipTests --settings screwdriver/settings/settings-publish.xml
MAVEN_RETURN_CODE=$?
if [[ ${MAVEN_RETURN_CODE} -ne 0 ]]; then
    echo "ERROR Publication failed."
    exit ${MAVEN_RETURN_CODE}
fi

rm -rf screwdriver/deploy
