#!/bin/bash
export MAVEN_OPTS="-Xmx3000m"

# This is a tag event with a version style tag
echo "INFO Publishing, this is a github release event"

#get the last tag on this branch
echo "INFO Last tag: $(git describe)"

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

echo "INFO Deploying to bintray: "
mvn -DskipTests deploy --settings screwdriver/bintray-settings.xml

MAVEN_RETURN_CODE=$?
if [[ ${MAVEN_RETURN_CODE} -ne 0 ]]; then
    echo "ERROR Publication failed."
    exit ${MAVEN_RETURN_CODE}
fi
