#!/bin/bash
export MAVEN_OPTS="-Xmx3000m"

# Debug mvn version
mvn -version

# We're not on a release tag, so build and test the code
echo "INFO mvn Install"
mvn -B package
#mvn -B -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn install -Pcoverage.build
#capture the maven return code
MAVEN_RETURN_CODE=$?
if [[ ${MAVEN_RETURN_CODE} -ne 0 ]]; then
    echo "ERROR Maven verify did not succeed."
    exit ${MAVEN_RETURN_CODE}
fi

echo "INFO dependency check"
mvn -B org.owasp:dependency-check-maven:check
#capture the maven return code
MAVEN_RETURN_CODE=$?
if [[ ${MAVEN_RETURN_CODE} -ne 0 ]]; then
    echo "ERROR dependency check did not succeed."
    exit ${MAVEN_RETURN_CODE}
fi

echo "INFO mvn Site"
mvn -B -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn post-site -Pcoverage.report,sonar

#echo "INFO mvn sonar"
#mvn -B -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn sonar:sonar -Dsonar.projectKey=yahoo_fili -Dsonar.organization=yahoo -Dsonar.login=${SONAR_TOKEN} -Pcoverage.report,sonar
