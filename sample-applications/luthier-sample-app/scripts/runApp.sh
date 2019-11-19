#!/bin/bash
# runs wiki example

PORT=${1:-9012}

#launch the project
mvn clean install -DskipTests -Dcheckstyle.skip exec:java@run-luthier-main -Dbard__fili_port="$PORT" \
    -Dbard__druid_coord=http://localhost:8081/druid/coordinator/v1 \
    -Dbard__druid_broker=http://localhost:8082/druid/v2
