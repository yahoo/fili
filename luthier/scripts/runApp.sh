#!/bin/bash
# runs wiki example

local PORT="${1:-9998}"

#find the root of the project
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

#launch the project
mvn -pl luthier exec:java -Dbard__fili_port=${PORT} -Dbard__druid_coord=http://localhost:8081/druid/coordinator/v1 -Dbard__druid_broker=http://localhost:8082/druid/v2
