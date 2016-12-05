#!/bin/bash

BARD_HOST=${1:localhost}

# Reset all dimensions
for dim in $(curl --silent http://$BARD_HOST:8080/v1/dimensions |python -c "import json; import sys; data = sys.stdin.read(); j=json.loads(data); print '\n'.join(i['name'] for i in j['rows'])"); do
  echo reseting $dim
  echo '{ "name": "'$dim'", "lastUpdated": "2016-01-01" }' | curl -X POST -d@- http://$BARD_HOST:8080/v1/cache/dimensions/$dim -H 'Content-Type: application/json'
done
