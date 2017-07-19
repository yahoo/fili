#!/bin/sh 

export HOSTIP="$(resolveip -s $HOSTNAME)" 
/usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf & 
echo "Waiting for Druid to finish setting up"
while ! curl http://localhost:8081/druid/coordinator/v1/datasources | grep -q "wikipedia"; do
      sleep 5
done
echo "Druid finished setting up. Starting Fili"
mvn -pl fili-generic-example exec:java -Dbard__fili_port=9998 
    -Dbard__druid_coord=http://localhost:8081/druid/coordinator/v1 
    -Dbard__non_ui_druid_broker=http://localhost:8082/druid/v2 
    -Dbard__ui_druid_broker=http://localhost:8082/druid/v2
