mvn -pl fili-wikipedia-example exec:java -Dbard__fili_port=9998 -Dbard__druid_coord=http://localhost:8081/druid/coordinator/v1 -Dbard__druid_broker=http://localhost:8082/druid/v2
