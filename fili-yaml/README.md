Fili Yaml Application
=====================

An easy way to get Fili up and running in front of Druid. No programming required!

Caveats: 
* Does not yet support all features of Fili; your mileage may vary.

## Example Usage

```
(cd .. && mvn clean install)
./build-docker.sh
docker build -t jetty-fili .
docker run -p 8080:8080 -v`pwd`/fili.yaml:/fili.yaml -t jetty-fili  -f /fili.yaml -d bard__property=value -p config.properties
```

After a few seconds, you should see Fili running on port 8080.

To actually make queries, Fili must be configured properly, of course. Some 
settings you'll want to set via the command line or in a properties file:

```
bard__non_ui_druid_broker=http://druid-url:4080/druid/v2
bard__ui_druid_broker=http://druid-url:4080/druid/v2
bard__druid_coord=http://coord-url:8082/druid/coordinator/v1
```

Finally, the YAML config doesn't support dimension loading yet. Fili will refuse to
return any data if dimensions are not loaded (healthcheck will fail), so you can
use the `reset-dims.sh` script supplied here to initialize the dimensions (python
and curl required).
