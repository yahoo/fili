Fili Wikipedia Example Application
==================================

This example is an entirely self contained example that provides a Fili application for the 
[Druid Wikipedia example](http://druid.io/docs/latest/tutorials/quickstart.html).

## Setup and Launching

1. Follow the [Druid instructions](http://druid.io/docs/latest/tutorials/quickstart.html) to bring up a local Druid 
   cluster on your Unix based machine.
   
2. Clone this repository to your computer.
    ```bash
    git clone git@github.com:yahoo/fili.git
    ```
3. Use Maven to install and launch the Fili Wikipedia example:

    ```bash
    cd fili
    mvn install
    mvn -pl fili-wikipedia-example exec:java
    ```
- Note that if your setup is different you can adjust it by changing the default parameters below
    ```bash
    # cd fili
    # mvn install
    mvn -pl fili-generic-example exec:java -Dbard__fili_port=9998 \
    -Dbard__druid_coord=http://localhost:8081/druid/coordinator/v1 \
    -Dbard__druid_broker=http://localhost:8082/druid/v2
    ```
    From another window, run a test query against the default druid data:

    _(Make sure the port matches the `-Dbard__fili_port` argument if you customized it in the previous step)_
    ```bash
    curl "http://localhost:9998/v1/data/wikipedia/hour/?metrics=deleted&dateTime=2015-09-12/PT2H" -H "Content-Type: application/json" | python -m json.tool
    ```
    If everything is working you should see something like:
    ```
    {
        "rows": [
            {
                "dateTime": "2015-09-12 00:00:00.000",
                "deleted": 1761.0
            },
            {
                "dateTime": "2015-09-12 01:00:00.000",
                "deleted": 16208.0
            }
        ]
    }        
    ```

## Example Queries

Here are some sample queries that you can run to verify your server:

- List tables:
  
      GET http://localhost:9998/v1/tables

- List dimensions:  

      GET http://localhost:9998/v1/dimensions

- List metrics:
  
      GET http://localhost:9998/v1/metrics/

- Count of edits by hour for the last 72 hours:  
  
      GET http://localhost:9998/v1/data/wikipedia/hour/?metrics=count&dateTime=PT72H/current

- Show debug info, including the query sent to Druid:  

      GET http://localhost:9998/v1/data/wikipedia/hour/?format=debug&metrics=count&dateTime=PT72H/current

## Importing and Running in IntelliJ

1. In IntelliJ, go to `File -> Open`
2. Select the `pom.xml` file at the root of the project
3. Run `WikiMain` which can be found in `fili-wikipedia-example` (e.g. right click and choose run)
