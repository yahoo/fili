Fili Generic Loader Application
==================================

This application will automatically configure fili to work with **any** instance
 of Druid and show the basic metrics and dimensions. This lets you test what it's
  like using Fili without putting any effort into setting it up.

In order to set up, this will connect to druid at  [http://localhost:8081/druid/coordinator/v1](http://localhost:8081/druid/coordinator/v1).
 If your set up is different, you'll have to change the `bard__druid_coord`,
  `bard__druid_broker` url in `applicationConfig.properties`.
  
Note that this was last tested using [version 0.9.1](https://github.com/yahoo/fili/tree/0.9.1)

## Setup and Launching

1. Have a [Druid](http://druid.io/docs/latest/tutorials/quickstart.html) cluster running on your Unix based machine.
   
2. Clone this repository to your computer.
    ```bash
    git clone git@github.com:yahoo/fili.git
    ```
3. Use Maven to install and launch the Fili Generic example:

    ```bash
    cd fili
    mvn install
    mvn -pl fili-generic-example exec:java
    ```

- Note that if your setup is different you can adjust it by changing the default parameters below

    ```bash
    mvn -pl fili-generic-example exec:java -Dbard__fili_port=9998 \
    -Dbard__druid_coord=http://localhost:8081/druid/coordinator/v1 \
    -Dbard__druid_broker=http://localhost:8082/druid/v2 \
    ```

From another window, run a test query against the default druid data.

## Example Queries

Here are some sample queries that you can run to verify your server:

### Any Server

- List [tables](http://localhost:9998/v1/tables):
  
      GET http://localhost:9998/v1/tables

- List [dimensions](http://localhost:9998/v1/dimensions):  

      GET http://localhost:9998/v1/dimensions

- List [metrics](http://localhost:9998/v1/metrics/):
  
      GET http://localhost:9998/v1/metrics/

### Specific to Wikipedia data

- If everything is working, the [query below](http://localhost:9998/v1/data/wikiticker/day/?metrics=deleted&dateTime=2015-09-12/PT24H)
    ```bash
    curl "http://localhost:9998/v1/data/wikiticker/day/?metrics=deleted&dateTime=2015-09-12/PT24H" -H "Content-Type: application/json" | python -m json.tool
    ```
     should show something like:
    ```
    {
        "rows": [{
            "dateTime": "2015-09-12 00:00:00.000",
            "deleted": 394298.0
        }]
    }
    ```

- Count of edits by hour for the last 72 hours:  
  
      GET http://localhost:9998/v1/data/wikiticker/day/?metrics=count&dateTime=PT72H/current
    
    Note: this will should be something like the response below since the 
    wikiticker table doesn't have data for the past 72 hours from now.
    ```json
    {
        "rows": [],
        "meta": {
            "missingIntervals": ["2017-03-30 00:00:00.000/2017-04-02 00:00:00.000"]
        }
    }
    ```  

- Show [debug info](http://localhost:9998/v1/data/wikiticker/day/?format=debug&metrics=count&dateTime=PT72H/current),
 including the query sent to Druid:  

      GET http://localhost:9998/v1/data/wikiticker/day/?format=debug&metrics=count&dateTime=PT72H/current

## Notable Restrictions

- Using this is great for testing out fili and druid, but it can't do interesting things with metrics.
- This can only use 1 timegrain even though a datasource in druid *could* have more.

## Importing and Running in IntelliJ

1. In IntelliJ, go to `File -> Open`

2. Select the `pom.xml` file at the root of the project
    
    **NOTE:** if you're running this locally and haven't changed any settings (like the Wikipedia example) 
    you can **skip step 3**.
3. Under `src/main/resources/applicationConfig.properties`, change `bard__druid_broker`,
 `bard__druid_coord`, and other properties.
    
4. Run `GenericMain` which can be found in `fili-generic-example` (e.g. right click and choose run)
