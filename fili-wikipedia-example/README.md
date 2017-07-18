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
    From another window, run a test query against the default druid data:

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

## Running Fili with Docker

There is a [Docker image](https://hub.docker.com/r/mpardesh/fili/) for Fili which can be found on 
Dockerhub. If you would like to experiment with Fili without having to download its dependencies, you can 
[install](https://www.docker.com/community-edition) and start Docker. Then run these commands: 

    
    docker pull mpardesh/fili 
    docker run --name fili-wikipedia-example -i --rm -p 3001:8081 -p 3000:8082 mpardesh/fili:1.0
    

This will start a container. Please wait a few minutes for Druid to get ready. 

Once Druid is ready, you can start querying! Here is a 
[sample query](http://localhost:9998/v1/data/wikipedia/day/?metrics=deleted&dateTime=2013-08-01/PT24H)
to get started:

    http://localhost:9998/v1/data/wikipedia/day/?metrics=deleted&dateTime=2013-08-01/PT24H
    
If Druid isn't ready yet, you will see this message:
    
    {
        "rows": [],
        "meta": {
            "missingIntervals": ["2013-08-01 00:00:00.000/2013-08-02 00:00:00.000"]
        }
    }
    
If the query is successful, you should see this:

    {
        "rows": [{
            "dateTime": "2013-08-01 00:00:00.000",
            "deleted": -39917308
        }]
    }
    
To stop the container, run 

    docker stop
    
in a different terminal tab. 

Note: the data used with Docker is from a different day than the data used with Druid quickstart. 

## Importing and Running in IntelliJ

1. In IntelliJ, go to `File -> Open`
2. Select the `pom.xml` file at the root of the project
3. Run `WikiMain` which can be found in `fili-wikipedia-example` (e.g. right click and choose run)
