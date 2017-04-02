Fili Generic Loader Application
==================================

This is application is meant to be able to run and connect to any instance of Druid to show the basic metrics and dimensions.

## Setup and Launching

1. Follow the [Druid instructions](http://druid.io/docs/latest/tutorials/quickstart.html) to bring up a local Druid 
   cluster on your Unix based machine.
   
2. Clone this repository to your computer.
    ```bash
    git clone git@github.com:yahoo/fili.git
    ```
3. Use Maven to install and launch the Fili Generic example:

Simliar to the [Wikipedia example](../fili-wikipedia-example) 

```bash
cd fili
mvn install
mvn -pl fili-generic-example exec:java
```
  
From another window, run a test query against the default druid data:

```bash
curl "http://localhost:9998/v1/data/wikiticker/day/?metrics=deleted&dateTime=2015-09-12/PT24H" -H "Content-Type: application/json" | python -m json.tool
```

If everything is working you should see something like:
    
```
{
	"rows": [{
		"dateTime": "2015-09-12 00:00:00.000",
		"deleted": 394298.0
	}]
}
```

## Example Queries

Here are some sample queries that you can run to verify your server:

### Any Server

- List tables:
  
      GET http://localhost:9998/v1/tables

- List dimensions:  

      GET http://localhost:9998/v1/dimensions

- List metrics:
  
      GET http://localhost:9998/v1/metrics/

### Specific to Wikipedia Example

- Count of edits by hour for the last 72 hours:  
  
      GET http://localhost:9998/v1/data/wikiticker/day/?metrics=count&dateTime=PT72H/current
    Note: this will should be something like the response below unless you have streaming data.
    ```json
    {
        "rows": [],
        "meta": {
            "missingIntervals": ["2017-03-30 00:00:00.000/2017-04-02 00:00:00.000"]
        }
    }
    ```  

- Show debug info, including the query sent to Druid:  

      GET http://localhost:9998/v1/data/wikiticker/day/?format=debug&metrics=count&dateTime=PT72H/current

## Importing and Running in IntelliJ

1. In IntelliJ, go to `File -> Open`
2. Select the `pom.xml` file at the root of the project
3. Run `GenericMain` which can be found in `fili-generic-example` (e.g. right click and choose run)
