Fili Wikipedia Example Application
==================================

This example is an entirely self contained example that provides a Fili application for the 
[Druid Wikipedia example](http://druid.io/docs/0.9.1.1/tutorials/quickstart.html).

## Setup and Launching

1. Follow the [Druid instructions](http://druid.io/docs/0.9.1.1/tutorials/quickstart.html) to bring up a local Druid 
   cluster on your Unix based machine.
2. Clone this repository to your computer.
3. Use Maven to install and launch the Fili Wikipedia example:

```bash
mvn install
mvn -pl fili-wikipedia-example exec:java -Dexec.mainClass=com.yahoo.wiki.webservice.application.WikiMain
```

## Example Queries

Here are some sample queries that you can run to verify your server:

- List tables:  
  http://localhost:9998/v1/tables

- List dimensions:  
  http://localhost:9998/v1/dimensions

- List metrics:  
  http://localhost:9998/v1/metrics/

- Count of edits by hour for the last 72 hours:  
  http://localhost:9998/v1/data/wikipedia/hour/?metrics=count&dateTime=PT72H/current

- Show debug info, including the query sent to Druid:  
  http://localhost:9998/v1/data/wikipedia/hour/?format=debug&metrics=count&dateTime=PT72H/current

## Importing and Running in IntelliJ

1. In IntelliJ, go to `File -> Open`
2. Select the `pom.xml` file at the root of the project
3. Run `WikiMain` which can be found in `fili-wikipedia-example` (e.g. right click and choose run)
