Fili Luthier
==================================

## What is Luthier Module
Luthier Module serves as a tool chain to automate configuration generation and supplies a standard configuration loader
 to extend from. Using Luthier is optional, but it will organize and speed up your workflow.

Users of Luthier goes through a four-step-process before launching an application. 
 Let us take the lifecycle of the default [`app`](luthier/src/main/lua/app) as an example:
 
## Setup and Launching
### Automatically
You can use the following script which fully automates configuration generation, installation, and java execution:
```bash
git clone git@github.com:yahoo/fili.git
cd fili
luthier/scripts/buildApp.sh
```

---

### Manually
1. Build `*Config.json` into `luthier/target/classes/`.
    > This is done by running a [Lua](https://www.lua.org/) script:
    > ```bash
    > cd luthier/src/main/lua/
    > lua config.lua app
    > ```
    > The `config.lua` will trigger more Lua files to build respective configuration concepts, e.g. dimensions,
    > logicalTables, metrics, etc. They can be found in the default [`app`](luthier/src/main/lua/app)
2. Supply necessary Factories to handle each type of configuration concepts in
 [`luthier/src/main/java/com/yahoo/bard/webservice/data/config/luthier/factories`](luthier/src/main/java/com/yahoo/bard/webservice/data/config/luthier/factories). 
    > We have already supplied the common ones, as you can see in the above link.
3. Install Luthier using mvn
4. Run the Fili webservice by executing the [LuthierMain](luthier/src/main/java/com/yahoo/bard/webservice/applicatoin/LuthierMain) file
    > ```bash 
    > # cd to the project base directory and 
    > mvn -pl luthier exec:java@run-luthier-main
    > ```
    > or 
    > ```bash 
    > # cd to this directory
    > mvn -pl exec:java@run-luthier-main
    > ```

From another window, run a test query against the default druid data.

## Example Queries

Here are some sample queries that you can run to verify your server:

### Any Server

- List [tables](http://localhost:9012/v1/tables):
  
      GET http://localhost:9012/v1/tables

- List [dimensions](http://localhost:9012/v1/dimensions):  

      GET http://localhost:9012/v1/dimensions

- List [metrics](http://localhost:9012/v1/metrics/):
  
      GET http://localhost:9012/v1/metrics/

## How to extend Luthier
We have provided a working example that takes advantage of Luthier's base app and extend it to build REST Api for
 some public data. Please see the set-up in [LUTHIER_WIKI_README.md](LUTHIER_WIKI_README.md) for details.

## Notable Restrictions
- Using this is great for testing out fili and druid, but it can't do interesting things with metrics.
- This can only use 1 timegrain even though a datasource in druid *could* have more.

## Importing and Running in IntelliJ

1. In IntelliJ, go to `File -> Open`

2. Select the `pom.xml` file at the root of the project
    
    **NOTE:** if you're running this locally and haven't changed any settings (like the Wikipedia example) 
    you can **skip step 3**.
3. Under `luthier/src/main/resources/applicationConfig.properties`, change `bard__druid_broker`,
    `bard__druid_coord`, and other properties.
    
4. Run `LuthierWikiMain` which can be found in `src/main/java/com/yahoo/bard/webservice/application`
    (e.g. right click and choose run)
