Fili Luthier
==================================

## What is Luthier Module
Luthier Module serves as a tool chain to automate configuration generation and supplies a standard configuration loader
 to extend from. Using Luthier is optional, but it will organize and speed up your workflow.

Users of Luthier goes through a four-step-process before launching an application. 
 Let us take the lifecycle of a bare-bone [`app`](luthier/src/main/lua/app) as an example:
 (for a more comprehensive example, see [LUTHIER_WIKI_README.md](luthier/LUTHIER_WIKI_README.md))
 
## Setup
Luthier uses the [LuaJ](http://www.luaj.org/luaj/3.0/README.html) library to execute the Lua configuration, so 
no manual setup needs to be performed. However, 
it is recommended that you install Lua on your dev machine to speed your development cycle when writing your 
configuration. To do so, run one of the following commands based on your architecture:

    * RHEL or CentOS: `yum install epel-release && yum install lua`

    * Fedora: `sudo dnf install lua`

    *Debian: `sudo apt-get install lua`

    *Homebrew on Mac OS X: `brew install lua`

The pre-existing system is tested on both Lua 5.3 and 5.2

## Running
To run a local Fili that uses a sample configuration:

```
~> cd luthier
~> ./scripts/runApp.sh
```
 
### Example Queries

Here are some sample queries that you can run to verify your server:

- List [tables](http://localhost:9012/v1/tables):
  
      GET http://localhost:9012/v1/tables

- List [dimensions](http://localhost:9012/v1/dimensions):  

      GET http://localhost:9012/v1/dimensions

- List [metrics](http://localhost:9012/v1/metrics/):
  
      GET http://localhost:9012/v1/metrics/

## How to extend Luthier
We have provided a working example that takes advantage of Luthier's base app and extend it to build REST Api for
some public data. The configuration can be found in `luthier/src/main/resources/`. `config.lua` is the entry point
for the configuration. The configuration itself can be found in `luthier/src/main/resources/app/`.

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
