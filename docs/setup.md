Setup
=====

The following will guide you through standing up Fili in front of a Druid instance.


Table of Contents
-----------------

- [Prerequisites](#prerequisites)
- [High Level Steps](#high-level-steps)
- [Fili Integration Application](#bard-integration-application)
- [Configure Metadata](#configure-metadata)
- [Configuration Files](#configuration-files)
- [Scripts](#scripts)
- [Build and deploy the WAR](#build-and-deploy-the-war)
- [Dimension Loading](#dimension-loading)
- [Troubleshooting](#troubleshooting)

Prerequisites
-------------

- [Jetty][jetty]

- A working [Druid][druid] cluster to serve as Fili's backend.

- (Optional: Dimension Caching) A source of truth for loaded dimensions. See [dimension loading](#dimension-loading) for 
more details.

- (Optional: Dimension Caching) [MDBM][mdbm] (or [Redis][redis]) for storing dimension data, if the cardinalities of the
dimensions are too high for an in-memory map.


High Level Steps
----------------

The following is a bird's eye view of the steps you must take to stand up a Fili instance.

- [Clone the bard wikipedia example into a separate project](#fili-wikipedia-example).

- [Modify the dimension, metric, physical table and logical table information to fit the needs of your 
application.](#configure-metadata)

- [Update the configuration files.](#configuration-files)

- [Build and deploy the war.](#build-and-deploy-the-war)

- [Setup the dimension loader.](#dimension-loading)

Fili Wikipedia Example
----------------------------

The [Fili wikipedia example][bard-wikipedia-example] is where you will leverage the Fili library. Here is where
you will configure your application-specific metrics, dimensions, and tables.

Configure Metadata
------------------

The bulk of the work is in configuring Fili's metadata:

- [Metrics][configuringMetricsDocumentation]

- [Dimensions][configuringDimensionsDocumentation]

- [Tables][loadTablesDocumentation]

- [Binding the configuration code (and other resources) to Fili][binderDocumentation]

Configuration Files
-------------------

Next, several configuration files and scripts need to be tweaked:

* In [applicationConfig.properties][applicationConfig] the following properties need to be set:
    - `bard__resource_binder = binder.factory.class.path`
    - `bard__dimension_backend = mdbm` (`redis` if you wish to use Redis for your dimension metadata, `memory` if
    you wish to use an in-memory map)
        - (Optional: MDBM) `bard__mdbm_location = dir/to/mdbm` - Note that Fili assumes this directory contains a
        `dimensionCache` folder.
    - `bard__non_ui_broker = http://url/to/druid/broker`
    - `bard__ui_broker = http://url/to/druid/broker`
    - `bard__druid_coord = http://url/to/druid/coordinator`
    
* [pom.xml][pomXml] -  Find the `fili.version` tag, and update that to point to the desired version of Fili, rather
   than a snapshot. 

Note that both `bard__non_ui_broker` and `bard__ui_broker` are set to the same broker URL. These parameters are 
artifacts of the project Fili was spun out of. Eventually, these two settings will be generalized into something useful
for other projects. For now, you can safely treat them as if they were the same.

Build and Deploy the WAR
------------------------

Now that the integration app has been properly configured, we need to build and deploy the WAR. Build the war by
running `mvn install` on your application. You will then find a WAR file under the `target` directory. This WAR should 
be dropped into the webapp directory of your Jetty instance.

Dimension Loading
-----------------

Dimensions in Fili fall into two categories: loaded and non-loaded. A Loaded dimension is one whose
values (and associated metadata) have been loaded into Fili. A non-loaded dimension is one that has been configured,
but whose values and metadata have not been loaded into Fili. Fili can filter on dimension metadata, and perform
dimension joins _only_ on loaded dimensions. However, you _can_ query Druid using non-loaded dimensions. So Fili is
quite useful even with non-loaded dimensions, but if you want to unlock its full power, you should ensure that 
all of your dimensions are loaded.

To load a dimension, you need to load its dimension rows into Fili by sending two POST requests
to `/v1/cache/dimensions/<myDimension>/dimensionRows`. The first request updates the dimension values, the second loads
the datetime at which the dimensions were successfully loaded. The second request is essentially used to mark that the 
dimension rows were successfully loaded. 

We will look at the first request first. The payload for each dimension is an object. The object contains 
a list of objects called `dimensionRows`. Each object in the list contains the data for a single value of the dimension:

```json
{ 
    "dimensionRows": [ 
        { 
            "dataField1": data, 
            "dataField2": data
        }, {
            "dataField1": data, 
            "dataField2": data
        },
        ...
    ]
}
```

A [well-defined dimension][configuringDimensionsDocumentation] has only two requirements: There must be a 
field that serves as a key field for each dimension value (some sort of `id` field), and a top-level field `lastUpdated`
for the entire dimension that provides the date at which the values for the dimension were last updated. 

The second request is a very simple JSON object:

```json
{
    "lastUpdated": "Roughly current date in ISO 8601 format",
}
```
 
For example, suppose we have a dimension `gender` with three values: `male`, `female`, and `unknown`. The metadata
consists of a field `id` and a field `description`. Then we might send the following payload to 
`/v1/cache/dimensions/gender/dimensionRows`:

```json
{
    "dimensionRows": [
        {
            "id": "male",
             "description": "The visitor was of the male persuasion.",
        }, {
            "id": "female",
             "description": "The visitor was of the female persuasion."
        }, {
            "id": "unknown",
            "description": "We don't know the gender of the visitor. Oh woe is us."
        } 
    ]
}
```

Followed by: 
```json
{
    "lastUpdated": "2015-12-16T10:25:00"
}
```

Typically this is done by setting up a program that runs in the background, periodically grabs dimension 
metadata from the dimension source of truth, and pushes it into Fili.

### Non-Loaded Dimensions ###

It may be the case that you don't need dimension joins, or to filter on dimension metadata for one or more of
your dimensions. You can make such a dimension a non-loaded dimension. A non-loaded dimension is configured as follows:

1. Configure the dimension to use the [NoOpSearchProvider][noOpSearchProvider]. See 
[Configuring Dimensions][configuringDimensionsDocumentation] for details on how to configure a dimension's 
`SearchProvider`.

2. Send a JSON payload to `/v1/cache/dimensions/dimensionName` containing just an `id` for the dimension, and a 
`lastUpdated` field with some date following the [ISO 8601][iso8601] specification.

For example, suppose we want to make gender non-loaded. Then, after configuring the `Gender` dimension with the 
`NoOpSearchProvider` and starting Jetty, we would send the following payload to `/v1/cache/dimensions/gender`: 

```json
    {
        "name": "gender",
        "lastUpdated": "2015-12-16T00:00:00"
    }
```

You can reduce the complexity of setup by making all of your dimensions non-loaded. Therefore, if you are primarily 
interested in rapidly setting up a Fili instance, you may wish to make all of your dimensions non-loaded. You can
load your dimensions later, once you have verified that Fili will meet your needs.


Troubleshooting
---------------

The Fili logs are Jetty logs, so they can be found wherever your Jetty instance stores its logs.

#### App tests return a 500 error ####

It may be that the versions of dependencies in your application's POM are out of sync with the dependency
versions used by fili. If that is the case, then modifying your dependency versions to use the same version as
fili should solve the problem.

#### Fili crashes and throws `IllegalStateException: Couldn't create dir: path/to/mdbm/dimensionCache/page` ####

There are two possible causes:
 
 1. The `dimensionCache` subdirectory in `path/to/mdbm` does not exist. Fili assumes `path/to/mdbm/dimensionCache`
already exists, and does not attempt to create it.
 
 2. `dimensionCache` exists, but does not have the correct read/write/execute permissions. The user that Jetty is 
 running under (typically `nobody`) needs to have read, write, and execute permissions on `dimensionCache`.

#### Server log claims the segment metadata loader is not healthy ####

Your dimensions have never been updated, and don't have a `lastUpdated` field set. This can happen if you forgot to 
set up your dimension loader. You can get more details about the problem at the `/healthcheck` endpoint.
If the dimensions are not being loaded, then see the [Dimension Loading](#dimension-loading) for more details on how
to set up the dimension loader (or [configure all of your dimensions to be non-loaded](#non-loaded-dimensions)).

[applicationConfig]: https://github.com/yahoo/fili/blob/master/fili-wikipedia-example/src/main/resources/applicationConfig.properties

[fili-wikipedia-example]: https://github.com/yahoo/fili/tree/master/fili-wikipedia-example
[binderDocumentation]: https://github.com/yahoo/fili/issues/11

[configuringDimensionsDocumentation]: https://github.com/yahoo/fili/issues/12
[configuringMetricsDocumentation]: configuring-metrics.md 

[druid]: http://druid.io

[iso8601]: https://en.wikipedia.org/wiki/ISO_8601

[jetty]: http://www.eclipse.org/jetty/

[loadTablesDocumentation]: https://github.com/yahoo/fili/issues/13

[mdbm]: http://yahoo.github.io/mdbm/

[noOpSearchProvider]: ../src/main/java/com/yahoo/bard/webservice/data/dimension/impl/NoOpSearchProvider.java

[pomXml]: https://github.com/yahoo/fili/blob/master/fili-core/pom.xml

[redis]: http://redis.io/
