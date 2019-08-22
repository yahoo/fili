# Overview

Fili is configured using Java. Limitations in the Java programming language
and in how our configuration is structured mean that some things can be very
difficult. For example, generating variations on a single metric (like `dailyAvg`)
is very difficult.
Java's type systema and object model make for verbose
configuration. Verbose configuration is difficult to understand, modify and
extend.

We can't just rely on configuration files (i.e. YAML or JSON) because
complex configurations may require programatically creating objects.
For example, customers may want to generate all possible permutations of
a metric operation. Perhaps a dailyAvgPerWeek, dailyAvgPerMonth, and
dailyAvgPerYear variation of each metric in a given table.

Therefore, we need configuration to be a language in its own right.

## Requirements
The language we use needs the following properties:

1. Simple. People unfamiliar with the language should be able to pick it up in
an afternoon, or tweak already existing code in less than that.

2. Expressive. People should be able to easily build variations on a theme, like
DailyAvg variations of an already existing metric.

3. Concise. The configuration should have minimal ceremony.

4. Clear. It should make the structure of the configuration objects clear. It
should be easy to understand the expected structure, and write custom code to
build the expected structure.

We feel that [Lua](https://www.lua.org/) has all these properties. Lua was
originally built as a data input language with simple data munging. It
is a dynamically typed garbage collected imperative language with a single
recursive data structure: tables (i.e. hashmaps). It supports looping,
functions as first class citizens, and a variety of syntactic sugar to make
working with string-keyed tables easy.

## Document Structure

The first section (and the bulk of the document) lays out in detail what the
configuration language looks like and what it supports. It's written as a first
draft of documentation for how to use the language. Our goal is to implement
the functionality documented here.

The second section gives some thoughts on what changes we need to make to the
existing code to support this functionality. I've focused on how I think the
major interfaces need to change. It's not super detailed, but hopefully it's
enough to give us a sense of the scale of what we need to do. We'll be moving a
lot of code around, and tweaking some, but I think we can use most of the
scaffolding that already exists.

The current code can be found on the `wiki-test` branch.

# Lua Configuration

Fili assumes there is a file `config.lua` under `src/config` that serves as the
point of entry into the configuration. This program imports three sibling files:
`dimensions.lua`, `tables.lua`, and `metrics.lua`. These three files contain
the customer's configuration. `dimensions.lua` contains their dimension
configuration, `tables.lua` their table configuration, and `metrics.lua` their
metrics configuration.

The config file imports each of these modules, munges them into a unified data
format, and generates JSON: `src/external/DimensionConfig.json`,
`src/external/MetricConfig.json`, and `src/external/TablesConfig.json`. Fili
will read in these three JSON files and use them to populate the
`TablesDictionary`, `MetricsDictionary`, and `DimensionsDictionary`.

## Using Lua Configuration

In order to use the Lua-based external configuration, a few changes need to 
be made to your BinderFactory.

1. Override the
   [getConfigurationLoader](https://github.com/yahoo/fili/blob/6b27301334a427fcfc3ea2215afca862f75ff05f/fili-core/src/main/java/com/yahoo/bard/webservice/application/AbstractBinderFactory.java#L1066)
to build and return an instance of the `LuthierIndustrialPark`.
`LuthierIndustrialPark` has a Builder this is comes prepopulated with the
default configuration types provided by Fili. Registering additional factories is as simple as `builder.with<Type>Factory(name, new CustomTypeFactory<>)`.

## Data Format

Modules in Lua are tables. A module is defined by populating a table and
returning it at the bottom of a file. For example, suppose we have the
following in a file `foo.lua`:

```lua
local M = {}

function M.publicFunction()
    print("Hello, world!")
end

return M
```

Then, we can import this in another file, `bar.lua`:

```lua
local foo = require 'foo'
foo.publicFunction()
```

Running `~> lua bar.lua` will print "Hello, World!" to stdout.

Each of `dimensions.lua`, `metrics.lua` and `tables.lua` define a module.
Each module has their own format, layed out below.

## dimensions.lua ##

`dimensions.lua` returns a table that maps dimension names to dimensions.

A dimension is a table with following keys:
* apiName - A string. The name used by customers querying Fili to work with this dimension.
    Optional. Defaults to the dimension's name.
* longName - A string. longer human friendly name for the dimension
    Optional. Defaults to the dimension's name
* description -  A string. Brief documentation about the dimension
    Optional. Defaults to the dimension's name
* fields - A list of tables describing the fields attached to the dimension. See
    [Fields](#fields) below.
* category - A string. An arbitrary category to put the dimension in. This is not
    used directly by Fili, but rather exists as a marker for UI's should
    they desire to use it to organize dimensions.
* searchProvider - A table that configures a search provider. See
    [SearchProvider](#search-provider) below.
* keyValueStore - A table that configures the key value store. See
    [Key Value Store](#key-value-store) below.
* type - A string. The type of dimension to build. Optional. Defaults
to Fili's default (KeyValueStoreDimension) if not provided.

### Fields

Fields define a dimension's "metadata." For example, the country dimension
may have the fields id, name, desc, and ISO. id is a unique identifier for
the country (typically this is the primary key in your dimension database),
name is a human readable name of the country, desc is a brief description
of the country, and ISO is the country' ISO code.

A field is a table with at least one parameter:
1. name - The name of the field
Fields may also have the optional parameter:
1. tags - A list of tags that may provide additional information about
    a field. For example, the "primaryKey" tag is used to mark a field
    as the dimension's primary key.

To aid in configuration, we provide two utility functions for creating
fields:
1. pk - Takes a name (string) and returns a primary key field. A primary key
    is a field with two keys:
        a. name - The name of the field
        b. tags - A singleton list containing the value "primaryKey"
2. field - A function that takes a variable number of field names and
    returns an equal number of fields. Each field is a table with
    one key:
        a. name - The name for the field.

For example, the following are equivalent:

```lua
    local country_fields_manually_defined = {
        id={ name="id", tags={"primaryKey"}},
        name={name="name"},
        desc={name="desc"},
        ISO={name="ISO"}
    }

    local country_fields_with_helper_functions = {
        id=pk("id"),
        field("name", "desc", "ISO")
    }
```

It is *strongly* encouraged that you use the helper functions, especially
for the primary key. Typos are a thing.

### Search Provider ###

A SearchProvider filters dimension values based on their fields. For
example, a SearchProvider can find all countries that have the string "States"
in their name. This allows us to store a single identifier for each dimension
value in Druid, but still perform human friendly searches in Fili (i.e.
`country|name-contains[States]` rather than `country|id-in[1,2,3,4]`).

A search provider's configuration is a table with at least one key:

* type - The type of the search provider. This is used by Fili to find the
    right [SearchProvider Factory](#search-provider-factory). The name
    doesn't matter, except search provider types exist in a global
    namespace, so care should be taken to make sure the names are unique.

It may support any other keys that particular search provider requires to be built.

Fili supports the following built in SearchProviders:

* fili-lucene - Uses
    [Apache Lucene](http://lucene.apache.org/) to handle field resolution.

    Additional keys:
    * luceneIndexPath - A String. The path to the lucene index files
    * maxResults - An integer. The maximum number of results allowed on a single
        page of results.
    * searchTimeout - An integer. The maximum number of milliseconds Lucene should
        search before timing out. Defaults to the
        `lucene_search_timeout_ms` configuration parameter.

* fili-scan - Uses an in-memory data structure to handle field resolutions.
    Not recommended for large dimensions.

    Additional keys:
        None

* fili-noop - Does not do any field resolution at all. Useful for dimensions
    that don't have any fields.

    Additional keys:
    * approximateCardinality - An integer. The rough size of the dimension.
        This is used when Fili is deciding whether to send a
        weight-check query before sending an actual query. If not
        present, Fili will use the query_weight_limit configuration
        parameter.

### Key Value Store ###

The Key Value Store is similar to a SearchProvider, except it only supports
lookups by ID. Ideally, the Key Value Store will be optimized to perform these
look ups *very fast*, since it's used to annotate the dimension values returned
by Druid with their fields.

A Key Value Store's required configuration is the same as the search provider's:

* type - The type of the key value store. This is used by Fili to find the
    right [KeyValueStoreFactory](#key-value-store-factory). These names
    exist in their own global namespace, so care must be taken to make sure
    all key value store names are unique.

It may have any other keys it needs to be configured.

See [Key Value Store Factory](#key-value-store-factory) for details on how to
define and use a custom key value store.

Fili supports the following built in KeyValueStores:

* fili-map - Uses an in-memory data structure to handle point lookups. Not
    recommended for large dimensions.
    
    Additional keys:
        None

* fili-reddis - Uses a Redis cluster to perform point lookups.

    Additional keys:
    * redisNamespace - The reddis namespace to use
    * storeName - The reddis store to use
    * reddisConfiguration - A table with the following keys:
        * host - The host running the Reddis service
        * port - The port to contact the host through. Optional.

See [KeyValueStoreFactory](#key-value-store-factory) for details on how to
define and use a custom key value store.

# Java Configuration

It is a fact of life that every project has its own tech stack, architectural
quirks, and business requirements.  Therefore, Fili provides deep support for
customization.

For example, Fili was built on the assumption that querying a
MySQL database once per dimension per query would be expensive, and so it
encourages customers to use Lucene to keep a local cache of dimension data.
Some customers may have a large number of MySQL read slaves, so hitting MySQL
directly is performant, and much simpler than maintaining a local cache. Such a
customer can define their own [`SearchProvider`](https://github.com/yahoo/fili/blob/luthier-design/fili-core/src/main/java/com/yahoo/bard/webservice/data/dimension/SearchProvider.java)
implementation that queries MySQL directly,
register it with Fili in their Java code, and then reference it
in their Lua config.

Every building block can be customized, and they are all customized following
the same basic pattern:

1. Implement the appropriate interface with your custom implementation.
2. Implement the Factory<T> interface, where `T` is the interface you 
    implemented in step 1. The `Factory<T>` is responsible for building
    instances of your custom implementation from te configuration.
3. Register your factory with your `LuthierIndustrialPark` in your overridden 
[AbstractBinderFactory::getConfigurationLoader](https://github.com/yahoo/fili/blob/706e5d899fce36a115581441f59abfd75f1c6f62/fili-core/src/main/java/com/yahoo/bard/webservice/application/AbstractBinderFactory.java#L1066).

Each Factory has a method `build` that takes in the current config and a
`LuthierIndustrialPark` object.

## LuthierIndustrialPark

`LuthierIndustrialPark` provides an API for constructing a configuration object's
dependencies. It has the following methods:

```java
    LogicalTable getLogicalTable(String tableName);
    PhysicalTable getPhysicalTable(String tableName);
    LogicalMetric getLogicalMetric(String metricName);
    Dimension getDimension(String dimensionName);
    SearchProvider getSearchProvider(String searchProviderName);
    KeyValueStore getKeyValueStore(String keyValueStoreName);
```

It is constructed using a `LuthierIndustrialPark.Builder`. The `Builder` is where we register our custom
factories. It includes the following methods:

```java
    LuthierIndustrialPark.Builder withLogicalTableFactory(String factoryName, Factory<LogicalTable> factory);
    LuthierIndustrialPark.Builder withPhysicalTableFactory(String factoryName, Factory<PhysicalTable> factory);
    LuthierIndustrialPark.Builder withLogicalMetricFactory(String factoryName, Factory<LogicalMetric> factory);
    LuthierIndustrialPark.Builder withDimensionFactory(String factoryName, Factory<Dimension> factory);
    LuthierIndustrialPark.Builder withSearchProviderFactory(String factoryName, Factory<SearchProvider> factory);
    LuthierIndustrialPark.Builder withKeyValueStoreFactory(String factoryName, Factory<KeyValueStore> factory);
    LuthierIndustrialPark.Builder withMetricFactory(String factoryName, Factory<Metric> factory);
    LuthierIndustrialPark.Builder withMetricMakerFactory(String factoryName, Factory<MetricMaker> factory);
```

The `LuthierIndustrialPark` is an implementation of
[`ConfigurationLoader`](https://github.com/yahoo/fili/blob/luthier-design/fili-core/src/main/java/com/yahoo/bard/webservice/data/config/ConfigurationLoader.java).
As such, it should be built in your overridden `AbstractBinderFactory::getConfigurationLoader`. This is
also where the `with` methods can be invoked on the builder to register custom factories.

## DimensionFactory

In order to use a custom dimension in your Lua config, you need to follow
three steps:

1. Implement the Dimension interface.
2. Implement the Factory<Dimension> interface to build an instance of your
custom Dimension.
3. Register your factory.

A `Factory<T>` is an object that takes in configuration information
and builds a `T`. The interface has a single method:

```java
    T build(String name, ObjectNode configTable, LuthierIndustrialPark resourceFactories);
```

We register all our custom dimension factories by attaching them to the
`LuthierIndustrialPark.Builder` in your overridden
`AbstractBinderFactory::getConfigurationLoader`.

For example the following may appear in your BinderFactory:

```java
    @Override
    public void getConfigurationLoader() {
        LuthierIndustrialPark.Builder builder = new LuthierIndustrialPark.Builder(new ResourceDictionaries());
        builder.withDimensionFactory("custom-dimension", new MyCustomDimensionFactory());
        return builder.build();
    }
```

## SearchProviderFactory

In order to use a custom search provider in your Lua config, you need to follow
the three steps:

1. Implement the SearchProvider interface.
2. Implement the Factory<SearchProvider> interface to build an instance of your
custom SearchProvider.
3. Register your factory.

A `Factory<T>` is an object that takes in configuration information
and builds a `T`. The interface has a single method:

```java
    T build(String name, ObjectNode configTable, LuthierIndustrialPark resourceFactories);
```

We register all our custom SearchProvider factories by attaching them to the
`LuthierIndustrialPark.Builder` in our overridden
`AbstractBinderFactory::getConfigurationLoader`.

For example the following may appear in our BinderFactory:

```java
    @Override
    public void getConfigurationLoader() {
        LuthierIndustrialPark.Builder builder = new LuthierIndustrialPark.Builder(new ResourceDictionaries());
        builder.withSearchProviderFactory("custom-searchProvider", new MyCustomSearchProviderFactory());
        return builder.build();
    }
```
## KeyValueStoreFactory

In order to use a custom search provider in your Lua config, you need to follow
the three steps:

1. Implement the KeyValueStore interface.
2. Implement the Factory<KeyValueStore> interface to build an instance of your
custom KeyValueStore.
3. Register your factory.

A `Factory<T>` is an object that takes in configuration information
and builds a `T`. The interface has a single method:

```java
    T build(String name, ObjectNode configTable, LuthierIndustrialPark resourceFactories);
```

We register all our custom KeyValueStore factories by attaching them to the
`LuthierIndustrialPark.Builder` in our overridden
`AbstractBinderFactory::getConfigurationLoader`.

For example the following may appear in our BinderFactory:

```java
    @Override
    public void getConfigurationLoader() {
        LuthierIndustrialPark.Builder builder = new LuthierIndustrialPark.Builder(new ResourceDictionaries());
        builder.withKeyValueStoreFactory("custom-keyValueStore", new MyCustomKeyValueStoreFactory());
        return builder.build();
    }
```

# Metrics

Metrics are formulas of aggregations and post-aggregations. These formulas are
constructed by taking Makers (which can be thought of as operators and
operands) and combining them to create metrics.

For example, suppose we have two metrics in Druid `foo` and `bar`. Both of
these are longs, so when we aggregate them, we want to sum them.
We can define two metrics in Fili that surface these by using the `LongSum`
maker. In pseudo-code:

```
fili-foo = LongSum(foo)
fili-bar = LongSum(bar)
```

Now, suppose we want another metric in Fili `baz` that computes the sum of `foo`
and `bar`. For this, we need to use the ArithmeticMaker:

```
baz = ArithmeticMakerPlus(fili-foo, fili-bar)
```

In our Lua config this would look like the following:
```lua
{
    fili-foo = {
        maker = "longSum",
        druidMetric = "foo"
    },
    fili-bar = {
        maker = "longSum",
        druidMetric = "bar"
    },
    baz = {
        maker = "arithmeticPLUS",
        dependencies = {"fili-foo", "fili-bar"}
    }
}
```

Fili has two types of makers: flat makers and nested makers.
Flat makers are makers that directly aggregate a Druid metric, and correspond
directly to a Druid aggregation (i.e. `longSum`, `doubleSum`). Nested makers
are makers that rely on already-defined Fili metrics (i.e. `arithmeticPLUS`).

We define metrics by "building" them out of makers. Fili comes with a host of
builtin makers that should fit most of your usecases, but it has deep support
for supplying custom makers.

Metrics also support the `type` field that allow customers to specify a custom
type of `LogicalMetric` if so needed. If not provided, it defaults to 
`com.yahoo.bard.webservice.data.metric.LogicalMetric`, the metric type provided
by Fili. Note that customers should rarely if ever need to provide a custom
`LogicalMetric`. Most needs will be met more cleanly by providing custom
makers.

# Custom Makers

Adding custom makers follows the same basic pattern as adding custom search
providers or key value stores. Each maker has a `name`, and any number of
needed fields. The
`name` is used to look up the appropriate `MetricMakerFactory` (yes, it's a
terrible name of the kind made fun of all over the internet. Sorry.). The
full table is passed to the `MetricMakerFactory.build` method as a `Map<String,
JsonNode>`. For example, the following is the definition of the
`arithmeticPLUS` maker, which is an instance of the ArithmeticMaker
MetricMaker:

```lua
M.arithmeticPLUS = {
    type = "ArithmeticMaker",
    operation = "PLUS",
}
```

Unlike search providers, makers don't vary based on the metric they're attached
to. Therefore, customers do not need to be building makers over and over again.
They only need to build them once (if they aren't builtin), and reference them
by name in each metric that uses them, as demonstrated [earlier](#metrics).

To use a custom maker in the Lua config, you need to follow these three
steps:

1. Implement the MetricMaker interface.
2. Implement the Factory<MetricMaker> interface to build an instance of your
custom MetricMaker.
3. Register your factory.

A `Factory<T>` is an object that takes in configuration information
and builds a `T`. The interface has a single method:

```java
    T build(String name, ObjectNode configTable, LuthierIndustrialPark resourceFactories);
```

We register all our custom MetricMaker factories by attaching them to the
`LuthierIndustrialPark.Builder` in our overridden
`AbstractBinderFactory::getConfigurationLoader`.

For example the following may appear in our BinderFactory:

```java
    @Override
    public void getConfigurationLoader() {
        LuthierIndustrialPark.Builder builder = new LuthierIndustrialPark.Builder(new ResourceDictionaries());
        builder.withMetricMakerFactory("custom-metricMaker", new MyCustomMetricMakerFactory());
        return builder.build();
    }
```

## MetricFactory

Fili supports building custom instances of `LogicalMetric` (though it's 
far more likely you'll want a custom `MetricMaker` rather than a custom
`LogicalMetric`). 

To define a custom `LogicalMetric`, we follow the same basic pattern as
everything else:

1. Implement the LogicalMetric interface.
2. Implement the Factory<LogicalMetric> interface to build an instance of your
custom LogicalMetric.
3. Register your factory.

A `Factory<T>` is an object that takes in configuration information
and builds a `T`. The interface has a single method:

```java
    T build(String name, ObjectNode configTable, LuthierIndustrialPark resourceFactories);
```

We register all our custom LogicalMetric factories by attaching them to the
`LuthierIndustrialPark.Builder` in our overridden
`AbstractBinderFactory::getConfigurationLoader`.

For example the following may appear in our BinderFactory:

```java
    @Override
    public void getConfigurationLoader() {
        LuthierIndustrialPark.Builder builder = new LuthierIndustrialPark.Builder(new ResourceDictionaries());
        builder.withLogicalMetricFactory("custom-logicalMetric", new MyCustomLogicalMetricFactory());
        return builder.build();
    }
```

# Tables

Tables are the easiest components to configure, since they're just putting
together dimensions and metrics.

There are two types of tables: Physical and Logical. Therefore, the
`tables.lua` module should contain two keys:

* `physical` - A table that maps physical table names to physical tables.
* `logical` - A table that maps logical table names to logical tables.

## Physical

Physical tables represent the actual tables that exist in Druid. Most of the
time, there will be a one-to-one mapping between physical tables and tables in
Druid, with the same schema. They are configured as Lua tables with the
following keys:

* name - A string. The name of the physical table. Optional.
    Defaults to the table's key in `M.physical`. This should be the same as
    the name of the associated table in Druid.
* description - A string. Brief documentation about the table. Optional.
    Defaults to `name`.
* metrics - A list of the names of the *Druid* metrics that live on
    this table in Druid.
* dimensions - A list of dimensions in Druid. If a dimension name is of the
    format `name1#nam2`, then `name1` is exposed to the rest of Fili as the name
    of the dimension, while `name2` is used to refer to the dimension when
    building the Druid query. This allows people to use the same physical dimension
    name to refer to different druid names in the respective Druid tables.
* granularity - The granularity of the physical table, one of "hour", "day",
    "week", or "month", or "all." The granularity is NOT case sensitive.
* type: The custom type of logical table to build (optional), defaults to Fili's
built in version if not provided.

TODO: I know we have multiple types of physical tables built into Fili. We should name
them and list them here.

For example, suppose we define two new physical tables, `physical1` and
`physical2`:

```lua
    M.physical = {
        physical1 = {
            metrics = {
                "foo",
                "bar"
            },
            dimensions = {
                "dimension1",
                "dimension2",
                "dimension3"
            },
            granularity="hour"
        },
        physical2 = {
            metrics = {
                "foo",
                "bar"
            },
            dimensions = {
                "dimension1",
                "dimension2",
                "dimension3#dimension3a"
            }
        },
        granularity="day"
    }
```

Here, we're asserting that our Druid cluster has two tables: `physical1` and
`physical2`. Both tables have metrics called `foo`, and `bar`, and dimensions
called `dimension1`, and `dimension2`. However `physical` doesn't have
`dimension2`. Both tables expose `dimension3` as part of their schema. However,
in `physical1` `dimension3` refers to `dimension3` in the actual Druid table
on the Druid cluster, while in `physical2` it refers to `dimension3a`.
`physical1` exposes an hourly view of its underlying data, while
physical2 exposes a daily view.

## Logical

Logical tables are the actual tables that we expose in the Fili API. They have
Fili metrics (configured in [Metrics](#metrics), Fili dimensions (configured in
[Dimensions](#dimensions)), and a list of physical tables. One of the key
capabilities that LogicalTables enable is query planning. If a logical table is
backed by multiple physical tables, Fili will automatically select the
"smallest" table that can satisfy the query. Tables with fewer dimensions tend
to be smaller, as are tables aggregated to a higher granularity.  Smaller
tables tend to be (often significantly) faster to query, so Fili allows
customers to trade memory in their Druid cluster for speed.

Logical tables are configured as Lua tables with the following keys:

* name - A string. The name of the table. Optional.
    Defaults to the table's key in `M.logical`.
* description - A string. Brief documentation about the table. Optional.
    Defaults to `name`.
* physicaltables - A list of the names of the physical tables that this
    logical table is backed by.
* metrics - A list of the names of the Fili metrics that should be attached
    to this table. These must be a subset of the metrics configured in
    `metrics.lua`.
* dimensions - A list of the names of the dimensions that should be
    attached to this table. They must be a subset of the dimensions
    configured in `dimensions.lua`.
* granularity - A list of the granularities that this table supports. One
    or more of: `all`, `hour`, `day`, `week`, `month`, `year`.
* type: The custom type of logical table to build (optional). Defaults to "logicalTable"
    Fili's built in implementation of LogicalTable. 

For example, let's define a logical table, `logical` that uses `physical1`
and `physical2` defined [earlier](#physical):

```lua
M.logical = {
    logical={
        metrics = {"foo", "bar", "baz"},
        dimensions = {"dimension1", "dimension2", "dimension3"},
        granularity = {"all", "hour", "day", "week", "month", "year"},
        physicaltables = {"physical1", "physical2"}
    }
}
```

Observe that `baz` isn't a metric on `physical1` or `physical2`. However,
`baz` is defined in terms of `foo` and `bar`, both of which do show up on
the physical tables.

When querying `logical`, Fili will use `physical2` for all queries of the `day`
granularity and above, since the two tables have the same schema. Fili will use
`physical1` for all hourly queries, because `physical2` can't answer hourly
queries.

### Namespaced Metrics

Fili supports "namespacing" metrics. This allows different logical tables to
use the same metric name to refer to different formulas. For example, suppose
we have two logical tables, `avgMetrics` and `medianMetrics`. Both want a metric
`pageViews`. However, `logical1` exposes an average number of pageViews over
the given time interval, while  `medianMetrics` exposes the *median* (this is
a rather questionable design, but let's not judge the imaginary architect too
harshly). Then, we can need to do the following:

1. Define a metric with the name `pageViews#average`
2. Define another metric with the name `pageViews#median`.

Then we configure the logical tables like normal:

```lua
    M.logical = {
        avgMetrics={
            metrics={"pageViews#average"},
            dimensions={"gender"},
            granularity={"day"},
            physicaltables={"physical1", "physical2"}
        },
        medianMetrics = {
            metrics ={"pageViews#median"},
            dimensions={"gender"},
            granularity={"day"},
            physicaltables={"physical1", "physical2"}
        }
    }
```

In this case, both tables expose the metric `pageViews`. `avgMetics`
uses the `pageViews#average` definition, while `medianMetrics` uses
the metric `pageViews#median`.

More generally to define a namespaced metric, define a metric with the
name `apiName#namespace`, where `apiName` is the name of the
metric that should be exposed, while `namespace` is some sort
of (human readable) string that defines a namespace. Multiple metrics
may share the same namespace, however the full string `m1#m2` must
be unique.

TODO: Move this paragraph under metrics as well, and talk about namespaced metrics there as
well, because this will impact how people write custom makers.
Only the name `m1` is pased in to the `MetricMaker` when constructing the metric
at configuration time. `m2` is stripped off and used in the `LuthierIndustrialPark`
to populate the `MetricDictionary` appropriately.

## LogicalTableFactory


In order to use a custom search provider in your Lua config, you need to follow
the three steps:

1. Implement the LogicalTable interface.
2. Implement the Factory<LogicalTable> interface to build an instance of your
custom LogicalTable.
3. Register your factory.

A `Factory<T>` is an object that takes in configuration information
and builds a `T`. The interface has a single method:

```java
    T build(String name, ObjectNode configTable, LuthierIndustrialPark resourceFactories);
```

We register all our custom LogicalTable factories by attaching them to the
`LuthierIndustrialPark.Builder` in our overridden
`AbstractBinderFactory::getConfigurationLoader`.

For example the following may appear in our BinderFactory:

```java
    @Override
    public void getConfigurationLoader() {
        LuthierIndustrialPark.Builder builder = new LuthierIndustrialPark.Builder(new ResourceDictionaries());
        builder.withLogicalTableFactory("custom-logicalTable", new MyCustomLogicalTableFactory());
        return builder.build();
    }
```

## PhysicalTableFactory

In order to use a custom physical table in your Lua config, you need to follow
three steps:

In order to use a custom search provider in your Lua config, you need to follow
the three steps:

1. Implement the PhysicalTable interface.
2. Implement the Factory<PhysicalTable> interface to build an instance of your
custom PhysicalTable.
3. Register your factory.

A `Factory<T>` is an object that takes in configuration information
and builds a `T`. The interface has a single method:

```java
    T build(String name, ObjectNode configTable, LuthierIndustrialPark resourceFactories);
```

We register all our custom PhysicalTable factories by attaching them to the
`LuthierIndustrialPark.Builder` in our overridden
`AbstractBinderFactory::getConfigurationLoader`.

For example the following may appear in our BinderFactory:

```java
    @Override
    public void getConfigurationLoader() {
        LuthierIndustrialPark.Builder builder = new LuthierIndustrialPark.Builder(new ResourceDictionaries());
        builder.withPhysicalTableFactory("custom-physicalTable", new MyCustomPhysicalTableFactory());
        return builder.build();
    }
```
# Configuration In Action

Fili comes with a folder `config` that includes a fully defined configuration
for the Druid cluster used in Druid's
[quickstart](http://druid.io/docs/latest/tutorials/index.html). Take a look
at that to see these ideas in action.

# Design

When executed, the Lua config generates `config/external/MetricConfig.json`,
`config/external/DimensionConfig.json`, and `config/external/TableConig.json`.

These JSON files will be read in and parsed by the LudierLoader.  The
LudierLoader is responsible for extracting from each JSON file a `ObjectNode` 
that maps names to configuration objects.  We'll need
one for logical tables, physical tables, dimensions, metrics, search providers
and key value stores.  We'll need to inject these dictionaries into the 
`ResourceDictionaries` object, and make sure the `LuthierIndustrialPark` have 
access to the `ResourceDictionaries`.

It will iterate over the table configuration, and build
each table. This will have the impact of constructing each logical table and
their dependencies (physical tables, metrics, dimensions, search providers, key
value stores).

It will look something like:
```java
    logicalTableConfiguration.entrySet().forEach(
        nameTableConfiguration -> {
            ObjectNode tableConfiguration = nameTableConfiguration.getValue();
            String tableType = tableConfiguration.getText("type");
            return resourceFactories.getLogicalTable(
                tableType == null ? "logicalTable" : tableType,
                nameTableConfiguration.getKey(), 
                nameTableConfiguration.getValue()
            );
        }
    )
```

Instances of
[`FactoryPark<T>`](https://github.com/yahoo/fili/blob/luthier-design/luthier/src/main/java/com/yahoo/bard/webservice/config/luthier/FactoryPark.java)
are responsible for extracting the appropriate configuration object from the
appropriate `ObjectNode` and passing it along to the
appropriate factory.

This will require us to extend `ResourceDictionaries` with dictionaries for all
the objects that can be defined in Lua. We will need a dictionary for
SearchProviders and for KeyValueStores.
