Change Log
==========

All notable changes to Fili will be documented here. Changes are accumulated as new paragraphs at the top of the current
major version. Each change has a link to the pull request that makes the change and to the issue that triggered the
pull request if there was one.

### Added:

- [Move off BaseCompositePhysicalTable inheritance usage](https://github.com/yahoo/fili/pull/748)
    * Added builder methods to `MetricUnionAvailability` and `PartitionAvailability` to save on needing to add
      additional table classes.

- [Adding withDoFork to LookBackQuery](https://github.com/yahoo/fili/issues/756)
    * Added `withDoFork` to `LookBackQuery` class.    
    
-- [An injection point for customizing the WebLoggingFilter to use during tests](https://github.com/yahoo/fili/pull/749)
    * Extend `JerseyTestBinder` and override `getLoggingFilter`.

- [An injection point for customizing Exception handling]https://github.com/yahoo/fili/pull/742)
    * Customers can provide their own logic for handling top level Exceptions in
      the `DataServlet` by implementing `DataExceptionHandler`, and any other
      servlet by implementing `MetadataExceptionHandler`.

- [Add support for "case_sensitive" attribute in FragmentSearchQuerySpec](https://github.com/yahoo/fili/pull/727)
    * Enable `FragmentSearchQuerySpec` to accept an argument for `case_sensitive` so that API users can configure this
      attribute for JSON serialization through Fili.

- [Add specs for InsensitiveContainsSearchQuerySpec & RegexSearchQuerySpec](https://github.com/yahoo/fili/pull/732)
    * `RegexSearchQuerySpec` and `InsensitiveContainsSearchQuerySpec` have no dedicated test specs. This PR adds tests
      for them.

- [Implement ContainsSearchQuerySpec](https://github.com/yahoo/fili/pull/730)
    * Adds serialization for `ContainsSearchQuerySpec` so that Fili API users can use that through Fili.

- [Add storageStrategy as a field of the DimensionConfig class](https://github.com/yahoo/fili/issues/718)
    * Adds getStorageStrategy as a field of the dimensionConfig class.
    * Passes the storage strategy to the KeyValueStoreDimension Constructor

- [Add more tests to RegisteredLookupMetadataLoadTask](https://github.com/yahoo/fili/pull/673)
    * Adds tests to make sure the load tasks can update status correctly.

- [Add tests to Utils](https://github.com/yahoo/fili/pull/675)
    * Add missing tests to `Utils` class.

### Changed:

- [Bumping query id inside withIntervals of LookBackQuery](https://github.com/yahoo/fili/issues/756)
    * Returning a new `LookBackQuery` with `doFork` set to `true` which bumps query id inside `withIntervals` method.
    * Renamed every occurrence of `doFork` to `incrementQueryId`.
    * Removed `withDoFork` from `LookBackQuery` class.
     
- [Change to ordered data structures for ApiRequestImpls](https://github.com/yahoo/fili/issues/753)
    * Change Set<foo> to LinkedHashSet<foo> in most ApiRequestImpl getters
    * Change Set<Interval> to List<Interval> in ApiRequests
    
- [Moved ResponseFormatType from Singleton enum to interface with enum impl](https://github.com/yahoo/fili/issues/711)
    * Refactored ResponseFormatType to allow expansion via additional interface implementors
    * Replaced equality based matching with 'accepts' model

- [Restructured Report Building out of ApiRequest](https://github.com/yahoo/fili/issues/711)
    * Pushed UriInfo used to produce `PaginationMapper` into `RequestContext`
    * Renamed 'getPage' to 'paginate' and refactored off `ApiRequest` and into `PageLinkBuilder`
    * Moved pagination mostly out of individual servlet classes and into `EndpointServlet`
    * Initialized per request `Response.ResponseBuilder` in EndpointServlet rather than `ApiRequestImpl` constructor
    * Simplified injected classes that took both `UrlInfo` and `ContainerRequestContext` to get the one from inside the other.
    * Pushed entire container request context to content disposition building code (prelude to https://github.com/yahoo/fili/issues/709 )

- [Change `BaseCompositePhysicalTable` into a concrete class](https://github.com/yahoo/fili/pull/745)
    * Currently `BaseCompositePhysicalTable` is an abstract class, though it has all of the functionality for a simple composite physical table. Changed to a concrete class to allow for simple composite table behavior with requiring an extension.
    * Two simple implementations of `BaseCompositePhysicalTable`, `PartitionCompositeTable` and `MetricUnionCompositeTable` are now deprecated. Instead, the availabilities for these tables should be created directly and passed in to `BaseCompositePhysicalTable`.  

- [Change availability behavior on BasePhysicalTable](https://github.com/yahoo/fili/pull/743)
    * Currently `BasePhysicalTable` overrides `getAvailableIntervals(constraint)` and `getAllAvailableIntervals()`, and defers this behavior to its availability. This PR changes `BasePhysicalTable` to also override `getAvailableIntervals()` and defer to its availability.  

- [Making Custom Sketch operations possible in PostAggreations](https://github.com/yahoo/fili/issues/740)    
    * Added `PostAggregation` and `Aggregation` instance check in `asSketchEstimate(MetricField field)` method of the class `ThetaSketchFieldConverter`.
    * `FieldAccessorPostAggregation` is called only for `Aggregation` and not for `PostAggregation`. 

- [Let DimensionApiRequestMapper throw RequestValidationException instead of BadApiRequestException](https://github.com/yahoo/fili/pull/715)
    * `DimensionApiRequestMapper.apply()` is made to obey the interface contract by throwing
      `RequestValidationException` instead of ``BadApiRequestException``

- [Inject customizable extraction functions to RegisteredLookupDimension](https://github.com/yahoo/fili/pull/724)
    * Instead of injecting registered lookup names, we inject registered lookup extraction functions for lookup
      dimension so that downstream projects can configure all fields of registered lookup extraction functions.

- [Abort request when too many Druid filters are generated](https://github.com/yahoo/fili/pull/690)
    * In order to avoid Druid queries with too much filters on high-cardinality dimension, Fili sets a upper limit
      on the number of filters and aborts requests if the limit is exceeded.
 
- [Class re-organization](https://github.com/yahoo/fili/pull/694)
    * Put `Granularity` interfaces and its implementations in the same package
    * Put `*ApiRequest` interfaces and their implementations in the same package

- [Avoid casting to generate SimplifiedIntervalList](https://github.com/yahoo/fili/pull/677)
    * Some downstream projects generated partial intervals as `ArrayList`, which cannot be cased to
      `SimplifiedIntervalList` in places like `getVolatileIntervalsWithDefault`. The result is a casting exception which
      crashes downstream applications. Casting is replaced with a explicit `SimplifiedIntervalList` object creation.

### Deprecated:

- [Undeprecated pagination by collection](https://github.com/yahoo/fili/issues/711)
    * Since we seem to be in no hurry to switch to heavier reliance on streams. (also renamed paginate and moved to `PageLinkBuilder`)

- [Deprecate `PartitionCompositeTable` and `MetricUnionCompositeTable`](https://github.com/yahoo/fili/pull/745)
    * Two simple implementations of `BaseCompositePhysicalTable` (`PartitionCompositeTable` and `MetricUnionCompositeTable`) are now deprecated. Instead, the availabilities for these tables should be created directly and passed in to `BaseCompositePhysicalTable`.  

### Fixed:

- [Fix GroovyTestUtils json parsing](https://github.com/yahoo/fili/pull/760)
    * Properly handles json parsing failures and non-JSON expected strings.

- [Fix generate intervals logic when availability is empty](https://github.com/yahoo/fili/pull/702)
    * Logic to generate intervals when `CURRENT_MACRO_USES_LATEST` flag is turned on has a bug. The code throws `NoSuchElementException` when the table has no availabilities. This PR fixes the bug by checking if the availability of the underlying table is empty.
- [Correct Druid coordinator URL in Wikipedia example](https://github.com/yahoo/fili/pull/683)
    * Config value for Druid coordinator URL is mis-typed.
    
- [Upgrade codenarc to recognize unused imports in Groovy](https://github.com/yahoo/fili/pull/685)
    * There are number of unused imports sitting in tests. The cause is an out-dated codenarc version. This PR upgrades
      the version and removes those unused imports with the new version.


### Known Issues:



### Removed:

Current
-------

v0.9.137 - 2018/04/13 
=====

0.9 Highlights
-------------

### Fili Security Added!
Release security module for fili data security filters. Created `ChainingRequestMapper`, and a set of mappers for gatekeeping on security roles and whitelisting dimension filters.

Added by @michael-mclawhorn in https://github.com/yahoo/fili/pull/405

### DataApiRequestFactory layer
Downstream projects now have more flexibility to construct `DataApiRequest` by using injectableFactory. An additional constructor for DataApiRequestImpl unpacks the config resources bundle to make it easier to override dictionaries.

Added by @michael-mclawhorn in https://github.com/yahoo/fili/issues/603

### Make Field Accessor PostAggregation able to reference post aggregations in adddition to aggregations
Druid allows (but does not protect against ordering) post aggregation trees referencing columns that are also post aggregation trees.  This makes it possible to send such a query by using a field accessor to reference another query expression.  Using this capability may have some risk.

Added by @michael-mclawhorn in https://github.com/yahoo/fili/pull/543

### Etag Cache
In the more recent versions of druid that are released after February 23rd, 2017. Druid added support for HTTP Etag. By including a If-None-Match header along with a druid query, druid will compute a hash as the etag in a way such that each unique response has a corresponding unique etag, the etag will be included in the header along with the response. In addition, if a query to druid includes the If-None-Match with a etag of the query, druid will check if the etag matches the response of the query, if yes, druid will return a HTTP Status 304 Content Not Modified response to indicate that the response is unchanged and matches the etag received from druid query request header. Otherwise druid will execute the query and respond normally with a new etag attached to the response header.

This new feature is designed by @garyluoex . For more info, visit @garyluoex 's design at https://github.com/yahoo/fili/issues/255

### More robust Lucene Search Provider and Key Value Store
Lucene Search Provider can re-open in a bug-free way and close more cleanly

Added by @garyluoex in https://github.com/yahoo/fili/pull/551 and https://github.com/yahoo/fili/pull/521

### Extraction Function on selector filter
Update Fili to accommodate the deprecated `ExtractionFilter` in druid, use selector filter with extraction function instead. Added extraction function on dimensional filter, defaults to extraction function on dimension if it exists.

Added by @garyluoex in https://github.com/yahoo/fili/pull/617

### More controllable RequestLog
Exposes the `LogInfo` objects stored in the `RequestLog`, via `RequestLog::retrieveAll` making it easier for customers to implement their own scheme for logging the `RequestLog`

Added by @archolewa in https://github.com/yahoo/fili/pull/574

### Druid lookup metadata load status check 
Fili now supports checking Druid lookup status as one of it's health check. It will be very easy to identify any failed lookups.

Added by @QubitPi in https://github.com/yahoo/fili/pull/620

### Add ability to use custom rate limiting schemes
While backward compatibility is guaranteed, Fili now allows users to rate limit(with a a new rate limiter) based on different criteria other than the default criteria.

Added by @efronbs in https://github.com/yahoo/fili/pull/591

### Support Time Format Extraction Function in Fili
Druid [`TimeFormatExtractionFunction`](http://druid.io/docs/0.10.1/querying/dimensionspecs.html#time-format-extraction-function) is added to Fili. API users could interact with Druid using `TimeFormatExtractionFunction` through Fili.

Added by @QubitPi in https://github.com/yahoo/fili/pull/611

### Dimension load strategy indicator
In order to allow clients to be notified if a dimension's values are browsable and searchable, a [storage strategy metadata](https://github.com/yahoo/fili/blob/master/fili-core/src/main/java/com/yahoo/bard/webservice/data/dimension/metadata/StorageStrategy.java) is added to dimension. A browsable and searchable dimension is denoted by `LOADED`, whereas the opposite is denoted by `NONE`. This will be very useful for UI backed by Fili on sending dimension-related queries.

Added by @michael-mclawhorn, @garyluoex and @QubitPi  in https://github.com/yahoo/fili/pull/575, https://github.com/yahoo/fili/pull/589, https://github.com/yahoo/fili/pull/558, https://github.com/yahoo/fili/pull/578

### Query Split Logging
Include metrics in logging to allow for better evaluation of the impact of caching for split queries. There used to be only a binary flag (`BardQueryInfo.cached`) that is inconsistently set for split queries. Now 3 new metrics are added

1. Number of split queries satisfied by cache
2. Number of split queries actually sent to the fact store. (not satisfied by cache)
3. Number of weight-checked queries

Added by @QubitPi in https://github.com/yahoo/fili/pull/537

### Configurable Metric Long Name
Logical metric has more config-richness to not just configure metric name, but also metric long name, description, etc. MetricInstance is now created by accepting a LogicalMetricInfo which contains all these fields in addition to metric name.

Added by @QubitPi in https://github.com/yahoo/fili/pull/492

### Search provider can hot-swap index and key value store can hot-swap store location
[`LuceneSearchProvider`](./fili-core/src/main/java/com/yahoo/bard/webservice/data/dimension/impl/LuceneSearchProvider.java) is able to hot swap index by replacing Lucene index by moving the old index directory to a different location, moving new indexes to a new directory with the same old name, and deleting the old index directory in file system.
[`KeyValueStore`](fili-core/src/main/java/com/yahoo/bard/webservice/data/dimension/KeyValueStore.java) is also made to support hot-swapping key value store location

Added by @QubitPi in https://github.com/yahoo/fili/pull/522

### Uptime Status Metric
A metric showing how long Fili has been running is available.

Added by @mpardesh in https://github.com/yahoo/fili/pull/518

### Consolidate UI & Non-UI broker configurations
`ui_druid_broke` and `non_ui_druid_broker` are not used separately anymore. Instead, a single `druid_broker` replaces the two. For backwards compatibility, Fili checks if `druid_broker` is set. If not, Fili uses `non_ui_druid_broker` and then `ui_druid_broker`

Added by @mpardesh in https://github.com/yahoo/fili/pull/489

Credits
---------

Thanks to everyone who contributed to this release!

@michael-mclawhorn Michael Mclawhorn
@garyluoex Gary Luo
@archolewa Andrew Cholewa
@QubitPi Jiaqi Liu
@asifmansoora Asif Mansoor Amanullah
@efronbs Ben Efron
@deepakb91 Deepak Babu
@tarrantzhang Tarrant Zhang
@kevinhinterlong Kevin Hinterlong
@mpardesh Monica Pardeshi
@colemanProjects Neelan Coleman
@onlinecco 
@dejan2609 Dejan Stojadinović

### Added:

- [Latest Time Macro](https://github.com/yahoo/fili/pull/697)
    * Added `logicalTableAvailability` to `TableUtils` which returns the union of intervals for the logical table.
    * Added `now` parameter to `generateIntervals` for which time macros will be relatively calculated.
    * Added `CURRENT_MACRO_USES_LATEST` flag which when turned on uses the first unavailable availability to generate the intervals. 

- [Annotate Functional Interface](https://github.com/yahoo/fili/pull/606)
    * Add `@FunctionalInterface` annotation to all functional interfaces.

- [Implement LookupLoadTask](https://github.com/yahoo/fili/pull/620)
    * Add capability for Fili to check load statuses of Druid lookups.

- [Extraction Function on selector filter](https://github.com/yahoo/fili/pull/617)
    * Added extraction function on dimensional filter, defaults to extraction function on dimension if it exists.

- [Implement TimeFormatExtractionFunction](https://github.com/yahoo/fili/pull/611)
    * Enable [`TimeFormatExtractionFunction`](http://druid.io/docs/0.10.1/querying/dimensionspecs.html#time-format-extraction-function)
      in Fili so that API users could interact with Druid using `TimeFormatExtractionFunction` through Fili.

- [Complete DateTimeUtils tests](https://github.com/yahoo/fili/pull/595)
    * Add tests for all un-tested methods in `DateTimeUtils`.
    
- [Enable checkstyle to detect incorrect package header](https://github.com/yahoo/fili/pull/594)
    * Fili was able to pass the build with wrong package headers in some source files. This needs to be fixed, and it's
      fixed in this PR by adding PackageDeclaration checkstyle rule.
    * In addition, checkstyle version has been bumped to the latest one(Nov, 2017), which is now able to detect more
      styling errors.

- [Add loaded strategy onto tables full view endpoint](https://github.com/yahoo/fili/pull/589)
    * Add dimension storage strategy to table full view endpoint

- [Add getter for LogicalMetricInfo in MetricInstance](https://github.com/yahoo/fili/pull/588)
    * There are 3 instance variables inside `MetricInstance` class, two of which have getters. The one without getter,
      `LogicalMetricInfo`, should have one, as well, so that subclass can access it without creating a duplicate
      `LogicalMetricInfo` inside their own.

- [Backwards compatible constructor for KeyValueStoreDimension around storage strategy]()
    * Provide a backwards compatible constructor for existing implementations that don't provide storage strategies.

- [Have Table Endpoint Filter Using QueryPlanningConstraint](https://github.com/yahoo/fili/pull/439)
    * Enable tables endpoint to fiilter availabilities based on availability-constraint

- [Implement dimension metadata to indicate storage strategy](https://github.com/yahoo/fili/pull/558)
    * In order to allow clients to be notified if a dimension's values are browsable and searchable, a storage strategy
      metadata is added to dimension.

- [Refactor ApiRequest](https://github.com/yahoo/fili/pull/538)
    * Add inteface layer to each type of API request class. The types of API request under the refactor are
        - `TablesApiRequest`
        - `DimensionApiRequest`
        - `SlicesApiRequest`
        - `MetricsApiRequest`
        - `JobsApiRequest`

- [Implement Query Split Logging](https://github.com/yahoo/fili/pull/537)
    * Include metrics in logging to allow for better evaluation of the impact of caching for split queries.
        - Currently there is only a binary flag (`BardQueryInfo.cached`) that is inconsistently set for split queries
        - Three new metrics are added
            1. Number of split queries satisfied by cache
            2. Number of split queries actually sent to the fact store. (not satisfied by cache)
            3. Number of weight-checked queries

- [Documentation that `Dimension::getFieldByName` should throw an `IllegalArgumentException` if there is no field with the passed in name](https://github.com/yahoo/fili/pull/547)

- [Evaluate format type from both URI and Accept header](https://github.com/yahoo/fili/pull/495)
    * Add a new functional interface `ResponseFormatResolver` to coalesce Accept header format type and URI format type.
    * Implement a concrete implementation of `ResponseFormatResolver` in `AbstractBindingFactory`.

- [Add Constructor and wither for TableApiRequest](https://github.com/yahoo/fili/pull/539)
    *  Making the TablesApiRequest similar to other ApiRequest classses so added an all argument constructor
       and withers. The all argument constructor is made private since its used only by the withers.

- [Add Code Narc to validate Groovy style](https://github.com/yahoo/fili/pull/420)
    * Checkstyle is great, but it doesn't process Groovy. Code Narc is Checkstyle for Groovy, so we should totally use
      it.

- [Allow Webservice to Configure Metric Long Name](https://github.com/yahoo/fili/pull/492)
    * Logical metric needs more config-richness to not just configure metric name, but also metric long name,
      description, etc. MetricInstance is now created by accepting a LogicalMetricInfo which contains all these fields
      in addition to metric name.

- [Enable search provider to hot-swap index and key value store to hot-swap store location](https://github.com/yahoo/fili/pull/522)
    * Add new default method to [`SearchProvider`](./fili-core/src/main/java/com/yahoo/bard/webservice/data/dimension/SearchProvider.java)
      interface in order to support hot-swapping index.
    * Implement the hot-swapping method of the `SearchProvider` interface in [`LuceneSearchProvider`](./fili-core/src/main/java/com/yahoo/bard/webservice/data/dimension/impl/LuceneSearchProvider.java)
          - replace Lucene index by moving the old index directory to a different location, moving new indexes to a new
            directory with the same old name, and deleting the old index directory in file system.
    * Add new default method to [`KeyValueStore`](fili-core/src/main/java/com/yahoo/bard/webservice/data/dimension/KeyValueStore.java)
      interface in order to support hot-swapping key value store location.

- [Translate doc, built-in-makers.md, to Chinese](https://github.com/yahoo/fili/pull/499)
    * Part of Fili translation in order to increase popularity of Fili in Chinese tech industries.

- [Add Uptime Status Metric](https://github.com/yahoo/fili/pull/518)
    * Add a metric to show how long Fili has been running

- [Add `druid_broker` config parameter to replace `ui_druid_broker` and `non_ui_druid_broker`](https://github.com/yahoo/fili/pull/489)

- [Have Tables Endpoint Support (but not use) Additional Query Parameters](https://github.com/yahoo/fili/pull/437)
    * Make the availability consider the TablesApiRequest by passing it into the getLogicalTableFullView method
    * Move auxiliary methods from `DataApiRequest` to `ApiRequest` in order to make them sharable between
    `DataApiRequest` and `TableApiRequest`.

- [Fili-security module](https://github.com/yahoo/fili/pull/405)
    * Added security module for fili data security filters
    * Created `ChainingRequestMapper`, and a set of mappers for gatekeeping on security roles and whitelisting dimension filters.

- [Add Table-wide Availability](https://github.com/yahoo/fili/pull/414)
    * Add `availableIntervals` field to tables endpoint by union the availability for the logical table without taking
      the TablesApiRequest into account.

- [Implement EtagCacheRequestHandler](https://github.com/yahoo/fili/pull/312)
    * Add `EtagCacheRequestHandler` that checks the cache for a matching eTag
    * Add `EtagCacheRequestHandler` to `DruidWorkflow`
    * Make `MemTupleDataCache` take parametrized meta data type

- [Implement EtagCacheResponseProcessor](https://github.com/yahoo/fili/pull/311)
    * Add `EtagCacheResponseProcessor` that caches the results if appropriate after completing a query according to
      etag value.

- [Add dimension dictionary to metric loader](https://github.com/yahoo/fili/pull/317)
    * Added a two argument version of `loadMetricDictionary` default method in `MetricLoader` interface that allows dimension
    dependent metrics by providing a dimension dictionary given by `ConfigurationLoader`


### Changed:

- [Avoid casting to generate SimplifiedIntervalList](https://github.com/yahoo/fili/pull/658)
    * Some downstream projects generated partial intervals as `ArrayList`, which cannot be cased to
      `SimplifiedIntervalList` in places like `getPartialIntervalsWithDefault`. The result is a casting exception which
      crashes downstream applications. Casting is replaced with a explicit `SimplifiedIntervalList` object creation.
      
- [ResponseProcessor is now injectable.](https://github.com/yahoo/fili/pull/663)
    * To add a custom `ResponseProcessor`, implement `ResponseProcessorFactory`, override 
        `AbstractBinderFactory::buildResponseProcessorFactory` to return your custom `ResponseProcessorFactory.class`. 

- [Add config to ignore partial/volatile intervals and cache everything in cache V2](https://github.com/yahoo/fili/pull/645)
    * In cache V2, user should be able to decide whether partial data or volatile data should be cached or not. This PR
      adds a config that allows the user to do this.

- [Lift required override on deprecated method in MetricLoader](https://github.com/yahoo/fili/pull/609)
    * Add default implementation to deprecated `loadMetricDictionary` in `MetricLoader` so that downstream projects are
      able to implement the new version without worrying about the deprecated version.

- [Added DataApiRequestFactory layer](https://github.com/yahoo/fili/issues/603)
    * Replaced static construction of DataApiRequest with an injectableFactory
    * Create an additional constructor for DataApiRequestImpl which unpacks the config resources bundle to make it easier to override dictionaries.

- [Refactored HttpResponseMaker to allow for custom ResponseData implementations](https://github.com/yahoo/fili/pull/605)
    * Currently ResponseData is being directly created in when building a response in the HttpResponseMaker. This creation
    has been extracted to a factory method, which subclasses of HttpResponseMaker can override.
    * Changed relevant methods fields from private to protected.

- [Move makeRequest to JTB](https://github.com/yahoo/fili/pull/590)
    * Move `makeRequest` from test to `JerseyTestBinder`
    * Some tests uses variable name `jerseyTestBinder`; some uses `jtb`. They are all renamed to the former for naming
      conformance
    * Re-indent testing strings for better code formatting

- [Moved availabilities to metrics construction to MetricUnionCompositeTableDefinition](https://github.com/yahoo/fili/pull/592)
    * Currently, the availability to metrics construction is taking place even before the availability is loaded. Hence, 
      moving the construction to MetricUnionCompositeTableDefinition so that availability is loaded first.

- [Better programmatic generation of metadata json in tests](https://github.com/yahoo/fili/pull/412)
    * Rework metadata tests to be more generated from strings and more pluggable to support heavier and more expressive
      testing. This allows for more consistency, as well as make it easier to test more cases.

- [Ability to use custom rate limiting schemes](https://github.com/yahoo/fili/pull/591)
    * Allows users to rate limit based on different criteria that the default criteria.
    * Existing rate limiting code is now located in `DefaultRateLimiter`.
    * Create a new rate limiter by:
        - implementing the `RateLimiter` interface
        - overriding the `buildRateLimiter` method in concrete implementation of `AbstractBinderFactory` to return
          custom `RateLimiter` implementation
        - Default token that uses a callback mechanism is available
            * `CallbackRateLimitRequestToken` takes an implementation of the callback interface `RateLimitCleanupOnRequestComplete`. When the request is completed the token calls the `cleanup` method of the callback to handle releasing any resources associate with the inflight request that this token belongs to.

- [Expose `RequestLog` `LogInfo` objects](https://github.com/yahoo/fili/pull/574)
    * Exposes the `LogInfo` objects stored in the `RequestLog`, via `RequestLog::retrieveAll` making it easier
        for customers to implement their own scheme for logging the `RequestLog`.

- [Display corrected case on StorageStrategy serialization](https://github.com/yahoo/fili/pull/578)
    * The default serialization of enum is `name()` which is final and thus cannot be overridden. An API method is added
      to return the API name of a storage strategy.

- [Made StorageStrategy lower case]

- [Make shareable methods accessiable to all types of API requests](https://github.com/yahoo/fili/pull/565)
    * As non-data endpoints are behaving more like data endpoints, some methods deserve to be shared among all types of
      API requests. Methods for

        - parsing and generating `LogicalMetrics`
        - parsing and generating `LogicalTable`
        - computing the union of constrained availabilities of constrained logical table

      are made available.

- [Substitute preflight method wildcard character with explicit allowed methods](https://github.com/yahoo/fili/pull/545)
    * Modify ResponseCorsFilter Allowed Methods header to explicitly list allowed methods. Some browsers do not support a wildcard header value.

- [Make Field Accessor PostAggregation able to reference post aggregations in adddition to aggregations]
    * Druid allows (but does not protect against ordering) post aggregation trees referencing columns that are also post aggregation trees.  This makes it possible to send such a query by using a field accessor to reference another query expression.  Using this capability may have some risk.

- [Include ETags on responses](https://github.com/yahoo/fili/pull/417)
    * Modify FullResponse JSON Objects to contain a flag showing whether a response is new or fetched from cache.

- [Fix wrong default druid url and broken getInnerMostQuery](https://github.com/yahoo/fili/pull/528)
    * Comment out the wrong default druid broker url in module config that break old url config compatibility, add check for validate url in `DruidClientConfigHelper `
    * Fix broken `getInnermostQuery` method in `DruidQuery`

- [Rename filter variables and methods in DataApiRequest](https://github.com/yahoo/fili/pull/507)
    * The method names `getFilter` and `getFilters` can be confusing, as well as the `filters` variable

- [Decoupled from static dimension lookup building]()
    * Instead of `ModelUtils`, create an interface for `ExtractionFunctionDimension` and rebase `LookupDimension` and `RegisteredLookupDimension` on that interface.
    * `LookupDimensionToDimensionSpec` now uses only the Extraction interface to decide how to serialize dimensions.

- [DruidDimensionLoader is now a more generic DimensionValueLoadTask](https://github.com/yahoo/fili/pull/449)
    * The `DimensionValueLoadTask` takes in a collection of `DimensionValueLoader`s to allow for non-Druid dimensions to be loaded.

- [DruidQuery::getInnerQuery and Datasource::getQuery return Optional](https://github.com/yahoo/fili/pull/485)
    * Returning `Optional` is more correct for their usage and should protect against unexpected null values.

- [Use all available segment metadata in fili-generic-example](https://github.com/yahoo/fili/pull/445)
    * The fili-generic-example now uses all segment metadata given by Druid instead of just the first one and also provides it to the metadata service.

- [Refactor Response class and implement new serialization logics](https://github.com/yahoo/fili/pull/455)
    * Define interface `ResponseWriter` and its default implementation
    * Refactor `Response` class, splitting into `ResponseData` and three implementations of `ResponseWriter`
    * Define interface `ResponseWriterSelector` and its default implementation.
    * Hook up the new serialization logic with `HttpResponseMaker` to replace the old one

- [LuceneSearchProvide needs to handle nulls](https://github.com/yahoo/fili/issues/487)
    * Lucene search provider cannot handle null load values.  Treat all null values as empty string.

- [Make AvroDimensionRowParser.parseAvroFileDimensionRows support consumer model](https://github.com/yahoo/fili/issues/483)
    * In order to do deferred/buffered file reading, create a call back style method.

- [Make HttpResponseMaker injectable and change functions signature related to custom response creation](https://github.com/yahoo/fili/pull/447)
    * Make `HttpResponseMaker` injectable. `DataServlet` and `JobsServlet` takes `HttpResponseMaker` as input parameter now
    * Add `ApiRequest` to `BuildResponse`, `HttpReponseChannel` and `createResponseBuilder` to enable passing information needed by customizable serialization
    * Remove duplicate parameter such as `UriInfo` that can be derived from `ApiRequest`

- [Change id field in DefaultDimensionField to lower case for Navi compatibility.](https://github.com/yahoo/fili/pull/423)
    * Navi's default setting only recongizes lower case 'id' key name.

- [Fix a bug where table loader uses nested compute if absent](https://github.com/yahoo/fili/pull/407)
    * Nesting `computeIfAbsent` on maps can cause a lot of issues in the map internals that causes weird behavior, nesting structure is now removed

- [Convert null avro record value to empty string](https://github.com/yahoo/fili/pull/395)
    * Make `AvroDimensionRowParser` convert null record value into empty string to avoid NPE

- [FailedFuture is replaced by CompletedFuture](https://github.com/yahoo/fili/pull/396)
    * CompletedFuture allows values to be returned when calling `.get` on a future instead of just throwing an exception

### Deprecated:

- [Extraction Function on selector filter](https://github.com/yahoo/fili/pull/617)
    * Deprecated `ExtractionFilter` since it is deprecated in druid, use selector filter with extraction function instead

- [Rename filter variables and methods in DataApiRequest](https://github.com/yahoo/fili/pull/507)
    * Deprecated `getFilters` in favor of `getApiFilters` and `getFilter` in favor of `getDruidFilter`

- [Deprecate `ui_druid_broker` and `non_ui_druid_broker` and added `druid_broker`](https://github.com/yahoo/fili/pull/489)

- [Add dimension dictionary to metric loader](https://github.com/yahoo/fili/pull/317)
    * Deprecated single argument version of `loadMetricDictionary` in `MetricLoader`, favor additional dimension dictionary argument `loadMetricDictionary` instead


### Fixed:

- [Correct exception message & add missing tests](https://github.com/yahoo/fili/pull/649)
    * Clarified exception message thrown by `StreamUtils.throwingMerger`
      
- [Fix lookup metadata loader by pulling the RegisteredLookupDimension](https://github.com/yahoo/fili/pull/651)
    * Lookup Metadata Health Check always return true when some Druid registered lookup are absolutely failing to be
      loaded. Instead of checking load status of `RegisteredLookupDimension`, `RegisteredLookupMetadataLoadTask` is
      checking the status of `LookupDimension`. This PR corrects this behavior.

- [Fix 'descriptionription' mis-naming in dimension field](https://github.com/yahoo/fili/pull/655)
    * This is caused by a "desc" -> "description" string replacement. A string handling method has been added to
      detect "desc" and transform it to "description". If it already comes with "description", no string transformation
      is made

- [Fix caching condition](https://github.com/yahoo/fili/pull/647)
    * We want to cache partial or volatile data when `cache_partial_data` is set to true. This is condition is currently
      reversed. This PR shall fix it

- [Add Missing perPage Param](https://github.com/yahoo/fili/pull/641)
    * Pagination links on the first pages are missing perPage param. This PR fixes this problem.

- [Having clause was nesting inward on nested queries resulting in rows that didn't exist being referenced](https://github.com/yahoo/fili/pull/614/files)

- [None show clause was not being respected](https://github.com/yahoo/fili/issues/612)
    * Changed `ResponseData` and `JsonApiResponseWriter` to suppress columns that don't have associated dimension fields.
    * Updated tests to reflect none being hidden.
     
- [Scoped metric dictionaries and the having clause now work together by default](https://github.com/yahoo/fili/pull/580)
    * Add a new ApiHavingGenerator that builds a temporary metric dictionary from the set of requested metrics(not from globally scoped metric dictionary), and then using those to resolve the having clause.
    * Add a table generating functions in BaseTableLoader that effectively allow the customer to provide a different metric dictionary at lower scope(not from the globally scoped metric dictionary) for use when building each table.

- [Debug BardQueryInfo to show query split counting](https://github.com/yahoo/fili/pull/596/files)
    * Query counter in `BardQueryInfo` does not show up in logging because the counter used to be static and JSON
      serializer does not serialize static fields.
    * This externalizes the state via a getter for serialization.

- [Fix intermittent class scanner error on DataSourceConstraint equal](https://github.com/yahoo/fili/pull/573)
   * Class Scanner Spec was injecting an improper dependant field due to type erasure.  Made field type explicit.

- [Fix tests with wrong time offset calculation](https://github.com/yahoo/fili/pull/567)
    * Time-checking based tests setup time offset in a wrong way. `timeZoneId.getOffset` is fixed to take the right
      argument.

- [Handle Number Format errors from empty or missing cardinality value](https://github.com/yahoo/fili/issues/549)

- [Fix lucene search provider replace method](https://github.com/yahoo/fili/pull/551)
    * Reopen the search index

- [Fix ConstantMaker make method with LogicalMetricInfo class](https://github.com/yahoo/fili/pull/540)
    * The ConstantMaker make method needs to be rewritten with the LogicalMetricInfo class.

- [Slices endpoint returns druid name instead of api name](https://github.com/yahoo/fili/pull/491)
    * The slices endpoint now gives the druid name instead of the api name for dimensions.

- [Prevent NPE in test due to null instance variables in DataApiRequest](https://github.com/yahoo/fili/pull/532)
    * A particular order of loading [`ClassScannerSpec`](./fili-core/src/test/groovy/com/yahoo/bard/webservice/util/ClassScannerSpec.groovy)
      classes results in `NullPointerException` and fails tests, because some instance variables from testing [`DataApiRequest`](./fili-core/src/main/java/com/yahoo/bard/webservice/web/DataApiRequest.java)
      are null. This patch assigns non-null values to those variables.
    * The testing constructor `DataApiRequestImpl()` is now deprecated and will be removed entirely.

- [Fix Lucene Cardinality in New KeyValueStores](https://github.com/yahoo/fili/pull/521)
    * Fix lucene to put correct cardinality value to new key value store that does not contain the cardinality key

- [Log stack trace at error on unexpected DimensionServlet failures](https://github.com/yahoo/fili/pull/425)
    * DimensionServlet was using debug to log unexpected exceptions and not printing the stack trace

- [Fix datasource name physical table name mismatch in VolatileDataRequestHandler](https://github.com/yahoo/fili/issues/505)
    * Fix fetching from `physicaltableDictionary` using datasource name. Now use proper physical table name instead.

- [Fix performance bug around feature flag](https://github.com/yahoo/fili/issues/473)
    * BardFeatureFlag, when used in a tight loop, is very expensive.  Underlying map configuration copies the config map on each access.
    * Switching to lazy value evaluation
    * Added reset contract so changes to feature flags can be directly reverted rather than going through the `SystemConfig` directly

- [Fix deploy branch issue where substrings of whitelisted branches could be released](https://github.com/yahoo/fili/issues/453)

- [Fix availability testing utils to be compatible with composite tables](https://github.com/yahoo/fili/pull/419)
    * Fix availability testing utils `populatePhysicalTableCacheIntervals` to assign a `TestAvailability` that will serialize correctly instead of always `StrictAvailability`
    * Fix internal representation of `VolatileIntervalsFunction` in `DefaultingVolatileIntervalsService` from `Map<PhysicalTable, VolatileIntervalsFunction>` to `Map<String, VolatileIntervalsFunction>`

- [Fix metric and dimension names for wikipedia-example](https://github.com/yahoo/fili/pull/415)
    * The metrics and dimensions configured in the `fili-wikipedia-example` were different from those in Druid and as a result the queries sent to Druid were incorrect

### Known Issues:



### Removed:

- [Remove testing constructor of *ApiRequestImpl](https://github.com/yahoo/fili/pull/559)
    * It is a better practice to separate testing code with implementation. All testing constructors of the following
      API requests are removed:
        - `ApiRequestImpl`
        - `DataApiRequestImpl`
        - `DimensionsApiRequestImpl`
        - `MetricsApiRequestImpl`
        - `SlicesApiRequestImpl`
        - `TablesApiRequestImpl`
    * Meanwhile, construction of testing API request is delegated to testing class, e.g. `TestingDataApiRequestImpl`

- [Reverted the druid name change in slices endpoint instead added to factName](https://github.com/yahoo/fili/pull/541)
    * Reverting the PR-419(https://github.com/yahoo/fili/pull/419) so that the name still points to apiName and added factName which points to druidName.
      `name` was not valid for cases when it is a Lookup dimension because it was pointing to the base dimension name , so reverted that change and added
      `druidName` which is the actual druid fact name and `name` being the apiName

- [Remove custom immutable collections in favor of Guava](https://github.com/yahoo/fili/pull/479)
    * `Utils.makeImmutable(...)` was misleading and uneeded so it has been removed. Use Guava's immutable collections.

- [Remove dependency on org.apache.httpcomponents](https://github.com/yahoo/fili/pull/482)
    * This library was only used in `fili-wikipedia-example` and has been replaced with AsyncHttpClient.

- [Remove dependency on org.json](https://github.com/yahoo/fili/pull/416)
    * Replace uses of org.json with the jackson equivalent

- [Remove NO_INTERVALS from SimplifiedIntervalList](https://github.com/yahoo/fili/pull/290)
    * This shared instance was vulnerable to being changed globally. All calls to this have been replaced with the empty constructor


v0.8.69 - 2017/06/06
--------------------

The main changes in this version are changes to the Table and Schema structure, including a major refactoring of
Physical Table. The concept of Availability was split off from Physical Table, allowing Fili to better reason about
availability of columns in Data Sources in ways that it couldn't easily do before, like in the case of Unions. As part
of this refactor, Fili also gains 1st-class support for queries using the Union data source.

<sub>Full description of changes to Tables, Schemas, Physical Tables, Availability, PartialDataHandler, etc. tbd</sub>

This was a long and winding journey this cycle, so the changelog is not nearly as tight as we'd like (hopefully we'll
come back and consolidate it for this release), but all of the changes are in there. Along the way, we also addressed
a number of other small concerns. Here are some of the _highlights_ beyond the main changes around Physical Tables:

Fixes:
- Unicode characters are now properly sent back to Druid
- Druid client now follows redirects

New Capabilities & Enhancements:
- Can sort on `dateTime`
- Can use Druid query response for final verification of response partiality
- Class Scanner Spec can discover dependencies, making its dynamic equality testing easier to use
- There's an example application that shows how to slurp configuration from an existing Druid instance
- Druid queries return a `Future` instead of `void`, allowing for blocking requests if needed (though use sparingly!)
- Support for extensions defining new Druid query types

Performance upgrades:
- Lazy DruidFilters
- Assorted log level reductions
- Lucene "total results" 50% speedup

Deprecations:
- `DataSource::getDataSources` no longer makes sense, since `UnionDataSource` only supports 1 table now
- `BaseTableLoader::loadPhysicalTable`. Use `loadPhysicalTablesWithDependency` instead
- `LogicalMetricColumn` isn't really a needed concept

Removals:
- `PartialDataHandler::findMissingRequestTimeGrainIntervals`
- `permissive_column_availability_enabled` feature flag, since the new `Availability` infrastructure now handles this
- Lots of things on `PhysicalTable`, since that system was majorly overhauled
- `SegmentMetadataLoader`, which had been deprecated for a while and relies on no longer supported Druid features


### Added:

- [Implement DruidPartialDataRequestHandler](https://github.com/yahoo/fili/pull/287)
    * Implement `DruidPartialDataRequestHandler` that injects `druid_uncovered_interval_limit` into Druid query context
    * Append `DruidPartialDataResponseProcessor` to the current next `ResponseProcessor` chain
    * Add `DruidPartialDataRequestHandler` to `DruidWorkflow` between `AsyncDruidRequestHandler` and
      `CacheV2RequestHandler` and invoke the `DruidPartialDataRequestHandler` if `druid_uncovered_interval_limit` is
      greater than 0

- [Prepare for etag Cache](https://github.com/yahoo/fili/pull/289)
    * Deprecate Cache V1 and V2 and log warning wherever they are used in codebase
    * Add config param `query_response_caching_strategy` that allows any one of the TTL cache, local signature cache, or
      etag cache to be used as caching strategy
    * Add 'CacheMode' that represent the caching strategy
    * Add 'DefaultCacheMode' that represents all available caching strategies
    * Make `AsyncDruidWebServiceImpl::sendRequest` not blow up when getting a 304 status response if etag cache is on
    * Add etag header to response JSON if etag cache is set to be used
    * Add `FeatureFlag::isSet` to expose whether feature flags have been explicitly configured

- [Implement DruidPartialDataResponseProcessor](https://github.com/yahoo/fili/pull/275)
    * Add `FullResponseProcessor` interface that extends `ResponseProcessor`
    * Add response status code to JSON response
    * Add `DruidPartialDataResponseProcessor` that checks for any missing data that's not being found

- [Add `DataSourceName` concept, removing responsibility from `TableName`](https://github.com/yahoo/fili/pull/263)
    * `TableName` was serving double-duty, and it was causing problems and confusion. Splitting the concepts fixes it.

- [Add a `BaseMetadataAvailability` as a parallel to `BaseCompositeAvailability`](https://github.com/yahoo/fili/pull/263)
    * `Concrete` and `PermissiveAvailability` both extend this new base `Availability`

- [Constrained Table Support for Table Serialization](https://github.com/yahoo/fili/pull/262)
    * Add ConstrainedTable which closes over a physical table and an availability, caching all availability merges.
    * Add PartialDataHandler method to use `ConstrainedTable`

- [Testing: ClassScannerSpec now supports 'discoverable' depenencies ](https://github.com/yahoo/fili/pull/262)
    * Creating `supplyDependencies` method on a class's spec allows definitions of dependencies for dynamic equality
      testing

- [Moved UnionDataSource to support only single tables](https://github.com/yahoo/fili/pull/262)
    * `DataSource` now supports getDataSource() operation

- [Prepare For Partial Data V2](https://github.com/yahoo/fili/pull/264)
    * Add new query context for druid's uncovered interval feature
    * Add a configurable property named "druid_uncovered_interval_limit"
    * Add new response error messages as needed by Partial Data V2

- [Merge Druid Response Header into Druid Response Body Json Node in AsyncDruidWebServiceImplV2](https://github.com/yahoo/fili/pull/267)
    * Add configuration to `AsyncDruidWebServiceImpl` so that we can opt-in configuration of JSON response content.
    * `AsyncDruidWebServiceImpl` takes a strategy for building the JSON from the entire response.

- [Add MetricUnionCompositeTableDefinition](https://github.com/yahoo/fili/pull/258)

- [Add partition availability and table](https://github.com/yahoo/fili/pull/244)

- [Add constructor to specify DruidDimensionLoader dimensions directly](https://github.com/yahoo/fili/pull/191)

- [Add `IntervalUtils::getTimeGrain` to determine the grain given an Interval](https://github.com/yahoo/fili/pull/191)

- [Add example for slurping in config from a Druid instance](https://github.com/yahoo/fili/pull/191)

- [Add Permissive Concrete Physical Table Definition](https://github.com/yahoo/fili/pull/212)
    * Added `PermissiveConcretePhysicalTableDefinition` for defining a `PermissiveConcretePhysicalTable`

- [Fix to use physical name instead of logical name to retrieve available interval](https://github.com/yahoo/fili/pull/226)
    * Added `PhysicalDataSourceConstraint` class to capture physical names of columns for retrieving available intervals

- [BaseCompositePhysicalTable](https://github.com/yahoo/fili/pull/242)
    * `BaseCompositePhysicalTable` provides common operations, such as validating coarsest ZonedTimeGrain, for composite
    tables.

- [Add Reciprocal `satisfies()` relationship complementing `satisfiedBy()` on Granularity](https://github.com/yahoo/fili/issues/222)

- [Add a timer around DataApiRequestMappers](https://github.com/yahoo/fili/pull/250)

- [MetricUnionAvailability and MetricUnionCompositeTable](https://github.com/yahoo/fili/pull/193)
    * Added `MetricUnionAvailability` which puts metric columns of different availabilities together and
    `MetricUnionCompositeTable` which puts metric columns of different tables together in a single table.

- [Method for finding coarsest ZonedTimeGrain](https://github.com/yahoo/fili/pull/230)
    * Added utility method for returning coarsest `ZonedTimeGrain` from a collection of `ZonedTimeGrain`s. This is
      useful to construct composite tables that requires the coarsest `ZonedTimeGrain` among a set of tables.

- [Should also setConnectTimeout when using setReadTimeout](https://github.com/yahoo/fili/pull/231)
    * Setting connectTimeout on DefaultAsyncHttpClientConfig when building AsyncDruidWebServiceImpl

- [CompositePhysicalTable Core Components Refactor](https://github.com/yahoo/fili/pull/179)
    * Added `ConcretePhysicalTable` and `ConcreteAvailability` to model table in druid datasource and its availability
      in the new table availability structure
    * Added class variable for `DataSourceMetadataService` and `ConfigurationLoader` into `AbstractBinderFactory` for
      application to access
    * Added `loadPhysicalTablesWithDependency` into `BaseTableLoader` to load physical tables with dependencies

- [PermissiveAvailability and PermissiveConcretePhysicalTable](https://github.com/yahoo/fili/pull/190)
    * Added `PermissiveConcretePhysicalTable` and `PermissiveAvailability` to model table in druid datasource and its
      availability in the new table availability structure. `PermissiveAvailability` differs from `ConcreteAvailability`
      in the way of returning available intervals: `ConcreteAvailability` returns the available intervals constraint by
      `DataSourceConstraint` and provides them in intersection. `PermissiveAvailability`, however, returns them without
      constraint from `DataSourceConstraint` and provides them in union. `PermissiveConcretePhysicalTable` is different
      from `ConcretePhysicalTable` in that the former is backed by `PermissiveAvailability` while the latter is backed
      by `ConcreteAvailability`.

- [Refactor DatasourceMetaDataService to fit composite table needs](https://github.com/yahoo/fili/pull/173)
    * `DataSourceMetadataService` also stores interval data from segment data as intervals by column name map and
      provides method `getAvailableIntervalsByTable` to retrieve it

- [QueryPlanningConstraint and DataSourceConstraint](https://github.com/yahoo/fili/pull/169)
    * Added `QueryPlanningConstraint` to replace current interface of Matchers and Resolvers arguments during query
      planning
    * Added `DataSourceConstraint` to allow implementation of `PartitionedFactTable`'s availability in the near future

- [Major refactor for availability and schemas and tables](https://github.com/yahoo/fili/pull/165)
    * `ImmutableAvailability` - provides immutable, typed replacement for maps of column availabilities
    * New Table Implementations:
	    * `BasePhysicalTable` core implementation
	    * `ConcretePhysicalTable` creates an ImmutableAvailability
    * `Schema` implementations
	    * `BaseSchema` has `Columns`, `Granularity`
    	* `PhysicalTableSchema` has base plus `ZonedTimeGrain`, name mappings
	    * `LogicalTableSchema` base with builder from table group
	    * `ResultSetSchema` base with transforming with-ers
    * `ApiName`, `TableName`: Added static factory from String to Name
    * `ErrorMessageFormat` for errors during `ResultSetMapper` cycle

- [Added default base class for all dimension types](https://github.com/yahoo/fili/pull/177)
   * Added base classes `DefaultKeyValueStoreDimensionConfig`, `DefaultLookupDimensionConfig` and
     `DefaultRegisteredLookupDimensionConfig` to create default dimensions.

- [dateTime based sort feature for the final ResultSet added](https://github.com/yahoo/fili/pull/178)
   * Now we support dateTime column based sort in ASC or DESC order.
   * Added `DateTimeSortMapper` to sort the time buckets and `DateTimeSortRequestHandler` to inject to the workflow

- [dateTime specified as sortable field in sorting clause](https://github.com/yahoo/fili/pull/170)
    * added `dateTimeSort` as class parameter in `DataApiRequest`. So it can be tracked down to decide the resultSet
      sorting direction.

- [Detect unset userPrincipal in Preface log block](https://github.com/yahoo/fili/pull/154)
    * Logs a warning if no userPrincipal is set on the request (ie. we don't know who the user is), and sets the `user`
      field in the `Preface` log block to `NO_USER_PRINCIPAL`.

- [Support timeouts for lucene search provider](https://github.com/yahoo/fili/pull/183)

### Changed:

- [Prepare for etag Cache](https://github.com/yahoo/fili/pull/289)
    * Made isOn dynamic on BardFeatureFlag

- [Rename `Concrete` to `Strict` for the respective `PhysicalTable` and `Availability`](https://github.com/yahoo/fili/pull/263)
    * The main difference is in the availability reduction, so make the class name match that.

- [Make `PermissiveConcretePhysicalTable` and `ConcretePhysicalTable` extend from a common base](https://github.com/yahoo/fili/pull/263)
    * Makes the structure match that for composite tables, so they can be reasoned about together.

- [Make `PermissiveConcretePhysicalTable` (now just `PermissivePhysicalTable`) a sibling, instead of extend from, `ConcretePhysicalTable`](https://github.com/yahoo/fili/pull/263)
    * The main difference is in the accepted availabilities, so make the class structure match that.

- [Make `MetricUnionAvailability` take a set of `Availability` instead of `PhysicalTable`](https://github.com/yahoo/fili/pull/263)
    * Since it was just unwrapping anyways, simplifying the dependency and pushing the unwrap up-stream makes sense

- [Add `DataSourceName` concept, removing responsibility from `TableName`](https://github.com/yahoo/fili/pull/263)
    * Impacts:
        - `DataSource` & children
        - `DataSourceMetadataService` & `DataSourceMetadataLoader`
        - `SegmentIntervalsHashIdGenerator`
        - `PhysicalTable` & children
        - `Availability` & children
        - `ErrorMessageFormat`
        - `SlicesApiRequest`

- [Force `ConcretePhysicalTable` only take a `ConcreteAvailability`](https://github.com/yahoo/fili/pull/263)
    * Only a `ConcreteAvailability` makes sense, so let the types enforce it

- [Clarify name of built-in static `TableName` comparator](https://github.com/yahoo/fili/pull/263)
    * Change to `AS_NAME_COMPARATOR`

- [Constrained Table Support for Table Serialization](https://github.com/yahoo/fili/pull/262)
    * Switched `PartialDataRequestHandler` to use the table from the query rather than the `PhysicalTableDictionary`
    * `DruidQueryBuilder` uses constrained tables to dynamically pick between Union and Table DataSource implementations
    * `PartialDataHandler` has multiple different entry points now depending on pre or post constraint conditions
    * `getAvailability` moved to a `ConfigTable` interface and all configured Tables to that interface
    * DataSource implementations bind to `ConstrainedTable` and only ConstrainedTable is used after table selection
    * `PhysicalTable.getAllAvailableIntervals` explicitly rather than implicitly uses `SimplifiedIntervalList`
    * Bound and default versions of getAvailableIntervals and getAllAvailableIntervals added to PhysicalTable interface
    * Package-private optimize tests in `DruidQueryBuilder` moved to protected
    * Immutable `NoVolatileIntervalsFunction` class made final

- [Moved UnionDataSource to support only single tables](https://github.com/yahoo/fili/pull/262)
    * `UnionDataSource` now accepts only single tables instead of sets of tables.
    * `DataSource` now supports `getDataSource()` operation
    * `IntervalUtils.collectBucketedIntervalsNotInIntervalList` moved to `PartialDataHandler`

- [Druid filters are now lazy](https://github.com/yahoo/fili/pull/269)
    * The Druid filter is built when requested, NOT at DatApiRequest construction. This will make it easier to write
      performant `DataApiRequest` mappers.

- [Reduce log level of failure to store a result in the asynchronous job store](https://github.com/yahoo/fili/pull/266)
    * Customers who aren't using the asynchronous infrastructure shouldn't be seeing spurious warnings about a failure
      to execute one step (which is a no-op for them) in a complex system they aren't using. Until we can revisit how we
      log report asynchronous errors, we reduce the log level to `DEBUG` to reduce noise.

- [Clean up `BaseDataSourceComponentSpec`](https://github.com/yahoo/fili/pull/263)
    * Drop a log from `error` to `trace` when a response comes back as an error
    * Make JSON validation helpers return `boolean` instead of `def`

- [Make `BasePhysicalTable` take a more extension-friendly set of `PhysicalTable`s](https://github.com/yahoo/fili/pull/263)
    * Take `<? extends PhysicalTable>` instead of just `PhysicalTable`

- [Update availabilities for PartitionAvailability](https://github.com/yahoo/fili/pull/244)
    * Created `BaseCompositeAvailability` for common features
    * Refactored `DataSourceMetadataService` methods to use `SimplifiedIntervaList` to standardize intersections

- [Queries to Druid Web Service now return a Future](https://github.com/yahoo/fili/pull/254)
    * Queries now return a `Future<Response>` in addition to having method callbacks.

- [Refactor Physical Table Definition and Update Table Loader](https://github.com/yahoo/fili/pull/207)
    * `PhysicalTableDefinition` is now an abstract class, construct using `ConcretePhysicalTableDefinition` instead
    * `PhysicalTableDefinition` now requires a `build` methods to be implemented that builds a physical table
    * `BaseTableLoader` now constructs physical tables by calling `PhysicalTableDefinition::build` in
      `buildPhysicalTablesWithDependency`
    * `BaseTableLoader::buildDimensionSpanningTableGroup` now uses `loadPhysicalTablesWithDependency` instead of
      deprecated `loadPhysicalTables`
    * `BaseTableLoader::buildDimensionSpanningTableGroup` now does not take druid metrics as arguments, instead
      `PhysicalTableDefinition` does

- [Fix to use physical name instead of logical name to retrieve available interval](https://github.com/yahoo/fili/pull/226)
    * `ConcreteAvailability::getAllAvailableIntervals` no longer filters out un-configured columns, instead
      `Physicaltable::getAllAvailableIntervals` does
    * `Availability::getAvailableIntervals` now takes `PhysicalDataSourceConstraint` instead of `DataSourceConstraint`
    * `Availability` no longer takes a set of columns on the table, only table needs to know
    * `Availability::getAllAvailableIntervals` now returns a map of column physical name string to interval list instead
      of column to interval list
    * `TestDataSourceMetadataService` now takes map from string to list of intervals instead of column to list of
      intervals for constructor

- [Reduced number of queries sent by `LuceneSearchProvider` by 50% in the common case](https://github.com/yahoo/fili/pull/234)
    * Before, we were using `IndexSearcher::count` to get the total number of documents, which spawned an entire second
      query (so two Lucene queries rather than one when requesting the first page of results). We now pull that
      information from the results of the query directly.

- [Allow GranularityComparator to return static instance](https://github.com/yahoo/fili/pull/225)
    * Implementation of [PR #193](https://github.com/yahoo/fili/pull/193) suggests an possible improvement on
      `GranularityComparator`: Put the static instance on the `GranularityComparator` class itself, so that everywhere
      in the system that wants it could just call `GranularityComparator.getInstance()`

- [Make `TemplateDruidQuery::getMetricField` get the first field instead of any field](https://github.com/yahoo/fili/pull/210)
    * Previously, order was by luck, now it's by the contract of `findFirst`

- Clean up config loading and add more logs and checks
    * Use correct logger in `ConfigurationGraph` (was `ConfigResourceLoader`)
    * Add error / logging messages for module dependency indicator
    * Tweak loading resources debug log to read better
    * Tweak module found log to read better
    * Convert from `Resource::getFilename` to `Resource::getDescription` when reporting errors in the configuration
      graph. `getDescription` is more informative, usually holding the whole file path, rather than just the terminal
      segment / file _name_

- [Restore non-default query support in TestDruidWebservice](https://github.com/yahoo/fili/pull/202)

- [Base TableDataSource serialization on ConcretePhysicalTable fact name](https://github.com/yahoo/fili/pull/202)

- [CompositePhsyicalTable Core Components Refactor](https://github.com/yahoo/fili/pull/179)
    * `TableLoader` now takes an additional constructor argument (`DataSourceMetadataService`) for creating tables
    * `PartialDataHandler::findMissingRequestTimeGrainIntervals` now takes `DataSourceConstraint`
    * Renamed `buildTableGroup` method to `buildDimensionSpanningTableGroup`

- [Restored flexibility about columns for query from DruidResponseParser](https://github.com/yahoo/fili/pull/198)
    * Immutable schemas prevented custom query types from changing `ResultSetSchema` columns.
    * Columns are now sourced from `DruidResponseParser` and default implemented on `DruidAggregationQuery`

- [Refactor DatasourceMetaDataService to fit composite table needs](https://github.com/yahoo/fili/pull/173)
    * `BasePhysicalTable` now stores table name as the `TableName` instead of `String`
    * `SegmentInfo` now stores dimension and metrics from segment data for constructing column to available interval map

- [`QueryPlanningConstraint` and `DataSourceConstraint`](https://github.com/yahoo/fili/pull/169)
    * `QueryPlanningConstraint` replaces current interface of Matchers and Resolvers `DataApiRequest` and
      `TemplateDruidQuery` arguments during query planning
    * Modified `findMissingTimeGrainIntervals` method in `PartialDataHandler` to take a set of columns instead of
      `DataApiRequest` and `DruidAggregationQuery`

- [Major refactor for availability and schemas and tables](https://github.com/yahoo/fili/pull/165)
    * `Schema` and `Table` became interfaces
	    * `Table` has-a `Schema`
	    * `PhysicalTable` extends `Table`, interface only supports read-only operations
	* `Schema` constructed as immutable, `Column`s no longer bind to `Schema`
		* Removed `addNew*Column` method
	* `Schema` implementations now: `BaseSchema`, `PhysicalTableSchema`, `LogicalTableSchema`, `ResultSetSchema`
    * `DimensionLoader` uses `ConcretePhysicalTable`
    * `PhysicalTableDefinition` made some fields private, accepts iterables, returns immutable dimensions
    * `ResultSet` constructor parameter order swapped
    * `ResultSetMapper` now depends on `ResultSetSchema`
    * `TableDataSource` constructor arg narrows: `PhysicalTable` -> `ConcreteTable`
    * `DataApiRequest` constructor arg narrows: `Table` -> `LogicalTable`
    * `DruidQueryBuilder` now polymorphic on building data sources models from new physical tables
    * `ApiFilter` schema validation moved to `DataApiRequest`
    * **Guava version bumped to 21.0**

- [Request IDs now support underscores.](https://github.com/yahoo/fili/pull/176)

- Added support for extensions defining new Query types
    * `TestDruidWebService` assumes unknown query types behave like `GroupBy`, `TimeSeries`, and `TopN`
    * `ResultSetResponseProcessor` delegates to `DruidResponseProcessor` to build expected query schema, allowing
      subclasses to override and extend the schema behavior

- [Add dimension fields to fullView table format](https://github.com/yahoo/fili/pull/155)

- [Make HealthCheckFilter reject message nicer](https://github.com/yahoo/fili/pull/153)
    * The previous message of `reject <url>` wasn't helpful, useful, nor very nice to users, and the message logged was
      not very useful either. The message has been made nicer (`Service is unhealthy. At least 1 healthcheck is
      failing`), and the log has been made better as well.

- [`RequestLog` timings support the try-with-resources block](https://github.com/yahoo/fili/pull/143)
    * A block of code can now be timed by wrapping the timed block in a try-with-resources block that starts the timer.
      Note: This won't work when performing timings across threads, or across contexts. Those need to be started and
      stopped manually.

- [Clean up logging and responses in `DimensionCacheLoaderServlet`](https://github.com/yahoo/fili/pull/163)
    * Switched a number of `error`-level logs to `debug` level to line up with logging guidance when request failures
      were result of client error
    * Reduced some `info`-level logs down to `debug`
    * Converted to 404 when error was cause by not finding a path element metadata generated by query we run to get the
      actual results.

- [Update LogBack version 1.1.7 -> 1.2.3](https://github.com/yahoo/fili/pull/235)
    * In web-applications, logback-classic will automatically install a listener which will stop the logging context and
      release resources when your web-app is reloaded.
    * Logback-classic now searches for the file `logback-test.xml`, then `logback.groovy`, and then `logback.xml`.
      In previous versions `logback.groovy` was looked up first which was non-sensical in presence of `logback-test.xml`
    * `AsyncAppender` no longer drops events when the current thread has its interrupt flag set.
    * Critical parts of the code now use `COWArrayList`, a custom developed allocation-free lock-free thread-safe
      implementation of the `List` interface. It is optimized for cases where iterations over the list vastly outnumber
      modifications on the list. It is based on `CopyOnWriteArrayList` but allows allocation-free iterations over the
      list.

- [Update Metrics version 3.1.2 -> 3.2.2](https://github.com/yahoo/fili/pull/235)
    * [Added support for disabling reporting of metric attributes.](https://github.com/dropwizard/metrics/pull/1048)
    * [Support for setting a custom initial delay for reporters.](https://github.com/dropwizard/metrics/pull/999)
    * [Support for custom details in a result of a health check.](https://github.com/dropwizard/metrics/issues/663)
    * [Support for asynchronous health checks](https://github.com/dropwizard/metrics/pull/1077)
    * [Added a listener for health checks.](https://github.com/dropwizard/metrics/pull/1068)
    * [Health checks are reported as unhealthy on exceptions.](https://github.com/dropwizard/metrics/issues/783)
    * [Added support for Jetty 9.3 and higher.](https://github.com/dropwizard/metrics/pull/1038)
    * [Shutdown health check registry](https://github.com/dropwizard/metrics/pull/1084)
    * [Add support for the default shared health check registry name](https://github.com/dropwizard/metrics/pull/1095)

- [Update hk2 version 2.5.0-b05 -> 2.5.0-b36](https://github.com/yahoo/fili/pull/235)
    * [Add class-proxy in the case of a generic factory using a registered contract](https://github.com/hk2-project/hk2/releases)

- [Update SLF4J version 1.7.21 -> 1.7.25](https://github.com/yahoo/fili/pull/235)
    * When running under Java 9, log4j version 1.2.x is unable to correctly parse the "java.version" system property.
      Assuming an incorrect Java version, it proceeded to disable its MDC functionality. The slf4j-log4j12 module
      shipping in this release fixes the issue by tweaking MDC internals by reflection, allowing log4j to run under Java
      9.
    * The slf4j-simple module now uses the latest reference to `System.out` or `System.err`.
    * In slf4j-simple module, added a configuration option to enable/disable caching of the System.out/err target.

- [Update Lucene version 5.3.0 -> 6.5.0](https://github.com/yahoo/fili/pull/233)
    * [Added `IndexSearcher::getQueryCache` and `getQueryCachingPolicy`](https://issues.apache.org/jira/browse/LUCENE-6838)
    * [`org.apache.lucene.search.Filter` is now deprecated](http://issues.apache.org/jira/browse/LUCENE-6301).
      You should use `Query` objects instead of Filters, and the `BooleanClause.Occur.FILTER` clause in order to let
      Lucene know that a `Query` should be used for filtering but not scoring.
    * [`MatchAllDocsQuery` now has a dedicated `BulkScorer` for better performance when used as a top-level query.](http://issues.apache.org/jira/browse/LUCENE-6756)
    * [Added a `IndexWriter::getFieldNames` method (experimental) to return all field names as visible from the `IndexWriter`](http://issues.apache.org/jira/browse/LUCENE-7659).
      This would be useful for `IndexWriter::updateDocValues` calls, to prevent calling with non-existent docValues
      fields

### Deprecated:

- [Revert deprecation of getAvailbleInterval with PhysicalDatasourceConstraint](https://github.com/yahoo/fili/pull/621)
    * The method is needed in order for availability to function correctly, there is a deeper dive and planning required to actually deprecate it in favor of simpler less confusing design.

- [Remove `PhysicalTable::getTableName` to use `getName` instead](https://github.com/yahoo/fili/pull/263)
    * Having more than 1 method for the same concept (ie. what's the name of this physical table) was confusing and not
      very useful.

- [Remove `PhysicalTableDictionary` dependency from `SegmentIntervalHashIdGenerator`](https://github.com/yahoo/fili/pull/263)
    * Constructors taking the dictionary have been deprecated, since it is not used any more

- [Add `DataSourceName` concept, removing responsibility from `TableName`](https://github.com/yahoo/fili/pull/263)
    * Impacts:
        - `DataSourceMetadataService` & `DataSourceMetadataLoader`
        - `ConcretePhysicalTable`

- [Deprecate old static `TableName` comparator](https://github.com/yahoo/fili/pull/263)
    * Changed to `AS_NAME_COMPARATOR` since it's more descriptive

- [Constrained Table Support for Table Serialization](https://github.com/yahoo/fili/pull/262)
    * Deprecated static empty instance of `SimplifiedIntervalList.NO_INTERVALS`
      - It looks like an immutable singleton, but it's mutable and therefore unsafe. Just make new instances of
        `SimplifiedIntervalList` instead.
    * `PartialDataRequestHandler` constructor using `PhysicalTableDictionary`

- [Moved UnionDataSource to support only single tables](https://github.com/yahoo/fili/pull/262)
    * `DataSource::getDataSources` no longer makes sense, since `UnionDataSource` only supports 1 table now

- [Support for Lucene 5 indexes](https://github.com/yahoo/fili/pull/265)
    * Added `lucene-backward-codecs.jar` as a dependency to restore support for indexes built on earlier instances.
    * Support for indexes will only remain while the current Lucene generation supports them.  All Fili users should
      rebuild indexes on Lucene 6 to avoid later pain.

- [Refactor Physical Table Definition and Update Table Loader](https://github.com/yahoo/fili/pull/207)
    * Deprecated `BaseTableLoader::loadPhysicalTable`. Use `loadPhysicalTablesWithDependency` instead.

- [`CompositePhysicalTable` Core Components Refactor](https://github.com/yahoo/fili/pull/179)
    * Deprecated `BasePhysicalTable::setAvailability` to discourage using it for testing

- [`RequestLog::stopMostRecentTimer` has been deprecated](https://github.com/yahoo/fili/pull/143)
    - This method is a part of the infrastructure to support the recently deprecated `RequestLog::switchTiming`.

- [`LogicalMetricColumn` doesn't need a 2-arg constructor](https://github.com/yahoo/fili/pull/204)
    * It's only used in one place, and there's no real need for it because the other constructor does the same thing

- [`DimensionColumn`'s 2-arg constructor is only used by a deprecated class](https://github.com/yahoo/fili/pull/204)
    * When that deprecated class (`LogicalDimensionColumn`) goes away, this constructor will go away as well

### Fixed:

- [Fix druid partial data and partition table incompatibility](https://github.com/yahoo/fili/pull/615)
    * Datasource names returned by partition table now contains only datasources that are actually used in the query
    * Fix the problem where uncovered intervals is given by druid for partition table that fili filtered out

- [Fix the generic example for loading multiple tables](https://github.com/yahoo/fili/pull/309)
    * Loading multiple tables caused it to hang and eventually time out.
    * Also fixed issue causing all tables to show the same set of dimensions.

- [Support for Lucene 5 indexes restored](https://github.com/yahoo/fili/pull/265)
    * Added `lucene-backward-codecs.jar` as a dependency to restore support for indexes built on earlier instances.

- [Specify the character encoding to support unicode characters](https://github.com/yahoo/fili/pull/221)
    * Default character set used by the back end was mangling Unicode characters.

- [Correct empty-string behavior for druid header supplier class config](https://github.com/yahoo/fili/pull/214)
    * Empty string would have tried to build a custom supplier. Now it doesn't.

- [Default the `AsyncDruidWebServiceImpl` to follow redirects](https://github.com/yahoo/fili/pull/271)
    * It defaulted to not following redirects, and now it doesn't, and will follow redirects appropriately

- Reenable custom query types in `TestDruidWebService`

- [Fixed `SegmentMetadataLoader` Unconfigured Dimension Bug](https://github.com/yahoo/fili/pull/197)
    * Immutable availability was failing when attempting to bind segment dimension columns not configured in the
      dimension dictionary.
    * Fix to filter irrelevant column names.

- [Major refactor for availability and schemas and tables](https://github.com/yahoo/fili/pull/165)
    * Ordering of fields on serialization could be inconsistent if intermediate stages used `HashSet` or `HashMap`.
    * Several constructors switched to accept `Iterable` and return `LinkedHashSet` to emphasize importance of
      ordering/prevent `HashSet` intermediates which disrupt ordering.

- [Fix Lookup Dimension Serialization](https://github.com/yahoo/fili/pull/187)
    * Fix a bug where lookup dimension is serialized as dimension spec in both outer and inner query

- Correct error message logged when no table schema match is found

- Setting `readTimeout` on `DefaultAsyncHttpClientConfig` when building `AsyncDruidWebServiceImpl`

### Removed:

- [Refactor Physical Table Definition and Update Table Loader](https://github.com/yahoo/fili/pull/207)
    * Removed deprecated `PhysicalTableDefinition` constructor that takes a `ZonlessTimeGrain`. Use `ZonedTimeGrain`
      instead
    * Removed `BaseTableLoader::buildPhysicalTable`. Table building logic has been moved to `PhysicalTableDefinition`

- [Move UnionDataSource to support only single tables](https://github.com/yahoo/fili/pull/262)
    * `DataSource` no longer accepts `Set<Table>` in a constructor

- [CompositePhsyicalTable Core Components Refactor](https://github.com/yahoo/fili/pull/179)
    * Removed deprecated method `PartialDataHandler::findMissingRequestTimeGrainIntervals`
    * Removed `permissive_column_availability_enabled` feature flag support and corresponding functionality in
      `PartialDataHandler`. Permissive availability is instead handled via table configuration, and continued usage of
      the configuration field generates a warning when Fili starts.
    * Removed `getIntersectSubintervalsForColumns` and `getUnionSubintervalsForColumns` from `PartialDataHandler`.
      `Availability` now handles these responsibilities.
    * Removed `getIntervalsByColumnName`, `resetColumns` and `hasLogicalMapping` methods in `PhysicalTable`. These
      methods were either part of the availability infrastructure, which changed completely, or the responsibilities
      have moved to `PhysicalTableSchema` (in the case of `hasLogicalMapping`).
    * Removed `PartialDataHandler::getAvailability`. `Availability` (on the PhysicalTables) has taken it's place.
    * Removed `SegmentMetadataLoader` because the endpoint this relied on had been deprecated in Druid. Use the
      `DataSourceMetadataLoader` instead.
      - Removed `SegmentMetadataLoaderHealthCheck` as well.

- [Major refactor for availability and schemas and tables](https://github.com/yahoo/fili/pull/165)
    *  Removed `ZonedSchema` (all methods moved to child class `ResultSetSchema`)
    * `PhysicalTable` no longer supports mutable availability
	    - Removed `addColumn`, `removeColumn`, `getWorkingIntervals`, and `commit`
		- Other mutators no longer exist, availability is immutable
		- Removed `getAvailableIntervals`. `Availability::getAvailableIntervals` replaces it.
    * Removed `DruidResponseParser::buildSchema`. That logic has moved to the `ResultSetSchema` constructor.
    * Removed redundant `buildLogicalTable` methods from `BaseTableLoader`


v0.7.37 - 2017/04/04
--------------------

This patch is to back-port a fix for getting Druid to handle international / UTF character sets correctly. It is
included in the v0.8.x stable releases.

### Fixed:

- [Specify the character encoding to support unicode characters](https://github.com/yahoo/fili/pull/221)
    * Default character set used by the back end was mangling Unicode characters.


v0.7.36 - 2017/01/30
--------------------

This release is a mix of fixes, upgrades, and interface clean-up. The general themes for the changes are around metric
configuration, logging and timing, and adding support for tagging dimension fields. Here are some of the highlights, but
take a look in the lower sections for more details.

Fixes:
- Deadlock in `LuceneSearchProvider`
- CORS support when using the `RoleBasedAuthFilter`

New Capabilities & Enhancements:
- Dimension field tagging
- Controls around max size of Druid response to cache
- Logging and timing enhancements

Deprecations / Removals:
- `RequestLog::switchTiming` is deprecated due to it's difficulty to use correctly
- Metric configuration has a number of deprecations as part of the effort to make configuration easier and less complex

Changes:
- There was a major overhaul of Fili's dependencies to upgrade their versions


### Added:

- [Dimension Field Tagging and Dynamic Dimension Field Serilization](https://github.com/yahoo/fili/pull/137)
    * Added a new module `fili-navi` for components added to support for Navi
    * Added `TaggedDimensionField` and related components in `fili-navi`

- [Ability to prevent caching of Druid responses larger than the maximum size supported by the cache](https://github.com/yahoo/fili/pull/93)
    * Supported for both Cache v1 and V2
    * Controlled with `bard__druid_max_response_length_to_cache` setting
    * Default value is `MAX_LONG`, so no cache prevention will happen by default

- [Log a warning if `SegmentMetadataLoader` tries to load empty segment metadata](https://github.com/yahoo/fili/pull/113)
    * While not an error condition (eg. configuration migration), it's unusual, and likely shouldn't stay this way long

- [More descriptive log message when no physical table found due to schema mismatch](https://github.com/yahoo/fili/pull/113)
    * Previous log message was user-facing only, and not as helpful as it could have been

- [Logs more finegrained timings of the request processing workflow](https://github.com/yahoo/fili/pull/110)

- [Added RegisteredLookupDimension and RegisteredLookupExtractionFunction](https://github.com/yahoo/fili/pull/132)
    * This enables supporting Druid's most recent evolution of the Query Time Lookup feature

- [`FilteredAggregationMaker`](https://github.com/yahoo/fili/pull/107)
    * This version is rudimentary. See [issue 120](https://github.com/yahoo/fili/issue/120) for future plans.

- [Support for Druid's `LinearShardSpec` metadata type](https://github.com/yahoo/fili/pull/107)

- [Added MetricField accessor to the interface of LogicalMetric](https://github.com/yahoo/fili/pull/124)
    * Previously accessing the metric field involved using three method calls

- [Ability for `ClassScanner` to instantiate arrays](https://github.com/yahoo/fili/pull/107)
    * This allows for more robust testing of classes that make use of arrays in their constructor parameters

- [Module load test code](https://github.com/yahoo/fili/pull/122)
    * Code to automatically test module is correctly configured.

- [Comprehensive tests for `MetricMaker` field coercion methods](https://github.com/yahoo/fili/pull/128)

### Changed:

- [The druid query posting timer has been removed](https://github.com/yahoo/fili/pull/141)
    * There wasn't really a good way of stopping timing only the posting itself. Since the timer is
       probably not that useful, it has been removed.

- [Dimension Field Tagging and Dynamic Dimension Field Serilization](https://github.com/yahoo/fili/pull/137)
    * Changed `fili-core` dimension endpoint `DimensionField` serialization strategy from hard coded static attributes
      to dynamic serialization based on `jackson` serializer

- [MetricMaker cleanup and simplification](https://github.com/yahoo/fili/pull/127)
    * Simplified raw aggregation makers
    * `ConstantMaker` now throws an `IllegalArgumentException` wrapping the raw NumberFormatException on a bad argument
    * `FilteredAggregation` no longer requires a metric name to be passed in. (Aggregation field name is used)
    * `FilteredAggregationMaker` now accepts a metric to the 'make' method instead of binding at construction time.
    * `ArithmeticAggregationMaker` default now uses `NoOpResultSetMapper` instead of rounding mapper. (breaking change)
    * `FilteredAggregationMaker`, `SketchSetOperationMaker` members are now private

- [Used Metric Field accessor to simplify maker code](https://github.com/yahoo/fili/pull/124)
    * Using metric field accessor simplifies and enables streaminess in maker code

- [Fili's name for a PhysicalTable is decoupled from the name of the associated table in Druid](https://github.com/yahoo/fili/pull/123)

- [No match found due to schema mismatch now a `500 Internal Server Error` response instead of a `400 Bad Request` response](https://github.com/yahoo/fili/pull/113)
    * This should never be a user fault, since that check is much earlier

- [Make `SegmentMetadata::equals` `null`-safe](https://github.com/yahoo/fili/pull/113)
    * It was not properly checking for `null` before and could have exploded

- [Default DimensionColumn name to use apiName instead of physicalName](https://github.com/yahoo/fili/pull/115)
    * Change `DimensionColumn.java` to use dimension api name instead of physical name as its name
    * Modified files dependent on `DimensionColumn.java` and corresponding tests according to the above change

- [`NoOpResultSetMapper` now runs in constant time and space.](https://github.com/yahoo/fili/pull/119)

- [Remove restriction for single physical dimension to multiple lookup dimensions](https://github.com/yahoo/fili/pull/112)
    * Change physical dimension name to logical dimension name mapping into `Map<String, Set<String>>` instead of
      `Map<String, String>` in `PhysicalTable.java`

- [SegmentMetadataLoader include provided request headers](https://github.com/yahoo/fili/pull/106)
    * `SegmentMetadataLoader` sends requests with the provided request headers in `AsyncDruidWebservice` now
    * Refactored `AsyncDruidWebserviceSpec` test and added test for checking `getJsonData` includes request headers too

- [Include physical table name in warning log message for logicalToPhysical mapping](https://github.com/yahoo/fili/pull/94)
    * Without this name, it's hard to know what table seems to be misconfigured.

- [`ResponseValidationException` uses `Response.StatusType` rather than `Response.Status`](https://github.com/yahoo/fili/pull/96)
    * `Response.StatusType` is the interface that `Response.Status` implements.
    * This will have no impact on current code in Fili that uses `ResponseValidationException`, and it allows customers
      to inject http codes not included in `Response.Status`.

- [Removed "provided" modifier for SLF4J and Logback dependencies in the Wikipedia example](https://github.com/yahoo/fili/pull/102)

- [Updated dependencies](https://github.com/yahoo/fili/pull/103)

    Unless otherwise noted, all dependency upgrades are for general stability and performance improvement. The called-
    out changes are only those that are likely of interest to Fili. Any dependency upgrade for which a changelog could
    not be found has not been linked to one, otherwise all other upgrades include a link to the relevant changelog.

    WARNING: There is a known dependency conflict between apache commons configuration 1.6 and 1.10. If after upgrading
    to the latest Fili, your tests begin to fail with `NoClassDefFoundExceptions`, it is likely that you are explicitly
    depending on the apache commons configuration 1.6. Removing that dependency or upgrading it to 1.10 should fix the
    issue.

    * [Gmaven plugin 1.4 -> 1.5](https://github.com/groovy/gmaven/compare/gmaven-1.4...gmaven-1.5)
    * [Guava 16.0.1 -> 20.0](https://github.com/google/guava/wiki/ReleaseHistory)
    * [Jedis 2.7.2 -> 2.9.0](https://github.com/xetorthio/jedis/releases):
        - Geo command support and binary mode support
        - ZADD support
        - Ipv6 and SSL support
        - Other assorted feature and Redis support upgrades
    * [Redisson 2.2.13 -> 3.1.0](https://github.com/redisson/redisson/blob/master/CHANGELOG.md):
        - Support for binary stream in and out of Reddison
        - Lots of features for distributed data structure capabilities
        - Can make fire-and-forget style calls in ack-response-only modes
        - Many fixes and improvements for PubSub features
        - Support for command timeouts
        - Fixed bug where connections did not always close when RedisClient shut down
        - Breaking API changes:
            - Moved config classes to own package
            - Moved core classes to api package
            - Moved to Redisson's RFuture instead of netty's Future
    * [JodaTime 2.8.2 -> 2.9.6](http://www.joda.org/joda-time/installation.html):
        - Faster TZ parsing
        - Added `Interval.parseWithOffset`
        - GMT fix for JDK 8u60
        - Fixed Interval overflow bug
        - TZ data update from 2015g to 2016i
    * [AsyncHttpClient 2.0.2 -> 2.0.24](https://github.com/AsyncHttpClient/async-http-client/milestones?state=closed):
        - Custom header separator fix
        - No longer double-wrapping CompletableFuture exceptions
    * [Apache HttpClient 4.5 -> 4.5.2](https://archive.apache.org/dist/httpcomponents/httpclient/RELEASE_NOTES-4.5.x.txt):
        - Supports handling a redirect response to a POST request
        - Fixed deflate zlib header issue
    * [RxJava 1.1.5 -> 1.2.2](https://github.com/ReactiveX/RxJava/releases):
        - Deprecate TestObserver in favor of TestSubscriber
    * Spymemcached 2.12.0 -> 2.12.1
    * [org.json 20141113 -> 20160810](https://github.com/stleary/JSON-java)
    * [Maven release plugin 2.5 -> 2.5.3](https://issues.apache.org/jira/browse/MRELEASE/?selectedTab=com.atlassian.jira.jira-projects-plugin:changelog-panel):
        - Fixes `release:prepare` not committing pom.xml if not in the git root
        - Fixes version update not updating inter-module dependencies
        - Fixes version update failing when project is not a SNAPSHOT
    * [Maven antrun plugin 1.7 -> 1.8](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12317121&version=12330276)
    * [Maven compiler plugin 3.3 -> 3.6.0](https://issues.apache.org/jira/browse/MCOMPILER/?selectedTab=com.atlassian.jira.jira-projects-plugin:changelog-panel):
        - Fix for compiler fail in Eclipse
    * [Maven surefire plugin 2.17 -> 2.19.1](https://issues.apache.org/jira/browse/SUREFIRE/?selectedTab=com.atlassian.jira.jira-projects-plugin:changelog-panel):
        - Correct indentation for Groovy's power asserts
    * [Maven javadoc plugin 2.10.3 -> 2.10.4](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12317529&version=12331967)
    * [Maven site plugin 3.5 -> 3.6](https://issues.apache.org/jira/browse/MSite/?selectedTab=com.atlassian.jira.jira-projects-plugin:changelog-panel)
    * [SLF4J 1.7.12 -> 1.7.21](http://www.slf4j.org/news.html):
        - Fixed to MDC adapter, leaking information to non-child threads
        - Better handling of ill-formatted strings
        - Cleaned up multi-thread consistency for LoggerFactory-based logger initializations
        - Closed a multi-threaded gap where early logs may be lost if they happened while SLF4J was initializing in a
          multi-threaded application
    * [Logback 1.1.3 -> ](http://logback.qos.ch/news.html):
        - Child threads no longer inherit MDC values
        - AsyncAppender can be configured to never block
        - Fixed issue with variable substitution when the value ends in a colon
    * [Apache Commons Lang 3.4 -> 3.5](https://commons.apache.org/proper/commons-lang/release-notes/RELEASE-NOTES-3.5.txt)
    * [Apache Commons Configuration 1.6 -> 1.10](https://commons.apache.org/proper/commons-configuration/changes-report.html#a1.7):
        - Tightened getList's behavior if the list values are non-strings
        - MapConfiguration can be set to not trim values by default
        - CompositeConfiguration can now handle non-BaseConfiguration core configurations
        - `addConfiguration()` overload added to allow correcting inconsistent configuration compositing
    * [Apache Avro 1.8.0 -> 1.8.1](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12310911&version=12333322)
    * [Spring Core 4.0.5 -> 4.3.4](https://docs.spring.io/spring/docs/4.3.x/spring-framework-reference/html/)
    * [CGLib 3.2.0 -> 3.2.4](https://github.com/cglib/cglib/releases):
        - Optimizations and regression fixes
    * [Objenesis 2.2 -> 2.4](http://objenesis.org/notes.html)
    * Jersey 2.22 -> 2.24:
        - https://jersey.java.net/release-notes/2.24.html
        - https://jersey.java.net/release-notes/2.23.html
        - `@BeanParam` linking support fix
        - Declarative linking with Maps fixed
        - Async write ordering deadlock fix
    * HK2 2.4.0-b31 -> 2.5.0-b05:
        - Necessitated by Jersey upgrade
    * [JavaX Annotation API 1.2 -> 1.3](https://jcp.org/en/jsr/detail?id=250)

### Deprecated:

- [Deprecated DefaultingDictionary usage in DefaultingVolatileIntervalsService](https://github.com/yahoo/fili/pull/259)

- [`RequestLog::switchTiming` has been deprecated](https://github.com/yahoo/fili/pull/141)
    - `RequestLog::switchTiming` is very context-dependent, and therefore brittle. In particular, adding any additional
      timers inside code called by a timed block may result in the original timer not stopping properly. All usages of
      `switchTiming` should be replaced with explicit calls to `RequestLog::startTiming` and `RequestLog::stopTiming`.

- [Dimension Field Tagging and Dynamic Dimension Field Serilization](https://github.com/yahoo/fili/pull/137)
    * Deprecated `DimensionsServlet::getDimensionFieldListSummaryView` and `DimensionsServlet::getDimensionFieldSummaryView`
      since there is no need for it anymore due to the change in serialization of `DimensionField`

- [Default DimensionColumn name to use apiName instead of physicalName](https://github.com/yahoo/fili/pull/115)
    * Deprecated `TableUtils::getColumnNames(DataApiRequest, DruidAggregationQuery, PhysicalTable)` returning dimension
      physical name, in favor of `TableUtils::getColumnNames(DataApiRequest, DruidAggregationQuery)` returning dimension
      api name
    * Deprecated `DimensionColumn::DimensionColumn addNewDimensionColumn(Schema, Dimension, PhysicalTable)` in favor of
      `DimensionColumn::DimensionColumn addNewDimensionColumn(Schema, Dimension)` which uses api name instead of
      physical name as column identifier for columns
    * Deprecated `LogicalDimensionColumn` in favor of `DimensionColumn` since `DimensionColumn` stores api name instead
      of physical name now, so `LogicalDimensionColumn` is no longer needed

- [Moved to static implementations for numeric and sketch coercion helper methods](https://github.com/yahoo/fili/pull/128)
    * `MetricMaker.getSketchField(String fieldName)` rather use `MetricMaker.getSketchField(MetricField field)`
    * `MetricMaker.getNumericField(String fieldName)` rather use `MetricMaker.getNumericField(MetricField field)`

- [MetricMaker cleanup and simplification](https://github.com/yahoo/fili/pull/127)
    * `AggregationAverageMaker` deprecated conversion method required by deprecated sketch library

- [Metric configuration deprecations](https://github.com/yahoo/fili/pull/124)
    * Deprecated superfluous constructor of `FilteredAggregator` with superfluous argument
    * Deprecated MetricMaker utility method in favor of using new field accessor on Metric

- [Deprecated MetricMaker.getDependentQuery lookup method in favor of simpler direct access](https://github.com/yahoo/fili/pull/124)

### Fixed:

- [Fixes a potential deadlock](https://github.com/yahoo/fili/pull/116)
    * There is a chance the `LuceneSearchProvider` will deadlock if one thread is attempting to read a dimension for the
      first time while another is  attempting to load it:
        - Thread A is pushing in new dimension data. It invokes `refreshIndex`, and acquires the write lock.
        - Thread B is reading dimension data. It invokes `getResultsPage`, and then `initializeIndexSearcher`, then
          `reopenIndexSearcher`. It hits the write lock (acquired by Thread A) and blocks.
        - At the end of its computation of `refreshIndex`, Thread A attempts to invoke `reopenIndexSearcher`. However,
          `reopenIndexSearcher` is `synchronized`, and Thread B is already invoking it.
        - To fix the resulting deadlock, `reopenIndexSearcher` is no longer synchronized. Since threads need to acquire
          a write lock before doing anything else anyway, the method is still effectively  synchronized.

- [Fix and refactor role based filter to allow CORS](https://github.com/yahoo/fili/pull/99)
    * Fix `RoleBasedAuthFilter` to bypass `OPTIONS` request for CORS
    * Discovered a bug where `user_roles` is declared but unset still reads as a list with empty string (included a temporary fix by commenting the variable declaration)
    * Refactored `RoleBasedAuthFilter` and `RoleBasedAuthFilterSpec` for better testing

- [Added missing coverage for `ThetaSketchEstimate` unwrapping in `MetricMaker.getSketchField`](https://github.com/yahoo/fili/pull/128)

- [`DataSource::getNames` now returns Fili identifiers, not fact store identifiers](https://github.com/yahoo/fili/pull/125/files)

- [Made a few injection points not useless](https://github.com/yahoo/fili/pull/98)
    * Template types don't get the same subclass goodness that method invocation and dependencies get, so this method
      did not allow returning a subclass of `DruidQueryBuilder` or of `DruidResponseParser`.

- [Made now required constructor for ArithmeticMaker with rounding public](https://github.com/yahoo/fili/pull/148)

### Removed:

- [Removed invalid constructor from SketchRoundUpMappepr](https://github.com/yahoo/fili/pull/148)


v0.6.29 - 2016/11/16
--------------------

This release is focused on general stability, with a number of bugs fixed, and also adds a few small new capabilities
and enhancements. Here are some of the highlights, but take a look in the lower sections for more details.

Fixes:
- Dimension keys are now properly case-sensitive (
  - ***Because this is a breaking change, the fix has been wrapped in a feature flag. For now, this defaults to the
    existing broken behavior, but this will change in a future version, and eventually the fix will be permanent.***
- `all`-grain queries are no longer split
- Closed a race condition in the `LuceneSearchProvider` where readers would get an error if an update was in progress
- Correctly interpreting List-type configs from the Environment tier as a true `List`
- Stopped recording synchronous requests in the `ApiJobStore`, which is only intended to hold async requests

New Capabilities & Enhancements:
- Customizable logging format
- X-Request-Id header support, letting clients set a request ID that will be included in the Druid query
- Support for Druid's `In` filter
- Native support for building `DimensionRow`s from AVRO files
- Ability to set headers on Druid requests, letting Fili talk to a secure Druid
- Better error messaging when things go wrong
- Better ability to use custom Druid query types

### Added:

- [Added Dimension Value implementation for PartitionTableDefinition]
    * Added `DimensionIdFilter` implementation of  `DataSourceFilter`
    * Created `DimensionListPartitionTableDefinition`

- [Added 'hasAnyRows' to SearchProvider interface](https://github.com/yahoo/fili/pull/259)
    * Has Any Rows allows implementations to optimize queries which only need to identify existence of matches

- [Customize Logging Format in RequestLog](https://github.com/yahoo/fili/pull/81)

- [Can Populate Dimension Rows from an AVRO file](https://github.com/yahoo/fili/pull/86)
    * Added `AvroDimensionRowParser` that parses an AVRO data file into `DimensionRow`s after validating the AVRO schema.
    * Added a functional Interface `DimensionFieldMapper` that maps field name.

- [Support for Druid's In Filter](https://github.com/yahoo/fili/pull/64)
    * The in-filter only works with Druid versions 0.9.0 and up.

- [Support for X-Request-ID](https://github.com/yahoo/fili/pull/68)

- [Documentation for topN](https://github.com/yahoo/fili/pull/43)

- [Adding slice availability to slices endpoint](https://github.com/yahoo/fili/pull/51)
    * Slice availability can be used to debug availability issues on Physical tables

- [Ability to set headers for requests to Druid](https://github.com/yahoo/fili/pull/62)
      * The `AsyncDruidWebServiceImpl` now accepts a `Supplier<Map<String, String>>` argument which specifies the headers
        to add to the Druid data requests. This feature is made configurable through `SystemConfig` in the
        `AbstractBinderFactory`.

### Changed:

- [Error messages generated during response processing include the request id.](https://github.com/yahoo/fili/pull/78)

- [`DimensionStoreKeyUtils` now supports case sensitive row and column keys](https://github.com/yahoo/fili/pull/90)
    * Wrapped this config in a feature flag `case_sensitive_keys_enabled` which is set to `false` by default for
      backwards compatibility. This flag will be set to `true` in future versions.

- [The `getGrainMap` method in `StandardGranularityParser` class is renamed to `getDefaultGrainMap` and is made public static.](https://github.com/yahoo/fili/pull/88)
    * Created new class `GranularityDictionary` and bind getGranularityDictionary to it

- [`PhysicalTable` now uses `getAvailableIntervals` internally rather than directly referencing its intervals](https://github.com/yahoo/fili/pull/79)

- [CSV attachment name for multi-interval request now contain '__' instead of ','](https://github.com/yahoo/fili/pull/76)
    * This change is made to allow running multi-api request with csv format using chrome browser.

- [Improves error messages when querying Druid goes wrong](https://github.com/yahoo/fili/pull/61)
    * The `ResponseException` now includes a message that prints the `ResponseException`'s internal state
        (i.e. the druid query and response code) using the error messages
        `ErrorMessageFormat::FAILED_TO_SEND_QUERY_TO_DRUID` and `ErrorMessageFormat::ERROR_FROM_DRUID`
    * The druid query and status code, reason and response body are now logged at the error level in the
      failure and error callbacks in `AsyncDruidWebServiceImpl`

- [Fili now supports custom Druid query types](https://github.com/yahoo/fili/pull/57)
    * `QueryType` has been turned into an interface, backed by an enum `DefaultQueryType`.
        - The default implementations of `DruidResponseParser` `DruidQueryBuilder`, `WeightEvaluationQuery` and
          `TestDruidWebService` only support `DefaultQueryType`.
    * `DruidResponseParser` is now injectable by overriding `AbstractBinderFactory::buildDruidResponseParser` method.
    * `DruidQueryBuilder` is now injectable by overriding `AbstractBinderFactory::buildDruidQueryBuilder` method.

- [Updated commons-collections4 dependency from 4.0 to 4.1 to address a security vulnerability in the library.](https://github.com/yahoo/fili/pull/52)
    * For details see: https://commons.apache.org/proper/commons-collections/security-reports.html#Apache_Commons_Collections_Security_Vulnerabilities
    * It should be noted that Fili does not make use of any the serialization/deserialization capabilities of any classes
      in the functor package, so the security vulnerability does not affect Fili.

- Clean up build plugins
    * Move some plugin configs up to `pluginManagement`
    * Make `fili-core` publish test javadocs
    * Default source plugin to target `jar-no-fork` instead of `jar`
    * Default javadoc plugin to target `javadoc-no-fork` as well as `jar`
    * Move some versions up to `pluginManagement`
    * Remove overly (and un-usedly) specified options in surfire plugin configs
    * Make all projects pull in the `source` plugin

- Corrected bug with Fili sub-module dependency specification
    * Dependency versions are now set via a fixed property at deploy time, rather than relying on `project.version`

- Cleaned up dependencies in pom files
    * Moved version management of dependencies up to the parent Pom's dependency management section
    * Cleaned up the parent Pom's dependency section to only be those dependencies that truly _every_ sub-project should
      depend on.
    * Cleaned up sub-project Pom dependency sections to handle and better use the dependencies the parent Pom provides

### Deprecated:

- [`DimensionStoreKeyUtils` now supports case sensitive row and column keys](https://github.com/yahoo/fili/pull/90)
    - Case insensitive row and column keys will be deprecated going forward.
    - ***Because this is a breaking change, the fix has been wrapped in a feature flag. For now, this defaults to the
        existing broken behavior, but this will change in a future version, and eventually the fix will be permanent.***
        - The feature flag for this is `bard__case_sensitive_keys_enabled`

- [All constructors of `ResponseException` that do not take an `ObjectWriter`](https://github.com/yahoo/fili/pull/70)
    * An `ObjectWriter` is required in order to ensure that the exception correctly serializes its associated Druid query

### Fixed:

- [Environment comma separated list variables are now correctly pulled in as a list](https://github.com/yahoo/fili/pull/82)
    * Before it was pulled in as a single sting containing commas, now environment variables are pulled in the same way
      as the properties files
    * Added test to test comma separated list environment variables when `FILI_TEST_LIST` environment variable exists

- [Druid queries are now serialized correctly when logging `ResponseExceptions`](https://github.com/yahoo/fili/pull/70)

- [Disable Query split for "all" grain ](https:://github.com/yahoo/fili/pull/75)
    * Before, if we requested "all" grain with multiple intervals, the `SplitQueryRequestHandler` would incorrectly
      split the query and we would get multiple buckets in the output. Now, the query split is disabled for "all" grain
      and we correctly get only one bucket in the response.

- [Fixed typo emit -> omit in Utils.java omitField()](https://github.com/yahoo/fili/pull/68)

- [Adds read locking to all attempts to read the Lucene index](https://github.com/yahoo/fili/pull/52)
    * Before, if Fili attempted to read from the Lucene indices (i.e. processing a query with filters) while loading
      dimension indices, the request would fail and we would get a `LuceneIndexReaderAlreadyClosedException`. Now, the
      read locks should ensure that the query processing will wait until indexing completes (and vice versa).

- [Fixes a bug where job metadata was being stored in the `ApiJobStore` even when the results came back synchronously](https://github.com/yahoo/fili/pull/49)
    * The workflow that updates the job's metadata with `success` was running even when the query was synchronous. That
      update also caused the ticket to be stored in the `ApiJobStore`.
    * The delay operator didn't stop the "update" workflow from executing because it viewed an `Observable::onCompleted`
      call as a message for the purpose of the delay. Since the two observables that that the metadata update gated on
      are empty when the query is synchronous, the "update metadata" workflow was being triggered every time.
    * The delay operator was replaced by `zipWith` as a gating mechanism.

- [#45, removing sorting from weight check queries](https://github.com/yahoo/fili/pull/46)

- [`JsonSlurper` can now handle sorting lists with mixed-type entries](https://github.com/yahoo/fili/pull/58)
    * even if the list starts with a string, number, or boolean

- [Broken segment metadata with Druid v0.9.1](https://github.com/yahoo/fili/issues/63)
    * Made `NumberedShardSpec` ignore unexpected properties during deserialization
    * Added tests to `DataSourceMetadataLoaderSpec` to test the v.0.9.1 optional field `shardSpec.partitionDimensions`
      on segment info JSON.


v0.1.x - 2016/09/23
-------------------

This release focuses on stabilization, especially of the Query Time Lookup (QTL) capabilities, and the Async API and
Jobs resource. Here are the highlights of what's in this release:

- A bugfix for the `DruidDimensionLoader`
- A new default `DimensionLoader`
- A bunch more tests and test upgrades
- Filtering and pagination on the Jobs resource
- A `userId` field for default Job resource representations
- Package cleanup for the jobs-related classes

### Added:

- [`always` keyword for the `asyncAfter` parameter now guarantees that a query will be asynchronous](https://github.com/yahoo/fili/pull/39)

- [A test implementation of the `AsynchronousWorkflowsBuilder`: `TestAsynchronousWorkflowsBuilder`](http://github.com/yahoo/fili/pull/39)
  * Identical to the `DefaultAsynchronousWorkflowsBuilder`, except that it includes hooks to allow outside forces (i.e.
    Specifications) to add additional subscribers to each workflow.

- [Functional tests for Asynchronous queries](https://github.com/yahoo/fili/pull/35)

- [Enrich jobs endpoint with filtering functionality] (https://github.com/yahoo/fili/pull/26)
  * Jobs endpoint now supports filters

- [Enrich the ApiJobStore interface] (https://github.com/yahoo/fili/pull/23)
  * `ApiJobStore` interface now supports filtering `JobRows` in the store
  * Added support for filtering JobRows in `HashJobStore`
  * Added `JobRowFilter` to hold filter information

- [QueryTimeLookup Functionality Testing](https://github.com/yahoo/fili/pull/34)
  * Added two tests `LookupDimensionFilteringDataServletSpec` and `LookupDimensionGroupingDataServletSpec` to test QTL
    functionality

- [Lookup Dimension Serializer](https://github.com/yahoo/fili/pull/31)
  * Created `LookupDimensionToDimensionSpec` serializer for `LookupDimension`
  * Created corresponding tests for `LookupDimensionToDimensionSpec` in `LookupDimensionToDimensionSpecSpec`

### Deprecated:

- [Allow configurable headers for Druid data requests](https:://github.com/yahoo/fili/pull/62)
  * Deprecated `AsyncDruidWebServiceImpl(DruidServiceConfig, ObjectMapper)` and
    `AsyncDruidWebServiceImpl(DruidServiceConfig, AsyncHttpClient, ObjectMapper)` because we added new construstructors
    that take a `Supplier` argument for Druid data request headers.

- [QueryTimeLookup Functionality Testing](https://github.com/yahoo/fili/pull/34)
  * Deprecated `KeyValueDimensionLoader`, in favor of `TypeAwareDimensionLoader`

### Changed:

- Removed `physicalName` lookup for metrics in `TableUtils::getColumnNames` to remove spurious warnings
     * Metrics are not mapped like dimensions are. Dimensions are aliased per physical table and metrics are aliazed per logical table.
     * Logical metric is mapped with one or many physical metrics. Same look up logic for dimension and metrics doesn't make sense.

#### Jobs:

- [HashPreResponseStore moved to `test` root directory.](https://github.com/yahoo/fili/pull/39)
  * The `HashPreResponseStore` is really intended only for testing, and does not have capabilities (i.e. TTL) that are
    needed for production.

- [The `TestBinderFactory` now uses the `TestAsynchronousWorkflowsBuilder`](http://github.com/yahoo/fili/pull/39)
  * This allows the asynchronous functional tests to add countdown latches to the workflows where necessary, allowing
    for thread-safe tests.

- [Removed `JobsApiRequest::handleBroadcastChannelNotification`](https://github.com/yahoo/fili/pull/39)
  * That logic does not really belong in the `JobsApiRequest` (which is responsible for modeling a response, not
    processing it), and has been consolidated into the `JobsServlet`.

- [ISSUE-17](https://github.com/yahoo/fili/issues/17) [Added pagination parameters to `PreResponse`](https://github.com/yahoo/fili/pull/19)
  * Updated `JobsServlet::handlePreResponseWithError` to update `ResultSet` object with pagination parameters

- [Enrich jobs endpoint with filtering functionality](https://github.com/yahoo/fili/pull/26)
  * The default job payload generated by `DefaultJobPayloadBuilder` now has a `userId`

- [Removed timing component in JobsApiRequestSpec](https://github.com/yahoo/fili/pull/27)
  * Rather than setting an async timeout, and then sleeping, `JobsApiRequestSpec::handleBroadcastChannelNotification`
    returns an empty Observable if a timeout occurs before the notification is received now verifies that the Observable
    returned terminates without sending any messages.

- [Reorganizes asynchronous package structure](https://github.com/yahoo/fili/pull/19)
  * The `jobs` package is renamed to `async` and split into the following subpackages:
    - `broadcastchannels` - Everything dealing with broadcast channels
    - `jobs` - Everything related to `jobs`, broken into subpackages
      * `jobrows` - Everything related to the content of the job metadata
      * `payloads` - Everything related to building the version of the job metadata to send to the user
      * `stores` - Everything related to the databases for job data
    - `preresponses` - Everything related to `PreResponses`, broken into subpackages
      * `stores` - Everything related to the the databases for PreResponse data
    - `workflows` - Everything related to the asynchronous workflow

#### Query Time Lookup (QTL)

- [QueryTimeLookup Functionality Testing](https://github.com/yahoo/fili/pull/34)
  * `AbstractBinderFactory` now uses `TypeAwareDimensionLoader` instead of `KeyValueStoreDimensionLoader`

- [Fix Dimension Serialization Problem with Nested Queries](https://github.com/yahoo/fili/pull/15)
  * Modified `DimensionToDefaultDimensionSpec` serializer to serialize Dimension to apiName if it's not in the
    inner-most query
  * Added `Util::hasInnerQuery` helper in serializer package to determine if query is the inner most query or not
  * Added tests for `DimensionToDefaultDimensionSpec`

#### General:

- [Preserve collection order of dimensions, dimension fields and metrics](https://github.com/yahoo/fili/pull/25)
  * `DataApiRequest::generateDimensions` now returns a `LinkedHashSet`
  * `DataApiRequest::generateDimensionFields` now returns a `LinkedHashMap<Dimension, LinkedHashSet<DimensionField>>`
  * `DataApiRequest::withPerDimensionFields` now takes a `LinkedHashSet` as its second argument.
  * `DataApiRequest::getDimensionFields` now returns a `LinkedHashMap<Dimension, LinkedHashSet<DimensionField>>>`
  * `Response::Response` now takes a `LinkedHashSet` and `LinkedHashMap<Dimension, LinkedHashSet<DimensionField>>>` as
    its second  and third arguments.
  * `ResponseContext::dimensionToDimensionFieldMap` now takes a `LinkedHashMap<Dimension, LinkedHashSet<DimensionField>>>`
  * `ResponseContext::getDimensionToDimensionFieldMap` now returns a `LinkedHashMap<Dimension, LinkedHashSet<DimensionField>>>`

- [`TestDruidWebService::jsonResponse` is now a `Producer<String>` Producer](https://github.com/yahoo/fili/pull/35)

- [QueryTimeLookup Functionality Testing](https://github.com/yahoo/fili/pull/34)
  * Modified some testing resources (PETS table and corresponding dimensions) to allow better testing on `LookupDimension`s

- [Memoize generated values during recursive class-scan class construction](https://github.com/yahoo/fili/pull/29)

### Fixed:

- [Fixing the case when the security context is not complete](https://github.com/yahoo/fili/pull/67)
  * Check for nulls in the `DefaultJobRowBuilder.userIdExtractor` function.

- [`DruidDimensionsLoader` doesn't set the dimension's lastUpdated date](https://github.com/yahoo/fili/pull/24)
  * `DruidDimensionsLoader` now properly sets the `lastUpdated` field after it finished processing the Druid response

