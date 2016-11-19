Change Log
==========

All notable changes to Fili will be documented here. Changes are accumulated as new paragraphs at the top of the current 
major version. Each change has a link to the pull request that makes the change and to the issue that triggered the
pull request if there was one.

Current
-------

### Added:

- [Ability to not try to cache Druid responses that are larger than the maximum size supported by the cache implementation](https://github.com/yahoo/fili/pull/93)
    * Supported for both Cache v1 and V2
    * Controlled with `bard__druid_max_response_length_to_cache` setting
    * Default value is `MAX_LONG`, so no cache prevention will happen by default

### Changed:

- [Remove restriction for single physical dimension to multiple lookup dimensions](https://github.com/yahoo/fili/pull/112)
    * Modified physical dimension name to logical dimension name mapping into a `Map<String, Set<String>>` instead of `Map<String, String>` in `PhysicalTable.java`

- [SegmentMetadataLoader include provided request headers](https://github.com/yahoo/fili/pull/106)
    * `SegmentMetadataLoader` sends requests with the provided request headers in `AsyncDruidWebservice` now
    * Refactored `AsyncDruidWebserviceSpec` test and added test for checking `getJsonData` includes request headers as well

- [Include physical table name in warning log message for logicalToPhysical mapping](https://github.com/yahoo/fili/pull/94)
    * Without this name, it's hard to know what table seems to be misconfigured.

- [`ResponseValidationException` uses `Response.StatusType` rather than `Response.Status`](https://github.com/yahoo/fili/pull/96)
    * `Response.StatusType` is the interface that `Response.Status` implements.
    * This will have no impact on current code in Fili that uses `ResponseValidationException`, and it allows customers to inject http
        codes not included in `Response.Status`.
         
- [Removed "provided" modifier for SLF4J and Logback dependencies in the Wikipedia example](https://github.com/yahoo/fili/pull/102)

### Deprecated:



### Fixed:

- [Fix and refactor role based filter to allow CORS](https://github.com/yahoo/fili/pull/99)
    * Fix `RoleBasedAuthFilter` to bypass `OPTIONS` request for CORS
    * Discovered a bug where `user_roles` is declared but unset still reads as a list with empty string (included a temporary fix by commenting the variable declaration)
    * Refactored `RoleBasedAuthFilter` and `RoleBasedAuthFilterSpec` for better testing

- [Made a few injection points not useless](https://github.com/yahoo/fili/pull/98)
    * Template types don't get the same subclass goodness that method invocation and
        dependencies get, so this method did not allow returning a subclass of
       `DruidQueryBuilder` or of `DruidResponseParser`.

### Known Issues:



### Removed:



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
