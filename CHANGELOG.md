Change Log
==========

All notable changes to Fili will be documented here. Changes are accumulated as new paragraphs at the top of the current 
major version. Each change has a link to the pull request that makes the change and to the issue that triggered the
pull request if there was one.

Current
-------

### Added:

-  [QueryTimeLookup Functionality Testing](https://github.com/yahoo/fili/pull/34)
    * Added two tests `LookupDimensionFilteringDataServeletSpec` and `LookupDimensionGroupingDataServletSpec` to test QTL functionality

- [Enrich the ApiJobStore interface] (https://github.com/yahoo/fili/pull/23)
    * `ApiJobStore` Interface now supports filtering `JobRows` in the store
    * Added support for filtering JobRows in `HashJobStore`
    * Added `JobRowFilter` to hold filter information

-  [Lookup Dimension Serializer](https://github.com/yahoo/fili/pull/31)
    * Created `LookupDimensionToDimensionSpec` serializer for `LookupDimension`
    * Created corresponding tests for `LookupDimensionToDimensionSpec` in `LookupDimensionToDimensionSpecSpec`


### Deprecated:

-  [QueryTimeLookup Functionality Testing](https://github.com/yahoo/fili/pull/34)
    * Deprecated `KeyValueDimensionLoader`, in favor of `TypeAwareDimensionLoader`


### Removed:


### Changed:

- [Memoize generated values during recursive class-scan class construction](https://github.com/yahoo/fili/pull/29)

-  [QueryTimeLookup Functionality Testing](https://github.com/yahoo/fili/pull/34)
    * `AbstractBinderFactory` now uses `TypeAwareDimensionLoader` instead of `KeyValueStoreDimensionLoader`
    * Modified some testing resources (PETS table and corresponding dimensions) to allow better testing on `LookupDimension`s

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

-  [Removed timing component in JobsApiRequestSpec](https://github.com/yahoo/fili/pull/27)
    * Rather than setting an async timeout, and then sleeping, 
      `JobsApiReqeustSpec::handleBroadcastChannelNotification returns an empty Observable if a timeout occurs before the notification is received`
      now verifies that the Observable returned terminates without sending any
      messages.

- [Preserve collection order of dimensions, dimension fields and metrics](https://github.com/yahoo/fili/pull/25)
    * `DataApiRequest::generateDimensions` now returns a `LinkedHashSet`
    * `DataApiRequest::generateDimensionFields` now returns a `LinkedHashMap<Dimension, LinkedHashSet<DimensionField>>`
    * `DataApiRequest::withPerDimensionFields` now takes a `LinkedHashSet` as its second argument.
    * `DataApiRequest::getDimensionFields` now returns a `LinkedHashMap<Dimension, LinkedHashSet<DimensionField>>>`
    * `Response::Response` now takes a `LinkedHashSet` and `LinkedHashMap<Dimension, LinkedHashSet<DimensionField>>>` as
      its second  and third arguments.
    * `ResponseContext::dimensionToDimensionFieldMap` now takes a `LinkedHashMap<Dimension, LinkedHashSet<DimensionField>>>`
    * `ResponseContext::getDimensionToDimensionFieldMap` now returns a `LinkedHashMap<Dimension, LinkedHashSet<DimensionField>>>`

-  [Fix Dimension Serialization Problem with Nested Queries](https://github.com/yahoo/fili/pull/15)
    * Modified `DimensionToDefaultDimensionSpec` serializer to serialize dimension to apiName if it is not the inner most query
    * Added helper `hasInnerQuery` to `Util` in serializer package to determine if current query is the inner most query or not
    * Added tests for `DimensionToDefaultDimensionSpec`


### Fixed:

- [`DruidDimensionsLoader` doesn't set the dimension's lastUpdated date](https://github.com/yahoo/fili/pull/24)
  * `DruidDimensionsLoader` now properly sets the `lastUpdated` field after it finished processing the Druid response


### Known Issues:

