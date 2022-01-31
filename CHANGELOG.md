Change Log
==========

All notable changes to Fili will be documented here. Changes are accumulated as new paragraphs at the top of the current
major version. Each change has a link to the pull request that makes the change and to the issue that triggered the
pull request if there was one.

Current
-------

### Fixed:

### Added:
- [Fili-sql minute timestampFormat support](https://github.com/yahoo/fili/issues/1210)
  * Add minute timestampFormat

- [Add better error messaging for invalid grain on base metrics](https://github.com/yahoo/fili/issues/1207)
   * Add better error message for base metrics with invalid grains by including the valid grains on the metrics
   
- [Add metric type to meta block](https://github.com/yahoo/fili/issues/1197)
   * Added metric columns part of data query with their type details to meta block
    
- [Support recurrence rules in dateTime expression](https://github.com/yahoo/fili/issues/1195)
   * Added explicit type parsing to dateTime Elements
   * Added library to parse RRules from the dateTime elements
   * Enhanced switches for various combinations of dateTimeElements
   * Additional testing on new and old paths
   * Created a limit parameter to prevent infinite recurrence rules from generating infinite reporting intervals.
   
- [Added long name contract to DimensionField](https://github.com/yahoo/fili/issues/1200)
   * `DimensionField` contains optionally serialized longName field.

- [Support macros on all granularity](https://github.com/yahoo/fili/issues/1208)
   * Support `current` and `next` on `all` granularity
   * Added `currentDay`, `currentWeek`, `currentMonth`, `currentQuarter`, `currentYear`
   * Added `nextDay`, `nextWeek`, `nextMonth`, `nextQuarter`, `nextYear`

### Changed:
- [Make the default search results something more reasonable for a UI](https://github.com/yahoo/fili/issues/1223)
  * Gave the DimensionSearchServlet it's own default pagination that can be controlled via config, but defaulted to 50.

- [Added afterCache behavior to CacheV2ResponseProcessor](https://github.com/yahoo/fili/issues/1214)
  * Added afterCache() method
  * Refactored `CacheV2ResponseProcessor` to make it more extensible (broke up component tasks into overridable methods)
  * Updated to add response context to after method and make the response construction extensible.

- [Capability to bypass URL in Role based authentication filter](https://github.com/yahoo/fili/pull/1205)
  * Added capability to bypass URL in `RoleBasedAuthFilter` even when user is not part of allowed user roles.
  * Added a config variable bard__allowed_urls to specify list of bypassed URLs. 
  
- [Turning on metadata support ensures metablock in response](https://github.com/yahoo/fili/issues/1204)

- [Update GeneratedMetricInfo to avoid StackOverflow](https://github.com/yahoo/fili/issues/1194)
   * Update getType() tp avoid recursion leading to stack overflow.
   
- [Enhance Rate Limiting Capability](https://github.com/yahoo/fili/issues/1188)
   * Add UI and non-UI user count capability to rate limiter
   
- [Make LookBackQuery Extensible](https://github.com/yahoo/fili/issues/1182)
   * Make field and constructor less private

- [Make GroupBy Query Extensible](https://github.com/yahoo/fili/issues/1181)
   * Make field less private

- [Make Data Servlet Extensible](https://github.com/yahoo/fili/issues/1176)
  * Make field less private
  * Decomposed getData into overridable methods

- [Make DataApiRequest Servlet Extensible](https://github.com/yahoo/fili/issues/1176)
  * Add parent subclass with query parameters to support adding general query elements

- [Support unnamed dimension fields](https://github.com/yahoo/fili/issues/1179)

- [Support virtual dimensions](https://github.com/yahoo/fili/issues/1179)
  * Virtual dimensions have no storage associated with them and no physical columns
  * Virtual dimensions will be bound in children of `ExtensibleDataApiRequestImpl` to maintain backwards compatibility for now in `DataApiRequestImpl`
  * `SimpleVirtualDimension`  will have its columns serialized without '|fieldname'
  * Output columns are not driven by requested columns not columns in the result set.  Missing dimension rows will be expressed as null field values.
  * parsing of PerDimensionFields will no longer rely on dimension dictionary but instead on the already chosen grouping dimensions
  *  `REQUESTED_API_DIMENSION_FIELDS` context property was added to support JobServlet asynchronous requests, however it doesn't work well with VirtualDimensions so if the ApiRequest is able to be the authority on requested fields it will be used instead.
  * `DataServlet` sanitized empty path elements early to avoid validation later

- [Support null valued dimension, metric and time values to be null]https://github.com/yahoo/fili/issues/1183)  
   * Result to support null serialization of time, dimensions
   * Sortable nullable DateTime in ResultSetMapper
   * TimeDimensionResultSetMapper pulls dimension time into timestamp 
   * TimeDimension SimpleVirtualDimension to simplify time injecting queries
   * Made virtual dimension equality based on apiName (to allow distinct but equal request and response dimensions)

- [Bumping druid api dependency] (https://github.com/yahoo/fili/issues/1174)
   * Moved druid dependency to Druid 0.20
   * Disabled jackson validation error

- [Moving having and limitspec support to an interface and abstract implementation](https://github.com/yahoo/fili/issues/1185)
   * Added an interface for the group by and other related query types to offer abstract support for limitspec and having
   * Deprecated withOrderBy because there hasn't been a lot of conceptual gain drawing a line between logical and actual sort implementations.
   * Made `LookbackQuery` support withDimensions in cases where the inner datasource supports withDimensions.
   * Made `GroupByQuery` devolve limitspec and having support to parent abstract class.
      
- [Add MetricType subType and metadata](https://github.com/yahoo/fili/issues/1189)
   * Elaborated type into a class in `LogicalMetricInfo` supporting subtype and metadata
   * Updated metric makers to support type overrides
   * Updated ProtocolMetric generator to support modifying types
   * Updated `ThetaSketchMaker` to demonstrate having a type generated on the Maker that captured sketch precision as metadata.

### Removed:

### Fixed:

- [Memcached client default size was smaller than expected](https://github.com/yahoo/fili/issues/1231)
    * Made timeout configurable on `TimeoutConfigureBinaryConnectionFactory`
    * Removed error generating error handling where response already submitted before attempting to write to session

- [Fixed error where weekOfWeekYear moved backwards on sql set on years after 53 week years](https://github.com/yahoo/fili/issues/1221)
   * setWeekOfWeekYear behaves unexpectedly when the 53rd week of the prior is on the first of the year.
   * Added test
   * set default day of year to th 7th to guarantee correct calendar year resolution
   * added utility to `DruidAggregationQuery` to simplify mocking

- [Update to fix for weekly rounding time grain on sql](https://github.com/yahoo/fili/issues/1221)
  *  Moved from equality to acceptance.

- [Made unstable rate limit test more stable](https://github.com/yahoo/fili/issues/1217)
   * Made sure shared state was cleared more accurately between runs.

- [Turned down nuisance level logging of dictionaries to trace](https://github.com/yahoo/fili/issues/1216)
   * Made sure shared state was cleared more accurately between runs.

- [Address zero day bug on log4j by removing dependencies] (https://github.com/yahoo/fili/issues/1219)
   * Exclude all transitive dependencies on log4j version 1.

### Deprecated:

### Known Issues:

## Contract changes:
-------------


### Prior chages logged at CHANGELOG_0_x.md

[Changelog 0.x](CHANGELOG_0_x.md)
