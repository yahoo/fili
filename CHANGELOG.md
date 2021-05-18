Change Log
==========

All notable changes to Fili will be documented here. Changes are accumulated as new paragraphs at the top of the current
major version. Each change has a link to the pull request that makes the change and to the issue that triggered the
pull request if there was one.

Current
-------

### Fixed:

### Added:

### Changed:
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

### Removed:

### Fixed:

### Deprecated:

### Known Issues:

## Contract changes:
-------------


### Prior chages logged at CHANGELOG_0_x.md

[Changelog 0.x](CHANGELOG_0_x.md)
