Change Log
==========

All notable changes to Fili will be documented here. Changes are accumulated as new paragraphs at the top of the current 
major version. Each change has a link to the issue that triggered the change, and to the pull request that makes the
change.

Current
-------

### Added:


### Deprecated:


### Removed:


#### Changed:

-  [Removed timing component in JobsApiRequestSpec](https://github.com/yahoo/fili/pull/27)
    * Rather than setting an async timeout, and then sleeping, 
      `JobsApiReqeustSpec::handleBroadcastChannelNotification returns an empty Observable if a timeout occurs before the notification is received`
      now verifies that the Observable returned terminates without sending any
      messages.
-  [Fix Dimension Serialization Problem with Nested Queries](https://github.com/yahoo/fili/pull/15)
    * Modified `DimensionToDefaultDimensionSpec` serializer to serialize dimension to apiName if it is not the inner most query
    * Added helper `hasInnerQuery` to `Util` in serializer package to determine if current query is the inner most query or not
    * Added tests for `DimensionToDefaultDimensionSpec`

### Fixed:


### Known Issues:

