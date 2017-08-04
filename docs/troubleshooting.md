Troubleshooting
===============

The following offers some solutions to common issues when setting up and using Fili.
 If you come across any issues that you think belong here, please feel free to contribute.

Table of Contents
-----------------

- [General](#general)
- [My app's tests are failing](#app-tests-return-a-500-error)
- [Fili crashes with "IllegalStateException: Couldn't create dir: path/to/mdbm/dimensionCache/page"](#fili-crashes-and-with-illegalstateexception)
- [Metadata loader not healthy](#server-log-claims-the-segment-metadata-loader-is-not-healthy)
- [My query isn't working](#debugging-queries)


General
-------
The Fili logs are Jetty logs, so they can be found wherever your Jetty instance stores its logs.

The logs have a lot of information and should help indicate the error. [More info on logging levels][fili-logging]

App tests return a 500 error
----------------------------

It may be that the versions of dependencies in your application's POM are out of sync with the dependency
versions used by fili. If that is the case, then modifying your dependency versions to use the same version as
fili should solve the problem.


Fili crashes and with IllegalStateException
-------------------------------------------

There are two possible causes for Fili crashing with `IllegalStateException: Couldn't create dir: path/to/mdbm/dimensionCache/page`:
 
 1. The `dimensionCache` subdirectory in `path/to/mdbm` does not exist. Fili assumes `path/to/mdbm/dimensionCache`
already exists, and does not attempt to create it.
 
 2. `dimensionCache` exists, but does not have the correct read/write/execute permissions. The user that Jetty is 
 running under (typically `nobody`) needs to have read, write, and execute permissions on `dimensionCache`.


Server log claims the segment metadata loader is not healthy
------------------------------------------------------------

Your dimensions have never been updated, and don't have a `lastUpdated` field set. This can happen if you forgot to 
set up your dimension loader. You can get more details about the problem at the `/healthcheck` endpoint.
If the dimensions are not being loaded, then see the [Dimension Loading](#dimension-loading) for more details on how
to set up the dimension loader (or [configure all of your dimensions to be non-loaded](#non-loaded-dimensions)).


Debugging Queries
-----------------

If you make a query to Fili that doesn't work as expected it may be helpful to add the `format=debug` to the end of your query like below.

```
GET http://localhost:9998/v1/data/wikipedia/day?metrics=added&dateTime=2000-01-01/3000-01-01&format=debug
```

This lets you see the exact query which would have been sent to Druid. [See the Druid Querying Docs][druid-docs]



[druid-docs]: http://druid.io/docs/latest/querying/querying.html
[fili-logging]: contributing/logging-guidelines.md
