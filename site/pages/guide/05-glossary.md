---
layout: guide
group: guide
title: Glossary
---

This is collection of terms related to Fili and its concepts.

Table of Contents
-----------------

- [Metrics](#metrics)
  - [Aggregation](#aggregation)
  - [Post Aggregation](#post-aggregation)
  - [Maker](#maker)
  - [Mapper](#mapper)
  - [Logical Metric](#logical-metric)
  - [Template Druid Query](#template-druid-query)
  - [Sketch](#sketch)
  - [Metric Column](#metric-column)
- [Dimensions](#dimensions)
  - [Dimension](#dimension)
  - [Dimension Row](#dimension-row)
  - [Star Schema](#star-schema)
  - [Snapshot Cache](#snapshot-cache)
  - [Search Provider](#search-provider)
  - [Filter](#api-filter)
- [Tables](#tables)
  - [Logical Table](#logical-table)
  - [Physical Table](#physical-table)
  - [Slice](#slice)
- [Time](#time)
  - [Time Grain](#time-grain)
  - [Granularity](#granularity)
  - [Interval](#interval)
- [Workflow](#workflow)
  - [Workflow](#workflow)
  - [Request Handler](#request-handler)
  - [Response Processor](#response-processor)
  - [Result](#result)
  - [Result Set](#result-set)
  - [Dimension Loader](#dimension-loader)
- [Application Concerns](#application-concerns)
  - [Health Check](#health-check)
  - [Feature Flag](#feature-flag)
  - [System Config](#system-config)
  - [Request Log](#request-log)
  - [Fact Store](#fact-store)
- [Capabilities](#capabilities)
  - [Top N](#top-n)
  - [Limit](#limit)
  - [Pagination](#pagination)
  - [Partial Data](#partial-data)
  - [Volatile Data](#volatile-data)
  - [Weight Check](#weight-check)
- [Miscellaneous](#miscellaneous)
  - [Spock](#spock)
  - [Groovy](#groovy)
  - [Servlet](#servlet)
  - [Fact](#fact)
  - [Web Service](#web-service)


Metrics
-------

### Aggregation

An Aggregation is a baseline part of a metric definition, and it tells the Fact Store how to aggregate the contents of 
a Metric Column when it aggregates the rows in the Data Source. Aggregations can also be thought of as "accumulators" 
or "reducers" from other contexts like stream processing or Map Reduce.

### Post Aggregation

A Post Aggregation is a 2nd-level metric definition component that usually applies some sort of transformation or 
expression to Aggregations or other Post Aggregations. Post Aggregations can be thought of as "operators" in an 
expression context, and tend to be composed into an expression tree.

### Maker

A Maker (or Metric Maker) is a configuration helper intended to make it easier to build Logical Metrics through 
composition.

### Mapper

A Mapper (or Result Set Mapper) is a formula component of a Logical Metric that allows for calculations and 
manipulations of the logical metric after the Result Set has come back from the Fact Store.

### Logical Metric

A Logical Metric is the user-facing (ie. API-oriented) definition of a metric. It has two main types of components: 
Metadata components and Formula components. The metadata components include things like API Name, Description, and 
Category, and the formula components, which define how the metric is calculated, include the Template Druid Query and 
Result Set Mapper.

### Template Druid Query

A Template Druid Query is a partial Druid query used to define Logical Metrics. It is a partial Druid query in that it 
doesn't have all of the fields of a Druid query. In particular, a Template Druid Query does not have a Data Source 
field.

### Sketch

A Sketch is a probabilistic set, often used for unique counting.

### Metric Column

A Metric Column is a column in a Fact Store that holds Metric values (as opposed to Dimension values). Metric columns 
cannot be grouped by and cannot be filtered on.


Dimensions
----------

### Dimension

A Dimension is a component of an "address" for a fact or metric. Dimensions can be used to group and filter facts. 
Dimensions can be thought of as lookup tables in a Star Schema, and have similar components:

- An API name that is used by users in the API
- A Druid Name that links it to the Physical Table columns in the Fact Store
- A set of Fields (one of which must hold unique values, called the Key field)
- A collection of Dimension Rows, which are the tuples of values for each of the Dimension's fields

### Dimension Row

A tuple of values for a Dimension with one value for each Field of that Dimension.

### Star Schema

A common database structure often found in analytics warehouses. It consists of a central Fact table and a set of 
Dimension tables that are used as lookup tables, joined to the central fact table via foreign keys.

### Snapshot Cache

A point-in-time cache. Because it is point-in-time, rather than continually updated, it's staleness applies to the 
entire cache, rather than individual entries in the cache which is typical of other caching strategies. This 
simplicity allows for a much simpler interaction model for the cache and may allow for better performance at the cost 
of a higher risk of stale data.

### Search Provider

A Search Provider is the component of a Key Value Dimension that is responsible for maintaining indexes for the 
Dimension Rows, as well as for using those indexes when searching for Dimension Rows via filtering.

### API Filter

An API Filter is the API concept of a filter for particular Dimension Rows within a Dimension. API Filters consist of 
3 components:

- **Selector**: Selects what Dimension Rows the filter is seeking and indicates what field the filter applies to.
- **Operator**: Determines how the values in the value list are used to find the Dimension Rows
- **Value List**: The list of values for the operator to use when selecting the dimension rows

### Dimension Loader

A Dimension Loader is responsible for loading the Dimension Rows into a Fili instance.


Tables
------

### Logical Table

A Logical Table is the user-facing connection between Dimensions, Logical Metrics, and Granularity. It primarily 
indicates what combinations of those things are legal to use together in a query.

### Physical Table

A Physical Table represents a fact table to query in the Fact Store. For Druid, this is usually a Data Source.

### Slice

A Slice (or Performance Slice) is another name for a Physical Table. It's often a more aggregated columnar subset of 
some wider Physical Table that is used for performance reasons.


Time
----

### Time Grain

A Time Grain is a Period that can be used to indicate how facts either have been, or should be, grouped when 
aggregating along the Time dimension.

### Granularity

A Granularity is a Time Grain that can also be "all", which doesn't group by any time bucket at all.

### Interval

An Interval is a specification of a concrete range of time based on two bounding exact instants in time.

### Period

A Period is a specification of a duration that is based on a particular calendaring system. Usually, a period is 
expressed in terms of Years, Months, Weeks, Days, Hours, Minutes, and Seconds.


Workflow
--------

### Workflow

Workflow refers to the general flow of processing a Data request after the Data Servlet has finished it's work. The 
Workflow consists of 3 main phases: Request Handling, Response Processing, and Result Set Mapping. The Request 
Handling phase is static, and is defined through a Request Workflow Provider, while the other 2 phases are built 
dynamically during the Request Handler phase.

### Request Handler

A Request Handler is the type of component that makes up the Request Handling phase of the workflow. Request Handlers 
work with a Druid Query and have the API Request available. This allows them to do things like manipulate the Druid 
query to, for example, enhance a metric or update the query in ways that the Template Druid Query for the Logical 
Metric was not able to express.

### Response Processor

A Response Processor is the component that makes up the Response Processing phase of the Workflow. They primarily deal 
with JSON responses that come from the Fact Store. The terminal Response Processor is also expected to do a number of 
different steps as well that will likely expand and get broken out into more explicit workflow steps in the future:

- Convert the JSON response into a Result Set
- Apply any Result Set Mappers
- Format the response

### Result Set

A Result Set is a collection of Results. It is essentially a tabular representation where the columns are Dimension or 
Metric columns, and the rows are Results.

### Result

A Result is a single row in a Result Set. It is essentially a tuple with a value for each column in it's Result Set. 
Another way to think about a Result is that it's a set of Metrics and their corresponding dimensional "address".


Application Concerns
--------------------

### Health Check

A Health Check is a mechanism to programatically assert if the web service is healthy or not in a binary yes/no 
fashion.

### Feature Flag

A Feature Flag is a boolean configuration mechanism that can be used to turn certain capabilities on or off via a 
simple flag-like setting.

### System Config

System Config is a layered configuration infrastructure that makes it easy to handle configuration within the code, as 
well as easy to specify configuration in different environments.

### Request Log

The Request Log is an extensible log line that Fili emits after a request has been handled and responded to. The data 
in this log line is built up as the request is processed and it includes information about nearly every phase of 
processing a request, including how long things took at both fine-grained and aggregate levels.

### Fact Store

A Fact Store is the generic name for the source of the fact rows that get aggregated, like Druid, Hive, or an SQL-
based Relational Database Management System (RDBMS).


Capabilities
------------

### Top N

Top N is a constraint on the number of rows that will be returned in each time bucket of a Data request in Fili. Top N 
is different from Limit in that it applies within a single time bucket for Data responses, whereas Limit applies at 
the top level of the collection.

### Limit

Limit is a constraint on the number of rows that will be returned to a request for a collection resource in Fili. 
Limit is different from Top N in that it applies at the top level of the collection, whereas Top N applies within a 
single time bucket for Data responses.

### Pagination

Fili allows for paginating collection responses, which means that users can request just a specific subset (ie. a 
page) of an otherwise larger collection.

### Partial Data

Partial Data is a situation that occurs when an aggregation bucket does not contain all of the information that the 
request indicates it should have. One situation where this could happen is if a request asks for monthly aggregated 
data, but a full month is not available in the Fact Store (perhaps the end of the month hasn't gotten here yet). In 
that case, the response indicates that the bucket is for a month of data, but the underlying Fact Store didn't have a 
full month of data to aggregate, resulting in a bucket that looks like it has a month of data, but only has a partial 
month.

Fili has the ability to protect against this for Fact Stores that provide availability information.

### Volatile Data

Volatile Data is similar to Partial Data, but instead of an aggregation bucket being partial because of data 
unavailability, the bucket is _volatile_ because the data aggregated into it is still changing. Aggregating over time 
ranges that Druid is still ingesting into via Realtime nodes is an example of when this might happen.

Fili has the ability to detect and indicate Volatile Data buckets if it is given a Volatility Provider to indicate 
what time ranges are volatile for a Physical Table.

### Weight Check

Weight Check is a Fili capability that estimates the memory pressure a query will put on Druid Broker nodes for 
queries that use Sketches.


Miscellaneous
-------------

### Spock

Spock is a Groovy-based BDD-style testing framework.

### Groovy

Groovy is a dynamic JVM-based programming language. It's dynamic and flexible nature make it particularly good for 
uses like testing.

### Servlet

A Servlet is a Java construct that usually is designed to handle an HTTP request. For Fili, we also have a Servlet 
construct, and while it's similar to the Java construct, it's more akin to a Controller in other MVC web frameworks 
like Ruby on Rails or Grails.

### Fact

A Fact (also known as a Metric) is some piece of measured information that is often addressed by a tuple of 
dimensional values.

### Web Service

A software system, usually located at the server side in a client-server organization on the web, acting as middleware 
or interface between a client and a database server. In a more general definition, 
[W3C](https://www.w3.org/TR/2004/NOTE-ws-gloss-20040211/#webservice) defines web service as 

> a software system designed to support interoperable machine-to-machine interaction over a network. 

See also [web service](https://en.wikipedia.org/wiki/Web_service).
