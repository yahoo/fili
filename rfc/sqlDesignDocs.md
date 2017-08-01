# Sql Design Docs

## Overview

Currently Fili only supports Druid for performing aggregations/grouping/post aggregations. Fili's processing pipeline is capable of sending queries to different backends, but lacks 

1. the translation (serialization) from `DruidQuery` to other query formats. 
2. the ability to manually build up a `ResultSet` and pass it through the `ResponseProcessor` instead of passing Druid's son responses

This RFC aims to solve the first issue - lack of translation to another query model by using Calcite to build up SQL queries and processing the results into a Druid format ( `GroupByQuery` response) which Fili is able to understand.

## Goals

Allow customers to query Fili's API and make a `DruidAggregationQuery`  which will be processed by a SQL backed datasource instead of Druid.

Customers will be able to configure their connection to a SQL database similar to Druid through the `applicationConfig.properties` and build a table in Fili specifically backed by SQL. They can then depend on the `fili`  and `fili-sql`module as opposed for configuring their application with SQL support.

## Implementation

1. Define a `SqlBackedClient` 

   ```java
   Future<JsonNode> executeQuery(
     DruidQuery<?> druidQuery,
     SuccessCallback successCallback,
     FailureCallback failureCallback
   ); //SqlBackedClient.java
   ```

   This method accepts and translates a `DruidQuery` to SQL, executes it, reads the results, and builds Druid json results.

2. Define a `DruidQueryToSqlConverter` so that we can query the backend

   1. Define a `SqlTimeConverter` which handles grouping/filtering on intervals. The default behaviour for grouping is to explode the timegrain out into the equivalent DateTime parts for grouping, then reassemble a DateTime when processing the results.

      ```java
      AllGranularity.INSTANCE -> ()
      DefaultTimeGrain.YEAR   -> (YEAR)
      DefaultTimeGrain.MONTH  -> (YEAR, MONTH)
      DefaultTimeGrain.WEEK   -> (YEAR, WEEK)
      DefaultTimeGrain.DAY    -> (YEAR, DAYOFYEAR)
      DefaultTimeGrain.HOUR   -> (YEAR, DAYOFYEAR, HOUR)
      DefaultTimeGrain.MINUTE -> (YEAR, DAYOFYEAR, HOUR, MINUTE)
      ```

   2. Define an `ApiToFieldMapper` to handle converting from API names to physical names.

   3. Make a `DruidSqlAggregationConverter` which provides a way to make SQL aggregations from the original `Aggregation`

      ```java
      Optional<SqlAggregationBuilder> fromDruidType(Aggregation aggregation, ApiToFieldMapper mapper);
      ```

   4. Define a `FilterEvaluator` which converts a `Filter` into an equivalent filter for SQL

   5. Define a `HavingEvaluator` which converts a `Having` into an equivalent having for SQL

   6. Define a `PostAggregationEvaluator` to manually perform the `PostAggregation` and type casting after the results have been collected.

3. Define a `SqlResultSetProcessor` so that the results can be read into the Druid `GroupByQuery` style and final name mapping + type castings can be performed.

4. Define a `SqlPhysicalTable` which will allow for detecting SQL backed queries and additionally provide the `schemaName` and `timestampColumn` parameters for querying the table.

5. Define a `SqlDimensionLoader` which will perform the equivalent operations as `DruidDimensionLoader` for SQL backed datasources.

6. Define a `SqlRequestHandler` to detect SQL backed queries which will be added to a `SqlWorkflow`. The `SqlRequestHandler` should be injected before any caching can occur because Fili shouldn't handle caching against a SQL backed datasource

## Milestones

1. Create a `fili-sql` module.
2. Use a `SqlBackedClient` to correctly execute `TimeSeriesQuery`, `GroupByQuery`, and unoptimized `TopNQuery` against a SQL backend
3. Use the `SqlPhysicalTable` to define a SQL backed datasource and correctly intercept the request using `SqlRequestHandler`
4. Implement the `SqlDimensionLoader` to allow for dimension caching and operations on dimensions.
5. Implement Nested `GroupByQuery`, `LookbackQuery`, `TopNQuery`

## Required Hacks

It would be great if part 2 of adding the SQL backend was implemented, but it's not necessary.

As a result not having part 2, there is a required hack of wrapping the `DruidAggregationQuery` into a `SqlAggregationQuery` so that the json results returned are always read the same (`GroupByQuery` style).