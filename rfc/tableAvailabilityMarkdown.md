# Table resource supporting filtered availability

This RFC implements [Github Issue 306](https://github.com/yahoo/fili/issues/306)

## Goals

1. Show the queryable range of time as part of the Logical Table endpoint view.
2. Allow the queryable range to reflect constraints as would be found in a data request.
  * Required metrics and dimensions
  * Applied filters


## Calculating availability

Availability for a logical table shall be defined as the maximal set of intervals that the table may be able to respond
 to.  Or put another way, the table can certainly not respond to any intervals beyond the range marked available.  As
 such the calculation will involve unioning the available intervals for all physical tables backing the logical table.
 
 Constrained availability will be defined as the availability for a table given a set of query constraints.  This will
 be the result of first filtering out physical tables on the logical table which cannot answer the query.  This may end
 up excluding ALL physical tables, meaning an empty availability.
 
 Existing code for constrained physical tables should be able to be repurposed:

1. Build a `QueryPlanningConstraint` from the TableApiRequest
    From: `DruidQueryBuilder`
    ```java 
        TableGroup group = logicalTable.getTableGroup();
    
        // Resolve the table from the the group, the combined dimensions in request, and template time grain
        QueryPlanningConstraint constraint = new QueryPlanningConstraint(request, template);
    
    ```
    * In the case of unconstrained queries, ensure that an 'unconstrained' constraint is built.
    * `QueryPlanningConstraint` will need more constructor richness to handle making metric constraints optional
    
2.  Get the tables from the `LogicalTable`'s `tableGroup`.
3.  Use matchers to filter the tables from the table group given the `QueryPlanningConstraint`
From: `DefaultPhysicalTableResolver`
    ```java
        public List<PhysicalTableMatcher> getMatchers(QueryPlanningConstraint requestConstraint) {
            return Arrays.asList(
                    new SchemaPhysicalTableMatcher(requestConstraint),
                    new AggregatableDimensionsMatcher(requestConstraint),
                    new TimeAlignmentPhysicalTableMatcher(requestConstraint)
            );
        }
    ```

    From: `BasePhysicalTableResolver`
    ```java
        public Set<PhysicalTable> filter(
                Collection<PhysicalTable> candidateTables,
                QueryPlanningConstraint requestConstraint
        ) throws NoMatchFoundException {
            return filter(candidateTables, getMatchers(requestConstraint));
        }
    ```
4. Map to collect the intervals from the filtered collection of physical tables, and union those intervals.  
    `PhysicalTable.getAvailableIntervals(DataSourceConstraint constraint)`
    * It may make sense to build a disposable UnionPhysicalTable to do this reduction rather than have a separate 
    reducer.

## Availability API Changes


From [Github Issue 306](https://github.com/yahoo/fili/issues/306)
```
/tables/myTable/week/dim1/dim2?metrics=myMetric&filters=dim3|id-in[foo,bar]
```

The table metadata endpoint should now accept an optional list of path separated grouping dimensions, an optional list
of metrics, and an optional filter clause.

Code currently in `DataApiRequest` will be shared with `TablesApiRequest` to support parsing these elements.

## Response Format Changes:

Add `availability` as a list of serialized intervals to the default table format.

```
{
      "category": "General",
      "name": "shapes",
      "longName": "shapes",
      "timeGrain": "day",
      "retention": "P1Y",
      "description": "shapes",
      "availableIntervals": ["2016-05-01 00:00:00.000/2017-05-27 00:00:00.000"]
      "dimensions": [
        {
          "cardinality": "0",
          "category": "General",
          "name": "color",
          "longName": "color",
          "uri": "http://localhost:9998/dimensions/color"
        }
      ],
      "metrics": [
        {
          "category": "General",
          "name":"rowNum",
          "longName": "rowNum",
          "uri": "http://localhost:9998/metrics/rowNum"
        }
      ]
    }
```

## Implementation

### 1st Milestone - Table-wide Availability
In `getLogicalTableFullView`, take the TablesApiRequest to calculate the availability for the logical table (calculate
directly on logical table(union the availability for the logical table) without taking the TablesApiRequest into account)

### 2nd Milestone - Have Tables Endpoint Support (but not use) Additional Query Parameters
Make the availability consider the TablesApiRequest by passing it into the `getLogicalTableFullView` method

### 3rd Milestone - Filter Using Dimensions, Metrics, and Filters
Have TablesApiRequest accept metrics and dimensions from the API request