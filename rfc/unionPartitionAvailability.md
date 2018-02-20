# Enable PhysicalTable-Specific Time Limit in PartitionCompositeTable

This RFC implements [Github Issue 627](https://github.com/yahoo/fili/issues/627).

## Goals

In a composite table, we need to have the underlying tables limit the times in which they participate. 

## Existing Problem
If a Druid instance has 2 datasources DS1 and DS2 that have the following availabilities for some metric M, and **they
are both behind a single composite table**

```
+-----+-----------------------+-----------------------+----------+
|     | 2017-01-01/2017-12-31 | 2018-01-01/2018-12-31 |  FUTURE  |
+-----+-----------------------+-----------------------+----------+
| DS1 |        HAS DATA       |        HAS DATA       | HAS DATA |
+-----+-----------------------+-----------------------+----------+
| DS2 |        NO DATA        |        HAS DATA       | HAS DATA |
+-----+-----------------------+-----------------------+----------+
```

Imagine DS2 is a new datasource added to Druid in 2018-01-01. Then DS2 is defined as starting on interval 2018/FUTURE.

If we query M on a groupBy dimension D in year 2017, it is legitimate to expect data from DS1 to be returned. But
current behavior is [returning a response with no data and missing interval of 2017 notified to client](https://github.com/yahoo/fili/blob/master/fili-core/src/main/java/com/yahoo/bard/webservice/table/availability/PartitionAvailability.java#L92).

What needs to be fixed is that, instead of returning no data, data of 2017 from DS1 shall be returned.

A summary of the new behavior is the following:

* If DS1 is included for filter set X and DS2 is also included for filter set X
* If DS2 is defined as starting on interval 2018/FUTURE

then the availability is

Requested Interval is "THE PAST/2018" => Intersect: all tables in range (DS1)

Requested Interval is "2018/THE FUTURE" => Intersect: all tables in range (DS1, DS2)

## Strategy
Redefine the availability of intervals as follows:

Add a "mark" to each partition in `PartitionCompositeTable`. The "mark" indicates **a starting instance of time, T,
after which data can possibly be available.**

With "mark", the decision on "missing intervals" is the following:

* If there is no data in this interval AND this interval is **before** T => NOT a missing interval; do not include it in
the availability intersection operation

* If there is no data in this interval AND this interval is **after** T => this IS a missing interval and will be part
of intersection operations

The value of T of each partition will be configured and loaded on start.

To calculate missing intervals, the idea is to take union of all available intervals and then "subtract" intervals which
are missing. The formula is to union each partition's difference of what's possibly available and what's actually
available. Here is an example:

```bash
DS1:
ancient         T1                                       latest
   |............|++++++++++++++++++++++++++++++++...++++++|
                | <-    I1_defined = I1_available      -> |   
   
DS2:
ancient                T2                                latest
   |....................|---------+++++++++++++++...++++++|
                         \__  ___/
                            \/
                       missingInterval
                                  |  <-  I2_available  -> |
                        | <-       I2_defined          -> |          
```

What's really missing from the example above is calculated like this:

(`I1_defined` - `I1_available`) UNION (`I2_defined` - `I2_available`) = missingInterval

Note that `I_defined` = T -> timeOfLatestData

## Implementation
The easiest implementation with the least amount of additional codes would be the following:

1. Create a sub-class of `DimensionListPartitionTableDefinition` that adds an additional map

    ```java
    Map<TableName, DateTime>
    ```

    that maps name of a Physical Table to it's mark, [T](#strategy).

2. Create a sub-class of `PartitionCompositeTable`. Pass the map in step 1 to it and use the map to construct a new map 

    ```java
    Map<Availability, DataTime>
    ```
    
    that maps an `Availabiltiy` to the mark T. Pass this new map to the
    [construction of `PartitionAvailability`](https://github.com/yahoo/fili/blob/master/fili-core/src/main/java/com/yahoo/bard/webservice/table/PartitionCompositeTable.java#L56-L57)
    
3. Create a sub-class of `PartitionAvailability`. Pass the map in step 2 to it and use the new
   map (call it "**availabilityToStart**") to
   [merge availabilities](https://github.com/yahoo/fili/blob/a23cf0412b6b50a7ca7cd718ef9b49cc79343972/fili-core/src/main/java/com/yahoo/bard/webservice/table/availability/PartitionAvailability.java#L89-L93)
   according to how we calculate "missingInterval" [above](#strategy):

    ```java
    private SimplifiedIntervalList mergeAvailabilities(PhysicalDataSourceConstraint constraint) {
        return filteredAvailabilities(constraint)
                .map(availability -> availability.getAvailableIntervals(constraint))
                .reduce(SimplifiedIntervalList::union).orElse(new SimplifiedIntervalList())
                .subtract(getMissingInterval(constraint));
    }
 
    private SimplifiedIntervalList getMissingInterval(PhysicalDataSourceConstraint constraint) {
       return filterAvailabilities(constrant)
               .map(availability ->
                       new SimplifiedIntervalList(
                            Collections.singleton(
                                 new Interval(
                                      availabilityToStart.get(availability),
                                      availability.getAvailableIntervals(constraint).getEnd()
                                 )
                            )
                       ).substract(availability.getAvailableIntervals(constraint)))
               .reduce(SimplifiedIntervalList::union).orElse(new SimplifiedIntervalList());
    }
    ```
    
    Note that `availability.getAvailableIntervals(constraint).getEnd()` returns "[timeOfLatestData](#strategy)"
