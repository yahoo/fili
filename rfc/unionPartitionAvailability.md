# Enable union of partition availability

This RFC implements [Github Issue 627](https://github.com/yahoo/fili/issues/627).

## Goals

Enable query to multiple datasources with non-intersecting data availabilities to see an union view of availabilities, 
instead of an intersection.

## Existing Problem
If a Druid instance has 2 datasources DS1 and DS2 that have the following availabilities for some metric M:

```
+-----+---------------+-----------+
|     | 2017          | 2018      |
+-----+---------------+-----------+
| DS1 | available     | available |
+-----+---------------+-----------+
| DS2 | not-available | available |
+-----+---------------+-----------+
```

When we ask for data of M in 2017, it is legitimate to expect data from DS1 to be returned. But current behavior is
[returning a response with no data and missing interval of 2017 notified to client](https://github.com/yahoo/fili/blob/master/fili-core/src/main/java/com/yahoo/bard/webservice/table/availability/PartitionAvailability.java#L92).

What needs to be fixed is that, instead of returning no data, data of 2017 from DS1 shall be returned.

## Strategy
1. Turn [PartitionAvailability](https://github.com/yahoo/fili/blob/master/fili-core/src/main/java/com/yahoo/bard/webservice/table/availability/PartitionAvailability.java)
to an abstract class.
2. Have existing [PartitionAvailability](https://github.com/yahoo/fili/blob/master/fili-core/src/main/java/com/yahoo/bard/webservice/table/availability/PartitionAvailability.java)]
as a sub-class that implements the intersection(existing behavior).
3. Have a new sub-class that implements the union(new behavior).
4. Same logic(1 - 3) goes to [Table](https://github.com/yahoo/fili/blob/master/fili-core/src/main/java/com/yahoo/bard/webservice/table/PartitionCompositeTable.java)
and [Table Definition](https://github.com/yahoo/fili/blob/master/fili-core/src/main/java/com/yahoo/bard/webservice/data/config/table/DimensionListPartitionTableDefinition.java).


## Implementation
### 1st Milestone
Implement steps 1 - 3 - Availability part.

### 2nd Milestone
Implement step 4 - Table part.

### 3rd Milestone
Implement step 4 - Table definition part.

### 4th Milestone
Add documentations to give downstream projects an easy-to-read instructions on how to decide and use the 2
availabilities(intersection & union).
