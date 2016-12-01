---
layout: guide
group: guide
title: Built In Makers
---

The following is a list of metric makers that are built-in to Fili. See [Configuring Metrics][configuring metrics] for
details on metric makers.

Makers for Druid Aggregation Operations
---------------------------------------

These makers are used to make first-level metrics, and are in a one-to-one mapping to the 
[Druid aggregations][druid aggregations].


1. [`LongSumMaker`][longSumMaker]
2. [`DoubleSumMaker`][DoubleSumMaker]
3. [`LongMaxMaker`][longMaxMaker]
4. [`DoubleMaxMaker`][doubleMaxMaker]
5. [`LongMinMaker`][longMinMaker]
6. [`DoubleMinMaker`][doubleMinMaker]
7. [`CountMaker`][countMaker]<sup>[1](#countCaveat)</sup>

Additionally, the Fili core libraries support Sketch datatypes and sketch set operations provided by
[theta sketches in Druid][druid sketch module].
  
8. [`SketchCountMaker`][sketchCountMaker] - The sketchCount is associated with the [sketch aggregation][sketch module]

More details about sketches can be found at http://datasketches.github.io/.


Makers for Druid Post-Aggregation Operations
--------------------------------------------

The following makers apply single Druid post-aggregators to queries. They correspond to the 
[Druid post-aggregations][druid post-aggregations]

1. [`ArithmeticMaker`][arithmeticMaker]
2. [`ConstantMaker`][constantMaker]

Sketch set operations are supported as well.

3. [`SketchSetOperationMaker`][sketchSetOperationMaker] - Part of the [Druid sketch module][sketch module]

We currently support neither the native Druid `JavaScript` nor the `HyperUnique Cardinality` post aggregators. Fili
supports sketches rather than hyperUnique objects, which handle the same problem domain. `JavaScript` post-aggregators 
are not considered production-safe in terms of performance or server load.


Makers for Constructed Operations
---------------------------------

These makers represent more complex operations which do not correspond directly to a single Druid aggregation or 
post-aggregation. They might create additional columns, constrain the grain of a druid query, or be a shorthand
for an arithmetic expression across aggregations (such as an average).

Currently, we only have one built-in metric maker of this type:

1. [`AggregationAverageMaker`][aggregationAverageMaker]:
    The `AggregationAverageMaker` allows us to aggregate a metric at one granularity, and then take the average of the 
    aggregated metric across a coarser granularity. For example, suppose we want to compute the daily average page views 
    for each month of the year 2012. In other words, for each month we first compute the total number of page views for 
    every day of the month. Then, we take the average of those totals, giving us the daily average page views for that 
    month. The `AggregationAverageMaker` is rather complex, and has [its own document][aggregationAverageMaker-docs] 
    that explains how `AggregationAverageMaker` works.
  
   
<sub><a name="countCaveat">1</a>: Technically, `CountMaker` does not translate directly to the Druid count aggregation, 
because of a bug that existed in Druid when `CountMaker` was first implemented. Instead, `CountMaker` creates a nested
query. The inner query adds a constant field with a value of 1 to each result row. The outer query then performs a 
`longSum` on said constant.</sub>


[aggregationAverageMaker]: https://github.com/yahoo/fili/blob/master/fili-core/src/main/java/com/yahoo/bard/webservice/data/config/metric/makers/AggregationAverageMaker.java
[aggregationAverageMaker-docs]: https://github.com/yahoo/fili/issues/10
[arithmeticMaker]: https://github.com/yahoo/fili/blob/master/fili-core/src/main/java/com/yahoo/bard/webservice/data/config/metric/makers/ArithmeticMaker.java

[configuring metrics]: configuring-metrics
[constantMaker]: https://github.com/yahoo/fili/blob/master/fili-core/src/main/java/com/yahoo/bard/webservice/data/config/metric/makers/ConstantMaker.java
[countMaker]: https://github.com/yahoo/fili/blob/master/fili-core/src/main/java/com/yahoo/bard/webservice/data/config/metric/makers/CountMaker.java

[doubleMaxMaker]: https://github.com/yahoo/fili/blob/master/fili-core/src/main/java/com/yahoo/bard/webservice/data/config/metric/makers/DoubleMaxMaker.java
[doubleMinMaker]: https://github.com/yahoo/fili/blob/master/fili-core/src/main/java/com/yahoo/bard/webservice/data/config/metric/makers/DoubleMinMaker.java
[doubleSumMaker]: https://github.com/yahoo/fili/blob/master/fili-core/src/main/java/com/yahoo/bard/webservice/data/config/metric/makers/DoubleSumMaker.java
[druid aggregations]: http://druid.io/docs/0.8.1/querying/aggregations.html
[druid post-aggregations]: http://druid.io/docs/0.8.1/querying/post-aggregations.html
[druid sketch module]: https://github.com/DataSketches/sketches-core

[longMaxMaker]: https://github.com/yahoo/fili/blob/master/fili-core/src/main/java/com/yahoo/bard/webservice/data/config/metric/makers/LongMaxMaker.java
[longMinMaker]: https://github.com/yahoo/fili/blob/master/fili-core/src/main/java/com/yahoo/bard/webservice/data/config/metric/makers/LongMinMaker.java
[longSumMaker]: https://github.com/yahoo/fili/blob/master/fili-core/src/main/java/com/yahoo/bard/webservice/data/config/metric/makers/LongSumMaker.java

[sketchCountMaker]: https://github.com/yahoo/fili/blob/master/fili-core/src/main/java/com/yahoo/bard/webservice/data/config/metric/makers/SketchCountMaker.java
[sketchSetOperationMaker]: https://github.com/yahoo/fili/blob/master/fili-core/src/main/java/com/yahoo/bard/webservice/data/config/metric/makers/SketchSetOperationMaker.java
[sketch module]: https://github.com/druid-io/druid/pull/1991/files
