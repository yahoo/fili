Configuring Metrics
===================

Table of Contents
-----------------

- [Overview](#overview)
- [Loading Metrics](#loading-metrics) 
    * [Naming Metrics](#naming-metrics)
- [Custom Metrics](#custom-metrics)
    * [Custom Makers](#custom-makers)
    * [Mappers](#mappers)
    * [Complex Metrics](#complex-metrics)   


Overview
--------

Fili metrics are either named aggregations of Druid metrics or named expressions over other Fili metrics. They range
from simple arithmetic to complex combinations of aggregations and post-aggregations.


There are two types of metrics: 

1. **First order metrics** are metrics that directly aggregate a Druid metric. For example, you might have two metrics, 
`page_views` and `additive_page_views`, which compute the [`longSums`][druid aggregations] of their equivalent Druid 
metrics, `druid_page_views` and `druid_additive_page_views`.  


2. **Higher order metrics** are metrics defined in terms of other metrics. For example, you might have a 
`total_page_views` metric that is the sum of `page_views` and `additive_page_views`. 


Loading Metrics
---------------

Fili relies on a [`MetricDictionary`][metricDictionary] to resolve names into metrics. This suggests there are
two pieces you need to define:

1. The names of your metrics

2. The metrics themselves

### Naming Metrics ###

You can name your metrics by implementing the [`ApiMetricName`][apiMetricName] interface. The interface has two
responsibilities: 

1. It provides a formal name to the metrics that can be used by other parts of the system (like the 
[`BaseTableLoader`][baseTableLoader]). 

2. It determines if the metric is valid for a given [`TimeGrain`][timeGrain].

For example, consider the following enum:

```java
    public enum ExampleApiMetricName implements ApiMetricName {
        PAGE_VIEWS,
        ADDITIVE_PAGE_VIEWS,
        TOTAL_PAGE_VIEWS;
      
        private final TimeGrain minimumGrain;

        ExampleApiMetricName() {
            this.minimumGrain = DefaultTimeGrain.DAY;
        }

        @Override
        public String getApiName() {
            return EnumUtils.enumJsonName(this);
        }

        @Override
        public boolean isValidFor(TimeGrain grain) {
            //Check if the passed in grain is coarser than the metric's grain.
            return grain.compareTo(minimumGrain) >= 0;
        }
        ...
    }
```

This enum specifies that all metrics are valid for time grains at the day level and coarser (week, month, year, etc).
The [`WikiApiMetricName`][wikiApiMetricName] in the [fili-wikipedia-example][fili-wikipedia-example] provides a
more complete example.

You also need to give Fili the names of the Druid metrics. This is done by implementing the [`FieldName`][fieldName]
interface in a similar manner as `ApiMetricName` (except Druid metric names do not require a minimum time grain). 

Implementing `FieldName` allows you to feed the Druid metric names into the [`BaseTableLoader`][baseTableLoader], which 
uses them to configure the physical tables. See the [`Binding Resources`](binding-resources) for more information about 
loading tables. The [`WikiDruidMetricName`][wikiDruidMetricName] enum provides an example.


### Building and Loading Metrics ###

Next, you need to write the code that builds the metrics and loads them into the `MetricDictionary` at Fili start up. To
do so, you need to implement the  [MetricLoader][metricLoader] interface, which has a single method 
`loadMetricDictionary`.

For example, suppose you want to register the three page view metrics introduced in [Overview](#overview).

Then the `loadMetricDictionary` method may look something like this:

```java
private MetricMaker longSumMaker;
private MetricMaker sumMaker;

@Override
public void loadMetricDictionary(MetricDictionary metricDictionary) {
    buildMetricMakers(metricDictionary);
    metricInstances = buildMetricInstances(metricDictionary);
    addToMetricDictionary(metricDictionary, metricInstances);
}
```

#### MetricMakers ####

A [`MetricMaker`][metricMaker] knows how to construct a [`LogicalMetric`][logicalMetric]. A `LogicalMetric` is a named 
Druid query plus a [`Mapper`](#mappers) for post-Druid processing. For example, the [`longSumMaker`][longSumMaker] knows 
how to construct a [`longSum`][druid aggregations] aggregation, while the [`sumMaker`][arithmeticMaker] knows how to 
construct an [arithmetic post aggregation][druid post-aggregations] using addition. 

For the running example, a `longSumMaker` and a `sumMaker` are needed:

```java
private void buildMetricMakers(MetricDictionary metricDictionary) {
    longSumMaker = new LongSumMaker(metricDictionary);
    sumMaker = new ArithmeticMaker(metricDictionary, ArithmeticPostAggregationFunction.PLUS);
}
```

#### MetricInstances ####

A [MetricInstance][metricInstance] knows how to use a `MetricMaker` to make a metric. In the running example, there
are three metrics: `page_views`, `additive_page_views`, and `total_page_views`. A `MetricInstance` is needed for
each metric:

```java
private List<MetricInstance> buildMetricInstances(MetricDictionary metricDictionary) {
    return Arrays.<MetricInstance>asList(
            new MetricInstance(PAGE_VIEWS, longSumMaker, DRUID_PAGE_VIEWS),
            new MetricInstance(ADDITIVE_PAGE_VIEWS, longSumMaker, DRUID_ADDITIVE_PAGE_VIEWS),
            new MetricInstance(TOTAL_PAGE_VIEWS, sumMaker, ADDITIVE_PAGE_VIEWS, PAGE_VIEWS)
    );
}
```

Observe that it is here that you tie metrics to their dependents. Since `page_views` and `additive_page_views` are
both Druid metrics, they rely on the respective druid metrics. Meanwhile, `total_page_views` relies on 
`additive_page_views` and `page_views`.

#### Creating Metrics and loading the MetricDictionary ####

Finally, the metrics need to be made, and added to the `MetricDictionary`. In the example, this is handled by
the `addToMetricDictionary` method:

```java
private void addToMetricDictionary(MetricDictionary metricDictionary, List<MetricInstance> metrics) {
    metrics.stream().map(MetricInstance::make).forEach(metricDictionary::add);
}
```

The [bard-wikipedia-example][bard-wikipedia-example] has a sample metric loader called
[`WIkiMetricLoader`][wikiMetricLoader].

Of course, Fili also needs to be told about the `MetricLoader` that you just defined. See
[Binding Resources](binding-resources) for details on how to do that.

Custom Metrics
--------------

Most custom metrics will be simple operations on metrics that already exist, using makers that already exist. In this 
case, defining the new metric is as simple as adding the following line to your 
[`buildMetricInstances`](#loading-metrics) method (or equivalent):

```java
   new MetricInstance(NEW_METRIC_NAME, metricMaker, DEPENDENT, METRIC, NAMES)
```
and adding `NEW_METRIC_NAME` to your implementation of [ApiMetricName][apiMetricName].

See [Built-in Metrics](built-in-makers) for a list of makers that come with Fili.
  
### Custom Makers ###

Sometimes you need more than what Fili provides out of the box. Perhaps you need to perform a calculation that cannot be
expressed in terms of other metrics, or you are working with a datatype that Druid does not support natively. In such 
cases, you can define your own custom maker. As a running example consider the [`ArithmeticMaker`][arithmeticMaker], 
which models post-aggregation arithmetic. 

First, you need to decide what kind of metric you want to define: first-order or higher-order.

If the metric is first-order, then you should extend [`RawAggregationMetricMaker`][rawAggregationMetricMaker]. You will 
also likely have to add a [custom Druid aggregation](http://druid.io/docs/0.8.1/development/modules.html) to your Druid 
cluster.

If the metric is higher-order, then you should extend [`MetricMaker`][metricMaker].

`ArithmeticMaker` is a higher-order metric, so it extends `MetricMaker`.

The bulk of the work in defining a custom Maker is in overriding the `makeInner` method, which performs the actual 
construction of the `LogicalMetric`:

```java
@Override
protected LogicalMetric makeInner(String metricName, List<String> dependentMetrics) {
...
}
```

`makeInner` generally performs the following steps:

1. **Merge Dependent Queries:** If there is more than one dependent metric, merge the queries of each dependent metric 
into a single query. This can be accomplished using the [`MetricMaker::getMergedQuery`][metricMaker] method. 

    Since `ArithmeticMaker` takes at least two other metrics, its dependent metrics need to be merged:

    ```java
    TemplateDruidQuery mergedQuery = getMergedQuery(dependentMetrics);
    ```

    A [`TemplateDruidQuery`][templateDruidQuery] is scaffolding of a Druid query that knows how to merge with another 
    `TemplateDruidQuery`.

2. **Build Aggregators and Post-Aggregators:** Construct the aggregations and post-aggregations the query depends on.

    In the case of [`ArithmeticMaker`][arithmeticMaker], the query consists of the aggregations performed by its 
    dependent metrics, a [field accessor][druid post-aggregations] for each aggregation, and a single 
    [arithmetic post-aggregation][druid post-aggregations].

    ```java
    Set<Aggregation> aggregations = mergedQuery.getAggregations();

    //Creates a field-accessor post-aggregation for the aggregator in each dependentMetric.
    List<PostAggregation> operands = dependentMetrics.stream()
            .map(this::getNumericField)
            .collect(Collectors.toList());
    PostAggregation arithmeticPostAgg = new ArithmeticPostAggregation(metricName, function, operands);
    ```


3. **Build the inner query:** Construct the inner query, if the metric requires query nesting. 

    The `ArithmeticMaker` uses the inner query constructed by the `getMergedQuery` method. See 
    [`AggregationAverageMaker`][aggregationAverageMaker-docs] for an example maker that builds a more interesting inner 
    query.

    ```java
    TemplateDruidQuery innerQuery = mergedQuery.getInnerQuery();
    ```

4. **Build TemplateDruidQuery:** Construct a [`TemplateDruidQuery`][templateDruidQuery]. 

    `ArithmeticMaker` constructs the following `TemplateDruidQuery`:

    ```java
    TemplateDruidQuery templateDruidQuery = new TemplateDruidQuery(
            aggregations,
            Collections.singletonSet(arithmeticPostAgg),
            innerQuery,
            mergedQuery.getTimeGrain()
    );
    ``` 

5. **Build Mapper:** Construct a [Mapper](#mappers). If a metric does not require post-Druid processing, then an 
instance of [`NoOpResultSetMapper`][noOpResultSetMapper] should be used.

    The `ArithmeticMaker` uses a [`ColumnMapper`][columnMapper] that is injected at construction time as 
    `resultSetMapper`. So all that needs to be done here is construct a new version of `resultSetMapper` with the name 
    of the metric being constructed:
    
    ```java
    ColumnMapper mapper = resultSetMapper.withColumnName(metricName);
    ```

6. **Build LogicalMetric:** Construct and return the `LogicalMetric`.

    ```java
    return new LogicalMetric(query, mapper, metricName);
    ```

### Mappers ###

Mappers are subclasses of [`ResultSetMapper`][resultSetMapper] that allow us to perform post-Druid processing in a
row-wise fashion. Fili constructs the post-Druid workflow by iterating through each `LogicalMetric` and composing their
Mappers into a function chain. When the Druid result comes in, the result set is then passed through each link in the
chain in the order of the metrics defined in the query.

To define a `Mapper`, you need to override two methods: `map(Result result, Schema schema)` and `map(Schema schema)`.
The first allows you to modify a single row in the result set. The second allows you to modify the result schema.

In order to allow Result processing in a (moderately) type-safe way, the [`Result`][result] class provides a variety of 
methods for extracting the value of a metric column of the appropriate type:

1. `getMetricValue`
2. `getMetricValueAsNumber`
3. `getMetricValueAsString`
4. `getMetricValueAsBoolean`
5. `getMetricValueAsJsonNode`

The first returns the metric value as an `Object`. The others cast the result to the appropriate type (`BigDecimal` in 
the case of `getMetricValueAsNumber`). 

[`NonNumericMetrics`][nonNumericMetrics] contains simple sample mappers for each of the non-numeric metrics. 

[`SketchRoundUpMapper`][sketchRoundUpMapper] is an example of a mapper for numeric metrics.

[`RowNumMapper`][rowNumMapper] is an example of a mapper that adds a column. 

### Complex Metrics ###

Complex (non-numeric) metrics are configured the same as custom numeric metrics. Fili supports all native JSON types:

1. Numbers
2. Strings
3. Booleans
4. Objects/Lists

Numbers, Strings, and Booleans are parsed into the corresponding Java types. JSON Objects and Lists are extracted from 
the Druid response as [`JsonNode`][jsonNode] instances. By default, Fili will pass the results from Druid on to the user
unchanged. If post-Druid processing is required, a [Mapper](#mappers) can be added to the mapper workflow stage. See 
[Custom Metrics](#custom-metrics) for details on how to add a Mapper to the workflow.

If Druid returns a JSON `null`, then Fili will parse it into the Java `null`.

[aggregationAverageMaker-docs]: https://github.com/yahoo/fili/issues/10
[apiMetricName]: ../src/main/java/com/yahoo/bard/webservice/data/config/names/ApiMetricName.java
[arithmeticMaker]: ../src/main/java/com/yahoo/bard/webservice/data/config/metric/makers/ArithmeticMaker.java

[baseTableLoader]: ../src/main/java/com/yahoo/bard/webservice/data/config/table/BaseTableLoader.java
[bard-wikipedia-example]: https://github.com/yahoo/fili/tree/master/fili-wikipedia-example

[columnMapper]: ../src/main/java/com/yahoo/bard/webservice/data/metric/mappers/ColumnMapper.java

[wikiApiMetricName]: https://github.com/yahoo/fili/blob/master/src/main/java/com/yahoo/wiki/webservice/data/config/names/WikiApiMetricName.java
[wikiDruidMetricName]:https://github.com/yahoo/fili/blob/master/fili-wikipedia-example/src/main/java/com/yahoo/wiki/webservice/data/config/names/WikiDruidMetricName.java
[wikiMetricLoader]: https://github.com/yahoo/fili/blob/master/src/main/java/com/yahoo/wiki/webservice/data/config/metric/WikiMetricLoader.java
[druid aggregations]: http://druid.io/docs/latest/querying/aggregations.html
[druid post-aggregations]: http://druid.io/docs/latest/querying/post-aggregations.html

[fieldName]: ../src/main/java/com/yahoo/bard/webservice/data/config/names/FieldName.java

[jsonNode]: http://fasterxml.github.io/jackson-databind/javadoc/2.0.0/com/fasterxml/jackson/databind/JsonNode.html

[logicalMetric]: ../src/main/java/com/yahoo/bard/webservice/data/metric/LogicalMetric.java
[longSumMaker]: ../src/main/java/com/yahoo/bard/webservice/data/config/metric/makers/LongSumMaker.java

[metricDictionary]: ../src/main/java/com/yahoo/bard/webservice/data/metric/MetricDictionary.java
[metricInstance]: ../src/main/java/com/yahoo/bard/webservice/data/config/metric/MetricInstance.java
[metricLoader]: ../src/main/java/com/yahoo/bard/webservice/data/config/metric/MetricLoader.java
[metricMaker]: ../src/main/java/com/yahoo/bard/webservice/data/config/metric/makers/MetricMaker.java
[metricMakerDir]: ../src/main/java/com/yahoo/bard/webservice/data/config/metric/makers

[nonNumericMetrics]: ../src/test/java/com/yahoo/bard/webservice/data/config/metric/NonNumericMetrics.java
[noOpResultSetMapper]: ../src/main/java/com/yahoo/bard/webservice/data/metric/mappers/NoOpResultSetMapper.java

[rawAggregationMetricMaker]: ../src/main/java/com/yahoo/bard/webservice/data/config/metric/makers/RawAggregationMetricMaker.java
[result]: ../src/main/java/com/yahoo/bard/webservice/data/Result.java
[resultSetMapper]: ../src/main/java/com/yahoo/bard/webservice/data/metric/mappers/ResultSetMapper.java
[rowNumMapper]: ../src/main/java/com/yahoo/bard/webservice/data/metric/mappers/RowNumMapper.java

[sketchRoundUpMapper]: ../src/main/java/com/yahoo/bard/webservice/data/metric/mappers/SketchRoundUpMapper.java

[templateDruidQuery]: ../src/main/java/com/yahoo/bard/webservice/data/metric/TemplateDruidQuery.java
[timeGrain]: ../src/main/java/com/yahoo/bard/webservice/data/time/TimeGrain.java

