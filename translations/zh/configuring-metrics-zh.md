配置数据度量（Configuring Metrics）
===================

目录
-----------------

- [概述](#概述)
- [加载度量](#加载度量)
    * [度量命名](#度量命名)
    * [建造和加载度量](#建造和加载度量)
- [自定义度量](#自定义度量)
    * [自定义建造器](#自定义建造器)
    * [映射](#映射)
    * [高级度量](#高级度量)   


概述
--------

Fili 的数据度量（metrics）包括 Druid 度量值聚合和 Fili 度量值聚合，两者涵盖简单的数学运算，到复杂的聚合和后聚合操作。


度量分两类:

1. **一阶（First order）度量** 直接聚合 Druid 的度量值/数据。例如，Fili 的两个度量 `page_views` 和
`additive_page_views`，分别计算和他们对应的两个 Druid 度量的 [`longSums`][druid aggregations]：`druid_page_views` 和
`druid_additive_page_views`。  

2. **高阶（Higher order）度量** 由其它度量值计算得来。例如，Fili 里面的 `total_page_views` 度量，是由 `page_views` 和
`additive_page_views` 之和得来的。


加载度量
---------------

Fili 用 [`MetricDictionary`][metricDictionary] 将度量名称映射到实际的度量对象（object），所以您需要定义两项内容：

1. 度量名称

2. 实际度量对象（object）

### 度量命名 ###

您可以用实现 [`ApiMetricName`][apiMetricName] interface 的方式来命名度量。该 interface 有两种功能：

1. 提供一个任何地方都适用的统一名称，方便其他地方（例如[`BaseTableLoader`][baseTableLoader]）使用。

2. 检查该度量是否适用于某个时间精度（[`TimeGrain`][timeGrain]）

例如，以下是一个 enum:

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

该 enum 里面的所有度量适用于以“天”为单位或者时间范围更大的（例如“周”，“月”，“年”等等）时间精度。您也可以参见
[Fili-wikipedia-example][fili-wikipedia-example]里面的[`WikiApiMetricName`][wikiApiMetricName]，这是一个更为完整的例子。

另外，您还需要把相对应的 Druid 度量名称告诉 Fili。方法是实现 [`FieldName`][fieldName]interface，实现方法和
`ApiMetricName` 类似（区别是 Druid 度量不需要时间精度的最小范围）。 

实现 `FieldName` 之后，您就能将 Druid 度量放入 [`BaseTableLoader`][baseTableLoader]，这个 loader 会用这些 Druid 度量去配置
实际数据列表（physical tables）。具体的加载数据列表方法请参见 [`Binding Resources`](binding-resources)。
[`WikiDruidMetricName`][wikiDruidMetricName] 自带了一个例子。

### 建造和加载度量 ###

下一步，您需要编写代码将命名好的度量在 Fili 启动的时候建造并加载到 `MetricDictionary`。要完成这一步，您需要实现
[`MetricLoader`][metricLoader] interface，这个 interface 包含了一个方法（method）叫做 `loadMetricDictionary`。

举个例子，如果你想放入[概述](#概述)中提到的三个页面访问度量，那么 `loadMetricDictionary` method 的实现方式类似于以下代码：

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

[`MetricMaker`][metricMaker] 是用来搭建 [`LogicalMetric`][logicalMetric] 的。`LogicalMetric` 实际上是一个特定的 Druid 查询
语句，加上一个 [`Mapper`](#mappers) 用来做 Druid 访问后期处理。例如，[`longSumMaker`][longSumMaker] 负责写一个
[`longSum`][druid aggregations] 聚合, [`sumMaker`][arithmeticMaker] 则是用相加运算写一个
[arithmetic post aggregation][druid post-aggregations]。

在我们的上一个例子中，我们要用到 `longSumMaker` 和 `sumMaker`：

```java
private void buildMetricMakers(MetricDictionary metricDictionary) {
    longSumMaker = new LongSumMaker(metricDictionary);
    sumMaker = new ArithmeticMaker(metricDictionary, ArithmeticPostAggregationFunction.PLUS);
}
```

#### MetricInstances ####

[MetricInstance][metricInstance] 使用 `MetricMaker` 来创建一个 metric。在上一个举例里，我们有三个度量：`page_views`，
`additive_page_views`，`total_page_views`。这样一来，每一个度量都需要一个 `MetricInstance`：

```java
private List<MetricInstance> buildMetricInstances(MetricDictionary metricDictionary) {
    return Arrays.<MetricInstance>asList(
            new MetricInstance(PAGE_VIEWS, longSumMaker, DRUID_PAGE_VIEWS),
            new MetricInstance(ADDITIVE_PAGE_VIEWS, longSumMaker, DRUID_ADDITIVE_PAGE_VIEWS),
            new MetricInstance(TOTAL_PAGE_VIEWS, sumMaker, ADDITIVE_PAGE_VIEWS, PAGE_VIEWS)
    );
}
```

注意，如果你的度量和其他基本度量存在依赖（相关）关系的话，就要在这里指明。`page_views` 和 `additive_page_views` 都是 Druid度量, 
他们自然而然地依赖于对应的 druid 度量。同时，`total_page_views` 是通过 `additive_page_views` 和 `page_views`计算得来的。

#### 创建并将度量加载到 MetricDictionary ####

最后，度量要被加载到 `MetricDictionary`。在我们的例子里，我们使用 `addToMetricDictionary` method：

```java
private void addToMetricDictionary(MetricDictionary metricDictionary, List<MetricInstance> metrics) {
    metrics.stream().map(MetricInstance::make).forEach(metricDictionary::add);
}
```

[Fili wikipedia example][fili-wikipedia-example] 带有一个基本的度量加载器叫做[`WikiMetricLoader`][wikiMetricLoader]。

另一个不可或缺的是您需要部署之前定义好的 `MetricLoader`。具体方法请详见 [Binding Resources](binding-resources) 。

自定义度量
--------------

大多数自定义度量都是在已经定义好的度量上进行基本的运算，建造器（makers）也是可以重复使用的。对于这种情况，定义一个新的度量只需要在
[`buildMetricInstances`](#loading-metrics) 方法（method）里（或者您自己对应的当中）中添加以下代码：

```java
   new MetricInstance(NEW_METRIC_NAME, metricMaker, DEPENDENT, METRIC, NAMES)
```

然后将 `NEW_METRIC_NAME` 加入到你的实现的 [ApiMetricName][apiMetricName]。

Fili 自带了一些已经定义好的 makers，详见 [Built-in Metrics](built-in-makers) 。
  
### 自定义建造器 ###

某些情况下 Fili 自带的建造器不足以满足您的需求，比如说数据计算不能用其他度量搭配数学运算完成，或者你需要一个 Druid 不预先设定的
数据类型。这个时候，您可以自定义自己的 maker。用 [`ArithmeticMaker`][arithmeticMaker] 举个例子，这个 maker 是用来进行后期
聚合数学运算的。

第一步，您需要确定度量类型：是一阶还是高阶。

如果是一阶，那么您需要 extend [`RawAggregationMetricMaker`][rawAggregationMetricMaker]。某些情况下，您还需要在 Druid 集群中
添加一个 [custom Druid aggregation](http://druid.io/docs/0.8.1/development/modules.html)。

如果是高阶，那么就需要 extend [`MetricMaker`][metricMaker]。

`ArithmeticMaker` 是一个高阶度量，所以是 extends `MetricMaker`。

自定义一个 Maker 主要是 override `makeInner` method，这个 method 定义了 `LogicalMetric` 的建造方法：

```java
@Override
protected LogicalMetric makeInner(String metricName, List<String> dependentMetrics) {
...
}
```

`makeInner` 执行如下步骤：

1. **合并依赖查询语句（Merge Dependent Queries）：** 如果存在至少一个相关度量（dependent metric），那么将每个相关度量的
查询语句合并成一个查询语句。合并方法用 [`MetricMaker::getMergedQuery`][metricMaker] method。 

    `ArithmeticMaker` 依赖于至少两个其他度量 ，所以相关度量需要合并：

    ```java
    TemplateDruidQuery mergedQuery = getMergedQuery(dependentMetrics);
    ```

    [`TemplateDruidQuery`][templateDruidQuery] 是一个特别方便的 Druid 查询语句模板，用来合并其它 `TemplateDruidQuery`。

2. **创建聚合器和后聚合器（Aggregators and Post-Aggregators）：** 搭建查询语句需要用到的聚合器和后聚合器。

    [`ArithmeticMaker`][arithmeticMaker] 中，对应的查询语句包含相关度量聚合：每个相关度量聚合的
    [field accessor][druid post-aggregations] 和一个 [arithmetic post-aggregation][druid post-aggregations]。

    ```java
    Set<Aggregation> aggregations = mergedQuery.getAggregations();

    //Creates a field-accessor post-aggregation for the aggregator in each dependentMetric.
    List<PostAggregation> operands = dependentMetrics.stream()
            .map(this::getNumericField)
            .collect(Collectors.toList());
    PostAggregation arithmeticPostAgg = new ArithmeticPostAggregation(metricName, function, operands);
    ```

3. **创建内层查询（inner query）：** 如果度量查询需要内含，则需要建造内层查询。

    `ArithmeticMaker` 使用 `getMergedQuery` method 搭建的内层查询。详见
    [`AggregationAverageMaker`][aggregationAverageMaker-docs]，那里有一个搭建内层查询的举例。

    ```java
    TemplateDruidQuery innerQuery = mergedQuery.getInnerQuery();
    ```

4. **创建 TemplateDruidQuery:** 搭建一个 [`TemplateDruidQuery`][templateDruidQuery]。

    `ArithmeticMaker` 搭建如下的 `TemplateDruidQuery`：

    ```java
    TemplateDruidQuery templateDruidQuery = new TemplateDruidQuery(
            aggregations,
            Collections.singletonSet(arithmeticPostAgg),
            innerQuery,
            mergedQuery.getTimeGrain()
    );
    ``` 

5. **创建映射（Mapper）：** 搭建一个 [映射](#mappers)。 如果度量不需要后期 Druid 聚合，那就用
[`NoOpResultSetMapper`][noOpResultSetMapper]。

    `ArithmeticMaker` 使用 [`ColumnMapper`][columnMapper]，这个映射在搭建的时候被注入（injected），成为 `resultSetMapper`。
    所以我们只需要建一个新的 `resultSetMapper`，配上需要被建造的度量名称：
    
    ```java
    ColumnMapper mapper = resultSetMapper.withColumnName(metricName);
    ```

6. **创建 LogicalMetric:** 创建 `LogicalMetric`.

    ```java
    return new LogicalMetric(query, mapper, metricName);
    ```

### 映射 ###

映射（Mappers）是 [`ResultSetMapper`][resultSetMapper] 的子类（subclass），用来对返回结果的每一行进行后期 Druid 处理。Fili
遍历每一个`LogicalMetric`，将每个的映射组合成一个 function chain，用这样的方式来组建后 Druid 的处理链。Druid 返回数据结果后，
数据被送入 chain 的每一个单元，单元顺序由查询语句中的度量定义顺序决定。

如果要定义一个 `Mapper`，您需要 override 两个 methods: `map(Result result, Schema schema)` 和 `map(Schema schema)`。第一
个用来处理修改返回数据的某一行结果，第二个用来处理修改结果的 schema。

为了保证数据处理正确处理每一种数据类型，[`Result`][result] class 提供了很多 methods ，用来提取某种数据类型的度量的列数据：

1. `getMetricValue`
2. `getMetricValueAsNumber`
3. `getMetricValueAsString`
4. `getMetricValueAsBoolean`
5. `getMetricValueAsJsonNode`

第一个将数据返回成一个 `Object`。其他的将其转换成对应的合适数据类型（`getMetricValueAsNumber` 会转换成 `BigDecimal`）。

[`NonNumericMetrics`][nonNumericMetrics] 包含了每一个非数值型度量的基本样例映射。

[`SketchRoundUpMapper`][sketchRoundUpMapper] 则是一个数值型度量映射。

[`RowNumMapper`][rowNumMapper] 是一个用于数据列相加的映射。

### 高级度量 ###

高级（非数值型）度量的配置方法和自定义数值型度量相同。Fili 支持所有原生 JSON 类型：

1. Numbers
2. Strings
3. Booleans
4. Objects/Lists


Numbers, Strings, 和 Booleans 自然而然地解析成对应的 Java 变量类型。JSON Objects 和 Lists 从 Druid 提取出来以后会被解析成
[`JsonNode`][jsonNode] 实例。Fili 默认将 Druid 返回的数据原封不动地发给用户。如果需要后 Druid 数据聚合处理，那么可以用一个
[Mapper](#mappers)，将其加到映射队列（mapper workflow stage）。添加方法详见 [Custom Metrics](#custom-metrics)。

如果 Druid 返回的 JSON 数据是 `null`，Fili 会将其解析成 Java `null`。

[aggregationAverageMaker-docs]: https://github.com/yahoo/fili/issues/10
[apiMetricName]: ../fili-core/src/main/java/com/yahoo/bard/webservice/data/config/names/ApiMetricName.java
[arithmeticMaker]: ../fili-core/src/main/java/com/yahoo/bard/webservice/data/config/metric/makers/ArithmeticMaker.java

[baseTableLoader]: ../fili-core/src/main/java/com/yahoo/bard/webservice/data/config/table/BaseTableLoader.java
[fili-wikipedia-example]: ../fili-wikipedia-example

[columnMapper]: ../fili-core/src/main/java/com/yahoo/bard/webservice/data/metric/mappers/ColumnMapper.java

[wikiApiMetricName]: ../fili-wikipedia-example/src/main/java/com/yahoo/wiki/webservice/data/config/names/WikiApiMetricName.java
[wikiDruidMetricName]: ../fili-wikipedia-example/src/main/java/com/yahoo/wiki/webservice/data/config/names/WikiDruidMetricName.java
[wikiMetricLoader]: ../fili-wikipedia-example/src/main/java/com/yahoo/wiki/webservice/data/config/metric/WikiMetricLoader.java
[druid aggregations]: http://druid.io/docs/latest/querying/aggregations.html
[druid post-aggregations]: http://druid.io/docs/latest/querying/post-aggregations.html

[fieldName]: ../fili-core/src/main/java/com/yahoo/bard/webservice/data/config/names/FieldName.java

[jsonNode]: http://fasterxml.github.io/jackson-databind/javadoc/2.0.0/com/fasterxml/jackson/databind/JsonNode.html

[logicalMetric]: ../fili-core/src/main/java/com/yahoo/bard/webservice/data/metric/LogicalMetric.java
[longSumMaker]: ../fili-core/src/main/java/com/yahoo/bard/webservice/data/config/metric/makers/LongSumMaker.java

[metricDictionary]: ../fili-core/src/main/java/com/yahoo/bard/webservice/data/metric/MetricDictionary.java
[metricInstance]: ../fili-core/src/main/java/com/yahoo/bard/webservice/data/config/metric/MetricInstance.java
[metricLoader]: ../fili-core/src/main/java/com/yahoo/bard/webservice/data/config/metric/MetricLoader.java
[metricMaker]: ../fili-core/src/main/java/com/yahoo/bard/webservice/data/config/metric/makers/MetricMaker.java
[metricMakerDir]: ../fili-core/src/main/java/com/yahoo/bard/webservice/data/config/metric/makers

[nonNumericMetrics]: ../fili-core/src/test/java/com/yahoo/bard/webservice/data/config/metric/NonNumericMetrics.java
[noOpResultSetMapper]: ../fili-core/src/main/java/com/yahoo/bard/webservice/data/metric/mappers/NoOpResultSetMapper.java

[rawAggregationMetricMaker]: ../fili-core/src/main/java/com/yahoo/bard/webservice/data/config/metric/makers/RawAggregationMetricMaker.java
[result]: ../fili-core/src/main/java/com/yahoo/bard/webservice/data/Result.java
[resultSetMapper]: ../fili-core/src/main/java/com/yahoo/bard/webservice/data/metric/mappers/ResultSetMapper.java
[rowNumMapper]: ../fili-core/src/main/java/com/yahoo/bard/webservice/data/metric/mappers/RowNumMapper.java

[sketchRoundUpMapper]: ../fili-core/src/main/java/com/yahoo/bard/webservice/data/metric/mappers/SketchRoundUpMapper.java

[templateDruidQuery]: ../fili-core/src/main/java/com/yahoo/bard/webservice/data/metric/TemplateDruidQuery.java
[timeGrain]: ../fili-core/src/main/java/com/yahoo/bard/webservice/data/time/TimeGrain.java

