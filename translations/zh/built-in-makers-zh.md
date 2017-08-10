内置建造器 Built In Makers
===============
下面是一个Fili内置指标列(metric)建造器表格。对于指标列(metric)建造器，请参考[`Configuring Metrics`][configuring metrics]。

Druid聚合操作建造器
---------------------------------------
如下列举的是一些一级指标列(metric)制造器，它们和Druid数据聚合功能一一对应。
[Druid aggregations][druid aggregations]。


1. [`长整数之和建造器LongSumMaker`][长整数之和建造器LongSumMaker]
2. [`双精度之和建造器DoubleSumMaker`][精度之和建造器DoubleSumMaker]
3. [`长整数最大值建造器LongMaxMaker`][长整数最大值建造器LongMaxMaker]
4. [`双精度最大值建造器DoubleMaxMaker`][双精度最大值建造器doubleMaxMaker]
5. [`长整数最小值建造器LongMinMaker`][长整数最小值建造器LongMinMaker]
6. [`双精度最小值建造器DoubleMinMaker`][双精度最小值建造器DoubleMinMaker]
7. [`计数器建造器CountMaker`][计数器建造器CountMaker]<sup>[1](#countCaveat)</sup>

另外，Fili的核心库支持Sketch类型和Sketch集合操作。详见
[theta sketches in Druid][druid sketch module]。

8. [`Sketch计数器建造器SketchCountMaker`][Sketch计数器建造器SketchCountMaker] -
sketchCount与[sketch aggregation][sketch module]相关联。

关于sketch的更多内容可参考http://datasketches.github.io/.

Druid后聚合操作建造器
--------------------------------------------

下列这些建造器在查询上应用了一次Druid后聚合运算符。这些运算符对应Druid的后期数据聚合操作。
[Druid post-aggregations][druid post-aggregations]

1. [`运算建造器ArithmeticMaker`][运算建造器ArithmeticMaker]
2. [`常量建造器ConstantMaker`][常量建造器ConstantMaker]

Fili也支持Sketch集合操作。

3. [`Sketch集合运算建造器SketchSetOperationMaker`][Sketch集合运算建造器SketchSetOperationMaker] - 是[Druid sketch module][sketch module]的一部分。

我们现在并不支持源生Druid `JavaScript` 及 `HyperUnique Cardinality`后聚合器。
Fili支持sketches而不是hyperUnique对象，因为二者功能重复。`JavaScript`后聚合器在大规模运行中会造成性能瓶颈。

自定义操作建造器
---------------------------------

这些建造器代表了一些并不能被单次Druid(后)数据聚合操作完成的操作。他们可能会创建新的列，细分Druid查询的规模，或者是在聚合操作之上的进一步数学运算（比如平均数）。

现在，我们只支持一个这样的内置指标列(metric)建造器：

1. [`聚合平均建造器AggregationAverageMaker`][聚合平均建造器AggregationAverageMaker]:
    `聚合平均建造器`可以在一个精度(dimension)上聚合一个方面的数据，然后取更广维度的聚合后的标准之平均数。
    举个例子，我们想计算2012年每月的日平均页面访问量。实际上，对于每个月份，我们先计算当月每日页面访问量，再把结果取平均值，就得到了当月的日平均页面访问量。
    `聚合平均建造器` 比较复杂，而且有[其自己的文档][aggregationAverageMaker-docs]
    详细说明。


<sub><a name="countCaveat">1</a>: 实际上`计数器建造器CountMaker`并不直接对应Druid的计数聚合操作，因为实现`计数器建造器CountMaker`的时候，Druid内部有一个bug。`计数器建造器CountMaker`的内部实现是创建一个多重查询。内层查询在每一个返回的结果列里插入一个值为1的常数列。外部查询则对该常数执行`长整数之和longSum`。

[聚合平均建造器AggregationAverageMaker]: ../fili-core/src/main/java/com/yahoo/bard/webservice/data/config/metric/makers/AggregationAverageMaker.java
[聚合平均建造器AggregationAverageMaker-文档]: https://github.com/yahoo/fili/issues/10
[数学建造器arithmeticMaker]: ../fili-core/src/main/java/com/yahoo/bard/webservice/data/config/metric/makers/ArithmeticMaker.java

[设置指标列configuring metrics]: configuring-metrics.md
[常数建造器constantMaker]: ../fili-core/src/main/java/com/yahoo/bard/webservice/data/config/metric/makers/ConstantMaker.java
[计数器建造器countMaker]: ../fili-core/src/main/java/com/yahoo/bard/webservice/data/config/metric/makers/CountMaker.java

[双精度最大值建造器DoubleMaxMaker]: ../fili-core/src/main/java/com/yahoo/bard/webservice/data/config/metric/makers/DoubleMaxMaker.java
[双精度最小值建造器DoubleMinMaker]: ../fili-core/src/main/java/com/yahoo/bard/webservice/data/config/metric/makers/DoubleMinMaker.java
[双精度之和建造器DoubleSumMaker]: ../fili-core/src/main/java/com/yahoo/bard/webservice/data/config/metric/makers/DoubleSumMaker.java
[Druid聚合操作druid aggregations]: http://druid.io/docs/0.8.1/querying/aggregations.html
[Druid后聚合操作druid post-aggregations]: http://druid.io/docs/0.8.1/querying/post-aggregations.html
[Druid sketch模块druid sketch module]: https://github.com/DataSketches/sketches-core

[长整数最大值建造器LongMaxMaker]: ../fili-core/src/main/java/com/yahoo/bard/webservice/data/config/metric/makers/LongMaxMaker.java
[长整数最小值建造器LongMinMaker]: ../fili-core/src/main/java/com/yahoo/bard/webservice/data/config/metric/makers/LongMinMaker.java
[长整数之和建造器LongSumMaker]: ../fili-core/src/main/java/com/yahoo/bard/webservice/data/config/metric/makers/LongSumMaker.java

[sketch计数器建造器sketchCountMaker]: ../fili-core/src/main/java/com/yahoo/bard/webservice/data/config/metric/makers/SketchCountMaker.java
[sketch集合操作建造器sketchSetOperationMaker]: ../fili-core/src/main/java/com/yahoo/bard/webservice/data/config/metric/makers/SketchSetOperationMaker.java
[sketch模块sketch module]: https://github.com/druid-io/druid/pull/1991/files
