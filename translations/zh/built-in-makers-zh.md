内置数据度量建造器 Built In Makers
===============

下面是Filiti提供的所有数据度量建造器。建造器细节请参考[`Configuring Metrics`][configuring metrics]。

Druid聚合操作建造器
---------------------------------------

如下列举的是一些基本运算制造器，它们和[Druid数据聚合功能][druid aggregations]一一对应。


1. [`长整数之和建造器 - LongSumMaker`][LongSumMaker]
2. [`双精度之和建造器 - DoubleSumMaker`][DoubleSumMaker]
3. [`长整数最大值建造器 - LongMaxMaker`][LongMaxMaker]
4. [`双精度最大值建造器 - DoubleMaxMaker`][doubleMaxMaker]
5. [`长整数最小值建造器 - LongMinMaker`][LongMinMaker]
6. [`双精度最小值建造器 - DoubleMinMaker`][DoubleMinMaker]
7. [`计数器建造器 - CountMaker`][CountMaker]<sup>[1](#countCaveat)</sup>

另外，Fili的核心库支持Sketch类型和Sketch集合操作。详见[theta sketches in Druid][druid sketch module]。

8. [`SketchCount 建造器`][SketchCountMaker] - sketchCount与[sketch aggregation][sketch module]相关联。

关于sketch的更多内容可参考http://datasketches.github.io/.

Druid后聚合操作建造器
--------------------------------------------

下列这些建造器在查询上应用了单次Druid后聚合运算符。这些运算符对应[Druid的后期数据聚合操作][druid post-aggregations]。

1. [`数学运算建造器 - ArithmeticMaker`][ArithmeticMaker]
2. [`常量建造器 - ConstantMaker`][ConstantMaker]

Fili也支持Sketch集合操作。

3. [`Sketch集合运算建造器 - SketchSetOperationMaker`][SketchSetOperationMaker] - 是[Druid sketch模块][sketch module]的一
部分。

我们现在并不支持源生Druid `JavaScript`及`HyperUnique Cardinality`后聚合器。
Fili支持sketches，不支持hyperUnique对象，因为二者功能重复。`JavaScript`后聚合器在大规模运行中会造成性能瓶颈。


自定义操作建造器
---------------------------------

这些建造器代表了一些并不能被单次Druid(后)数据聚合操作完成的操作。他们可能会创建新的列，细分Druid查询的规模，或者是在聚合
操作之上的进一步数学运算（比如平均数）。

目前，我们只支持如下自定义操作建造器：

1. [`聚合平均建造器 - AggregationAverageMaker`][AggregationAverageMaker]:
    `聚合平均建造器`可以在一个精度上聚合一个方面的数据，然后在更广维度上取所有聚合后数据的平均数。举个例子，我们想计算
    2012年每月的日平均页面访问量。计算方法是，我们先计算每月每日页面访问量，再把结果取平均值，就得到了当月的日平均页面访
    问量。`聚合平均建造器` 比较复杂，所以有[其自己的文档][aggregationAverageMaker-docs]详细说明。


<sub><a name="countCaveat">1</a>: 实际上`计数器建造器 - CountMaker`和Druid的计数聚合操作（count aggregation）并不完全相
同，因为实现Druid的`计数器建造器 - CountMaker`的时候，出了一个bug。`计数器建造器 - CountMaker`的内部实现是创建一个多重查
询。内层查询在每一个返回的结果列里插入一个值为1的常数列。外部查询则对所有返回结果（行）的长数列执行长整数之和 -
`longSum`。

[aggregationAverageMaker]: ../fili-core/src/main/java/com/yahoo/bard/webservice/data/config/metric/makers/AggregationAverageMaker.java
[aggregationAverageMaker-docs]: https://github.com/yahoo/fili/issues/10
[arithmeticMaker]: ../fili-core/src/main/java/com/yahoo/bard/webservice/data/config/metric/makers/ArithmeticMaker.java

[configuring metrics]: configuring-metrics.md
[constantMaker]: ../fili-core/src/main/java/com/yahoo/bard/webservice/data/config/metric/makers/ConstantMaker.java
[countMaker]: ../fili-core/src/main/java/com/yahoo/bard/webservice/data/config/metric/makers/CountMaker.java

[doubleMaxMaker]: ../fili-core/src/main/java/com/yahoo/bard/webservice/data/config/metric/makers/DoubleMaxMaker.java
[doubleMinMaker]: ../fili-core/src/main/java/com/yahoo/bard/webservice/data/config/metric/makers/DoubleMinMaker.java
[doubleSumMaker]: ../fili-core/src/main/java/com/yahoo/bard/webservice/data/config/metric/makers/DoubleSumMaker.java
[druid aggregations]: http://druid.io/docs/0.8.1/querying/aggregations.html
[druid post-aggregations]: http://druid.io/docs/0.8.1/querying/post-aggregations.html
[druid sketch module]: https://github.com/DataSketches/sketches-core

[longMaxMaker]: ../fili-core/src/main/java/com/yahoo/bard/webservice/data/config/metric/makers/LongMaxMaker.java
[longMinMaker]: ../fili-core/src/main/java/com/yahoo/bard/webservice/data/config/metric/makers/LongMinMaker.java
[longSumMaker]: ../fili-core/src/main/java/com/yahoo/bard/webservice/data/config/metric/makers/LongSumMaker.java

[sketchCountMaker]: ../fili-core/src/main/java/com/yahoo/bard/webservice/data/config/metric/makers/SketchCountMaker.java
[sketchSetOperationMaker]: ../fili-core/src/main/java/com/yahoo/bard/webservice/data/config/metric/makers/SketchSetOperationMaker.java
[sketch module]: https://github.com/druid-io/druid/pull/1991/files
