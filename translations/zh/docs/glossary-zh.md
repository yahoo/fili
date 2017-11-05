概念表
========

Fili 和相关概念汇总
This is collection of terms related to Fili and its concepts.

目录
-----------------

- [度量（Metrics）](#度量)
  - [聚合（Aggregation）](#聚合)
  - [后期聚合（Post Aggregation）](#后期聚合)
  - [建造器（Maker）](#建造器)
  - [映射器（Mapper）](#映射器)
  - [逻辑度量（Logical Metric）](#逻辑度量)
  - [Druid 模板查询语句](#Druid-模板查询语句)
  - [梗概（Sketch）](#梗概)
  - [度量列（Metric Column）](#度量列)
- [维度](#维度)
  - [维度](#维度)
  - [维度值（Dimension Row）](#维度值)
  - [星形模型（Star Schema）](#星形模型)
  - [快照缓存（Snapshot Cache）](#快照缓存)
  - [搜索提供程序（Search Provider）](#搜索提供程序)
  - [API 筛选器（Filter）](#API-筛选器)
  - [维度加载器(Dimension Loader)](#维度加载器)
- [列表（Tables）](#列表)
  - [逻辑列表（Logical Table）](#逻辑列表)
  - [物理列表（Physical Table）](#物理列表)
  - [切片列表（Slice）](#切片列表)
- [时间（Time）](#时间)
  - [时间精度（Time Grain）](#时间精度)
  - [精度（Granularity）](#精度)
  - [时间段（Interval）](#时间段)
  - [时期（Period）](#时期)
- [工作流（Workflow）](#工作流)
  - [工作流（Workflow）](#工作流)
  - [请求处理器（Request Handler）](#请求处理器)
  - [响应处理器（Response Processor）](#响应处理器)
  - [结果集（Result Set）](#结果集)
  - [结果（Result）](#结果)
- [应用相关](#应用相关)
  - [系统检查（Health Check）](#系统检查)
  - [功能标志（Feature Flag）](#功能标志)
  - [系统配置（System Config）](#系统配置)
  - [请求日志（Request Log）](#请求日志)
  - [数据库（Fact Store）](#数据库)
- [功能](#功能)
  - [Top N](#top-n)
  - [Limit](#limit)
  - [分页（Pagination）](#分页)
  - [部分数据（Partial Data）](#部分数据)
  - [不确定性数据（Volatile Data）](#不确定性数据)
  - [强度测试（Weight Check）](#强度测试)
- [其它（Miscellaneous）](#其它)
  - [Spock](#spock)
  - [Groovy](#groovy)
  - [服务连接器（Servlet）](#服务连接器)
  - [数据（Fact）](#数据)
  - [Web 服务](#web-服务)


度量
----

### 聚合

聚合（Aggregation）是度量概念的基础部分，定义数据存储用一种什么样的方式把数据库中的行（rows）对应的度量列（Metric Column）聚合
起来。聚合可以理解为流数据中的 “accumulators” 或者 MapReduce 中的 “reducers”。

### 后期聚合

后期聚合（Post Aggregation）是更高一级（2nd-level）的度量定义方式，通常是在聚合或者其他后期聚合上进行转换（transformation）或者
使用一些表达式（expressions）。后期聚合可以理解为表达（expression context）中的运算符（operators），用以构建一个表达树
（expression tree）。

### 建造器

建造器（又叫“度量建造器”）用于辅助配置，布置简化逻辑度量（Logical Metrics）的搭建。

### 映射器

映射器（又叫“结果集映射器”）是在数据从数据库调出以后，能够在逻辑度量（Logical Metrics）上进行进一步计算和操作的逻辑度量表达式组件。

### 逻辑度量

逻辑度量（Logical Metric）是用户层面（用 API 的方式）的度量定义，包含两个主要部分：元数据（metadata）和公式。元数据包括了诸如 API
名称，描述，和类别的信息。公式的意思是定义如何计算得出这个度量，囊括了 Druid 模板查询语句和结果集映射器。

### Druid 模板查询语句

Druid 模板查询语句是一个不完全的 Druid 查询语句，用来定义逻辑度量（Logical Metrics）。“不完全”的原因是该语句没有涵盖所有 Druid
语句里包含的值域。尤其是 Druid 模板查询语句里面没有数据源（Data Source）信息。

### 梗概

梗概（Sketch）是一个概率模型化的数集，通常用来计数。

### 度量列

度量列（Metric Column）在数据库（Fact Store）里面是一个存储度量值的列（这和维度值是不一样的）。度量列不能被分组（group by），也
不能被筛选（filtered）。


维度
----

### 维度

维度是数据或度量在某一个方面的不同数值。维度可以用来分组和筛选数据，可以理解成星形模型（Star Schema）中的查询表格，包含：

- 一个 API 名称，供 API 用户调用
- 一个 Druid 对应名称，对应了数据库中的物理表格列（Physical Table columns）
- 一组值域，其中一个值域的所有值必须唯一，叫做 Key field
- 一组维度值，对应每个值域的所有可能值

### 维度值

一组维度的所有可能值，每一个值属于一个值域。

### 星形模型

星形模型（Star Schema）是分析型数据仓库的一类常用的数据库结构，包含了一个大的中心数据表格，和一些维度列表，用作查找，这些列表通过
外键（foreign keys）连向中心数据表格。

### 快照缓存

即点对点缓存（point-in-time cache）。因为是点对点，不是连续更新的，所有缓存有可能会出现信息滞后，连续更新才会导致部分缓存滞后。
这种缓存方式比较简洁，虽然会出现信息滞后，但是系统运行会更快。

### 搜索提供程序

搜索提供程序（Search Provider）是键值维度（Key Value Dimension）的一个相关组件，用来存储维度值的检索（indexes）和在用筛选过滤
维度值的时候调用这些检索。

### API 筛选器

API 筛选器（API Filter）在 API 里面用来筛选维度值的，包括三个组成部分： 

- **选择器（Selector）**: 选择被筛选的维度和维度值域
- **运算符（Operator）**: 定义如何筛选到需要的维度值
- **值（Value List）**: 运算符筛选的所有值

### 维度加载器

维度加载器（Dimension Loader）将维度值加载到 Fili 实例中。


列表
----

### 逻辑列表

逻辑列表（Logical Table）是用户层面的，囊括了维度，逻辑度量，和时间精度，定义了那些维度，度量，和精度组合是可以用在一个查询语句
中的。

### 物理列表

物理列表（Physical Table）代表数据库里面接收查询的一个实际数据列表。在 Druid 里面，这通常叫做数据源（Data Source）。

### 切片列表

切片列表（Slice），又叫性能切片列表，实质上也是物理列表。区别是切片列表是物理列表里面把一些列的数据聚合之后得到的，这么做的目的是使
查询更快速。


时间
----

### 时间精度

时间精度（Time Grain）是一个时期，用来定义数据在时间维度上如何聚合。

### 精度

精度实际上就是可以包含 “all” 的时间精度，“all”的意思不把数据用时间分组。

### 时间段

时间段（Interval）是用两个边界时间定义的时间范围。

### 时期

时期（Period）是用公历系统表示的一种时间范围。时期通常用年，月，周，日，小时，分钟，秒表示。


工作流
------

### 工作流

工作流（Workflow）指的是数据服务连接器（Data Servlet）处理完请求之后发生的数据请求的处理流程，包括三个主要阶段：请求处理，
响应处理，数据集映射。请求处理阶段是静态的，由请求工作流提供程序定义，其他两个阶段在请求处理阶段动态生成。

### 请求处理器

请求处理器（Request Handler）是工作流中处理请求的组成部分。请求处理器可以中途读写 Druid 查询语句，而且还可以用到 API 请求的相关
信息数据。这使得处理器可以做一些模板 Druid 语句在逻辑度量上无法实现的事情，例如处理 Druid 语句进行度量优化或者中途改写查询语句
等等。

### 响应处理器

响应处理器（Response Processor）是工作流中处理响应的组成部分，主要处理从数据库调出来的 JSON 响应结果。多个处理器链中的最后一个
处理器亦会做很多步处理，未来可能会细分化成具体的步骤：

- 把 JSON 响应结果转化成结果集（Result Set）
- 加入任何结果集映射器
- 数据用不同格式表示出来

### 结果集

结果集（Result Set）是数据的集合，用列表列出，每一列是维度和度量，每一行是数据。

### 结果

一条结果是结果集里的一行数据，实质上是一个数组（tuple），每个元素对应了结果集里每一列上的值。结果可以理解成是包含一组度量和这些度量
在维度上不同值的体现。


应用相关
--------

### 系统检查

系统检查（Health Check）是一个用程序化方式检查 web 服务是否正常运行的机制（yes/no 两种结果）。

### 功能标志

功能标志（Feature Flag）是一个 boolean 配置机制，可以用来关启特定的系统功能，用 true 或者 false 就能指定是否启用。

### 系统配置

系统配置（System Config）是独立的配置抽象层架构，方便代码的各种配置环节，也简化了不同环境下的配置。

### 请求日志

请求日志（Request Log）是一个 Fili 里面一个可扩展的日志类别，在请求被处理，响应发回去之后就会产生一次这样的日志。日志在请求被边
处理边创建，包含了基本上各个环节的处理信息，例如一个环节耗时多久（单独与总和）。

### 数据库

数据库（Fact Store）在这里是一个抽象概念，代表所有可以聚合的数据行来源，具体的例子包括 Druid，Hive，或者关系型数据库（RDBMS）。


功能
----

### Top N

Top N 是 Fili 所有时间单位数据返回数量的一个限制条件。Top N 和 Limit 的区别是，前者是以某个时间段内进行限制，后者是在所有数据里
限制

### Limit

Limit 是 Fili 里访问某类资源返回的数据的行数限制。Limit 和 Top N 的区别是，前者是在所有数据里限制，后者是以某个时间段内进行限制。

### 分页

Fili 支持数据分页，用户可以只访问大数据下的一部分数据（某页）。

### 部分数据

部分数据（Partial Data）的意思是聚合时间段内没能包含请求需要的所有数据。部分数据出现的可能情况可以是有一个请求要得到
每个月的聚合数据，但是某个整月的聚合数据在数据库不存在（可能是还没到月底）。这种情况下，响应会回复整月数据里，数据库没有
这个月完整的数据去聚合，所以会返回这个月里部分数据，但是时间跨度还是这个月的。

Fili 可以显示有数据的有效时间段，防止用户在不知情的情况下得到不完全数据。

### 不确定性数据

不确定性数据（Volatile Data）和"部分数据类似"，只不过不是因为数据不完全出来的一个不完全聚合，而是聚合进去的数据还在
变化，所以叫做"不确定"。不确定性数据可能发生在 Druid 还在往实时数据节点（Realtime nodes）存放数据的过程中，聚合了那个
时间段的数据。

Fili 能够探测并报告不确定数据，您只需要提供一个不确定数据提供程序（Volatility Provider），定义物理列表里那些时间段的数据
是不确定的。

### 强度测试

Fili 的强度测试（Weight Check）功能可以测算一条查询[梗概](#梗概)的语句在 Druid 中间节点（Druid Broker）需要耗费多少内存。


其它
----

### Spock

Spock 是一个基于 Groovy 语言的，[行为驱动开发](https://zh.wikipedia.org/wiki/%E8%A1%8C%E4%B8%BA%E9%A9%B1%E5%8A%A8%E5%BC%80%E5%8F%91)
样式的测试框架

### Groovy

Groovy 是一门基于动态JVM的程序设计语言。Groovy 动态和灵活的机制使之成为一门测试专用语言。

### 服务连接器

服务连接器（Servlet）是一个 Java 编写的服务器端程序，用以接收 HTTP 请求。Fili 也有这样的服务连接器，和 Java 原生的
服务器端程序很像，但是更接近于 MVC web 框架里的 Controller，例如 Ruby on Rail 和 Grails 里面。

### 数据

数据（Fact），也叫度量（Metric），是一条条计量数据，具体的值用一列维度值展现。

### Web 服务

Web 服务是一个"客户端-服务器"机制下的，部署在网络服务器里的一个软件系统，用作客户端和数据库之间的一个中间件或接口。广义
的 Web 服务在 [W3C](https://www.w3.org/TR/2004/NOTE-ws-gloss-20040211/#webservice) 里被定义为

> a software system designed to support interoperable machine-to-machine interaction over a network. 

详见 [web 服务](http://www.baidu.com/s?ie=utf-8&f=8&rsv_bp=0&rsv_idx=1&tn=baidu&wd=web%20service&rsv_pq=f314028900009caf&rsv_t=ff7eN06c68%2Fyb2bHVDgNkXD4AaMUpgm0qTxOfAFO%2FDLdoxDvsfPyUB02b7c&rqlang=cn&rsv_enter=1&rsv_sug3=11&rsv_sug2=0&inputT=1369&rsv_sug4=1369).
