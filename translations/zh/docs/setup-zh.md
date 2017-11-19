Setup
=====

本文将引导您如何在 Druid 集群上启动 Fili 

[若操作过程遇到问题请参见 Troubleshooting-zh.md](troubleshooting-zh.md)

目录
----

- [基础设施（Prerequisites）](#基础设施)
- [总体步骤](#总体步骤)
- [Fili Wikipedia Example](#fili-wikipedia-example)
- [配置元数据（Metadata）](#配置元数据)
- [配置文件](#配置文件)
- [编译并部署 WAR](#编译并部署WAR)
- [维度加载](#维度加载)

基础设施
--------

- [Jetty][jetty]。

- 一个正在运行的 [Druid][druid] 集群作为 Fili 的数据后台。

- （可选：维度缓存）最新的已经加载好的维度的信息。详见 [维度加载](#维度加载)。

- （可选：维度缓存）[MDBM][mdbm] (或者 [Redis][redis]) 。当维度值的数量太大，放不进内存的时候，用来存储维度数据。


总体步骤
--------

以下列出了启动 Fili 实例的总体步骤。

- [将 fili 的 wikipedia 的样本软件复制到指定目录下。](#fili-wikipedia-example)

- [根据需求配置维度，度量，物理列表和逻辑列表。](#配置元数据)

- [修改配置文件。](#配置文件)

- [编译并部署WAR。](#编译并部署WAR)

- [设置维度加载器。](#维度加载)

Fili Wikipedia Example
----------------------

[Fili wikipedia 样本软件][fili-wikipedia-example] 是一个使用 Fili 语料库的一个例子。您可以在这个应用里配置您的度量
（metrics），维度（dimensions），和列表（tables）。

配置元数据
----------

配置 Fili 的元数据是整个过程的主要部分：

- [度量（Metrics）][configuringMetricsDocumentation]

- [维度（Dimensions）][configuringDimensionsDocumentation]

- [列表（Tables）][loadTablesDocumentation]

- [将配置代码（和其它资源）捆绑到 Fili][binderDocumentation]

配置文件
--------

下一步，需要加入一些配置文件和脚本：

* 在 [applicationConfig.properties][applicationConfig] 中，以下属性需要配置：
    - `bard__resource_binder = binder.factory.class.path`
    - `bard__dimension_backend = mdbm` （如果您想用 Redis 存储维度元数据，请使用 `redis`，如果想用内存存储
    （in-memory map），请使用 `memory`）
        - （可选：MDBM） `bard__mdbm_location = dir/to/mdbm` - 该路径必须包含一个叫做 `dimensionCache` 的文件夹。
    - `bard__non_ui_druid_broker = http://url/to/druid/broker`（Druid broker 节点地址）
    - `bard__ui_druid_broker = http://url/to/druid/broker`（Druid broker 节点地址）
    - `bard__druid_coord = http://url/to/druid/coordinator`（Druid coordinator 节点地址）
    
* [pom.xml][pomXml] - 找到 `fili.version` 标签，把 snapshot 版本改成某个 Fili 的版本。

我们将 `bard__non_ui_broker` 和 `bard__ui_broker` 设成了相同的 broker 地址，这两个变量是 Fili 的两个相关应用用到的。日后
这些设置会被改成任意应用都能用得上的资源。目前，您可以将它们等同对待。

编译并部署WAR
-------------

应用配置好了以后，就可以编译并部署 WAR 文件了。编译出 war 可以用 `mvn install`，之后您就会在 `target` 目录下看到 WAR
文件了，该 WAR 文件需要被放入 Jetty 实例里的 webapp 文件夹下。

维度加载
--------

Fili 的维度分两类：加载类（loaded）和非加载类（non-loaded）。加载类的值（和相关元数据）会被加载进 Fili。非加载类的虽然
被配置了，但是值和元数据还没有被加载进 Fili。Fili 只能在加载类维度上进行维度元数据筛选和维度合并，不过非加载类维度确有其
用途，您可以用非加载类的维度去访问查询 Druid。但是如果您想在任何维度上进行筛选和合并，您须要让您的所有维度都属于加载类
的。

要加载一个维度，您需要给 Fili 发送两个 POST 请求到 `/v1/cache/dimensions/<myDimension>/dimensionRows`，为的是加载
维度值组（dimension rows）。第一个请求设置更新维度值，第二个请求发送维度成功加载的日期时间，第二个请求的目的很简单，只是
用来标识某一维度成功加载了。

我们先来看第一个请求。加载每一个维度的请求内容（payload）是一个 object。这个 object 是一个包含多个 `dimensionRows` 的
列表。每个 `dimensionRow` 里面的数据合起来代表这个维度的一个值：

```json
{ 
    "dimensionRows": [ 
        { 
            "dataField1": data, 
            "dataField2": data
        }, {
            "dataField1": data, 
            "dataField2": data
        },
        ...
    ]
}
```

一个[符合要求的维度][configuringDimensionsDocumentation]要满足两点：必须有一个值名称作为每一列维度值得 key
（类似于`id`），还必须有一个叫做 `lastUpdated` 的总体值名称，用来记录这个维度最新一次的更新时间。

第二个请求发送一个简单的 JSON object：

```json
{
    "lastUpdated": "Roughly current date in ISO 8601 format"
}
```

值的意思代表用 ISO 8601 格式表示的当前时间。
 
举个例子，我们有一个维度叫做 `gender`（性别），包含三个值：`male`（男性），`female`（女性），和 `unknown`（未提供）。
元数据包括了一个值名称 `id` 和一个值名称 `description`（描述）。我们以此可以向
`/v1/cache/dimensions/gender/dimensionRows` 发送如下内容：

```json
{
    "dimensionRows": [
        {
            "id": "male",
            "description": "The visitor was of the male persuasion."
        }, {
            "id": "female",
            "description": "The visitor was of the female persuasion."
        }, {
            "id": "unknown",
            "description": "We don't know the gender of the visitor. Oh woe is us."
        } 
    ]
}
```

然后发送：

```json
{
    "lastUpdated": "2015-12-16T10:25:00"
}
```

加载维度的常用手段是在后台开设一个程序，每隔一段时间去抓取最新的维度信息，放进 Fili。

### 非加载类维度 ###

有些情况下您可能不需要对某些维度进行维度合并（dimension joins）或维度元数据筛选。您可以将这些维度设成非加载类维度。
非加载类维度配置方法如下：

1. 将维度设置成使用 [NoOpSearchProvider][noOpSearchProvider]。配置维度 `SearchProvider` 的方法，请详见
[Configuring Dimensions][configuringDimensionsDocumentation]。

2. 向 `/v1/cache/dimensions/dimensionName` 发送一条 JSON，包含维度 `id`，和一个 `lastUpdated` ，格式遵循
[ISO 8601][iso8601] 标准。

举例，如果我们想将 `gender`（性别）设成非加载类的，除了将其使用 `NoOpSearchProvider`，启动了 Jetty 之后，我们需要往
`/v1/cache/dimensions/gender` 发送：

```json
    {
        "name": "gender",
        "lastUpdated": "2015-12-16T00:00:00"
    }
```

将所有的维度设成非加载类的可以简化设置。所以，如果您想快速搭建好一个 Fili 实例，您可以选择将所有维度设成非加载类的。在您
打算使用 Fili 之后，您再加载维度也不迟。


[applicationConfig]: ../fili-wikipedia-example/src/main/resources/applicationConfig.properties

[fili-wikipedia-example]: ../fili-wikipedia-example
[binderDocumentation]: https://github.com/yahoo/fili/issues/11

[configuringDimensionsDocumentation]: https://github.com/yahoo/fili/issues/12
[configuringMetricsDocumentation]: configuring-metrics.md 

[druid]: http://druid.io

[iso8601]: https://baike.baidu.com/item/ISO%208601/3910715?fr=aladdin

[jetty]: http://www.eclipse.org/jetty/

[loadTablesDocumentation]: https://github.com/yahoo/fili/issues/13

[mdbm]: http://yahoo.github.io/mdbm/

[noOpSearchProvider]: ../fili-core/src/main/java/com/yahoo/bard/webservice/data/dimension/impl/NoOpSearchProvider.java

[pomXml]: ../fili-core/pom.xml

[redis]: http://redis.io/
