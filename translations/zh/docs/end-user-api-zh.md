Fili API 指南
==================

Fili 以数据查询为核心，提供快速简洁的 API。

API使用 HTTPS `GET` 查询模式, 您的查询语句就是一个简单的URL，便于记录和共享。

目录
-----------------

- [核心概念](#核心概念)
    - [维度](#维度)
    - [度量](#度量)
    - [列表](#列表)
    - [数据筛选](#数据筛选)
    - [时间段](#时间段)
    - [精度](#精度)
- [数据查询](#数据查询)
    - [基本介绍](#基本介绍)
    - [维度划分](#维度划分)
    - [筛选举例](#筛选举例)
    - [数据格式举例](#数据格式举例)
- [查询选项](#查询选项)
    - [分页](#分页)
    - [响应格式](#格式)
    - [筛选](#筛选)
    - [Having](#having)
    - [排序](#排序)
    - [TopN](#topn)
- [异步查询](#异步查询)
    - [任务接口](#任务接口)
- [其它](#其它)
    - [日期和时间](#日期和时间)
      - [日期区间](#日期区间)
      - [Date Macros](#date-macros)
      - [时区](#时区)
    - [大小写区分](#大小写区分)
    - [速率限定](#速率限定)
    - [报错](#报错)

核心概念
-------------

Fili 包含五个主要概念：

- [维度](#维度)
- [度量](#度量)
- [列表](#列表)
- [数据筛选](#数据筛选)
- [精度](#精度)
- [时间段](#时间段)

### 维度 ###

数据维度定义您可以如何去划分数据（slice and dice）。维度可以用来数据分组，聚合，筛选数据，有整个系统极其重要的一项功能。
每个维度包含一些值域（fields），每个值域有一系列的值。这些值域和值主要用于数据筛选和标注（Annotating）查询结果。

所有维度包含一个 ID 值域（用作数据库的 key）和关于这个维度的描述（便于人们阅读理解，也是一个值域），这两项可以用来筛选
查询结果，每条结果用行（rows）表示，查询结果本身亦会包含这两项。

查询显示[所有维度](https://sampleapp.fili.org/v1/dimensions):

    GET https://sampleapp.fili.org/v1/dimensions

显示[某一维度](https://sampleapp.fili.org/v1/dimensions/productRegion):

    GET https://sampleapp.fili.org/v1/dimensions/productRegion

显示[某一维度的所有可能值](https://sampleapp.fili.org/v1/dimensions/productRegion/values):

    GET https://sampleapp.fili.org/v1/dimensions/productRegion/values

除此之外，查询维度的值还包含一些访问可选项：

- [分页](#分页)
- [格式](#格式)
- [筛选](#筛选) (支持各种筛选方式)

例如，得到[维度描述包含"U"的用户国家，第二页，每页五项结果，用JSON表示](https://sampleapp.fili.org/v1/dimensions/userCountry/values?filters=userCountry|desc-contains[U]&page=2&perPage=5&format=json):

    GET https://sampleapp.fili.org/v1/dimensions/userCountry/values?filters=userCountry|desc-contains[U]&page=2&perPage=5&format=json

### 度量 ###

度量（Metrics）是数据点，例如网页访问量，日均使用时间等等。度量是基于某个数据列表和时间精度的（time grain），要知道有
哪些配置好的度量，您可以访问这个数据列表或者直接访问度量资源端点获取所有系统支持的度量

显示[所有度量](https://sampleapp.fili.org/v1/metrics):

    GET https://sampleapp.fili.org/v1/metrics

访问[某一度量](https://sampleapp.fili.org/v1/metrics/timeSpent):

    GET https://sampleapp.fili.org/v1/metrics/timeSpent

### 列表 ###

列表（Tables）可以显示在某组[度量](#度量)，[维度](#维度)，[精度](#精度)结合情况下的数据。每个列表在某个精度上，都存在
一组可访问的度量和维度。

显示[所有列表](https://sampleapp.fili.org/v1/tables):

    GET https://sampleapp.fili.org/v1/tables

显示[某个列表](https://sampleapp.fili.org/v1/tables/network/week):

    GET https://sampleapp.fili.org/v1/tables/network/week

### 筛选 ###

筛选（Filters）功能可以过滤数据。不同的资源对应不同的筛选：筛选功能可以过滤某一行或多行（rows）数据库返回的数据，也可以
筛选系统内部处理的数据。

如果访问请求不是针对数据资源（non-Data resource）的，由于没有数据聚合（data aggregation），筛选功能在这里主要是增删
结果行（row）。

但如果访问请求是[针对数据资源](#数据查询)的话，就会过滤聚合后的数据。如果要像访问非数据资源那样过滤一行行结果，访问请求
就必须在维度筛以外，在访问路径后头附上一个[维度划分路径](#维度划分)。

### 时间段 ###

查询中用到的时间段（Interval，或者 `dateTime`）指的是访问数据所处的时间范围。时间段用一个起始时间和终止时间表示，表示
方法采用 ISO 8601 格式，即包含起始时间，但不包括终止时间。您可以很快适应这种时间段的表述方式。

特别注意，时间段必须和查询语句中的[精度](#精度)相吻合。例如，如果精度是"月份"，时间段必须以某月份边界作为起止，如果是
以星期为单位，则必须以星期一作为起止（我们约定星期一作为星期边界）。

### 精度 ###

精度是每行返回数据的数据时间跨度（granularity 或者 "bucket size"），换言之，精度是数据聚合的时间范围。精度在和计数相关的
度量里用得十分普遍，例如计算 ID 的个数。

目前支持的精度有秒，分钟，小时，天，星期，月，季度，年。还有一个"全部（all）"精度，把所有数据聚合到一个单元里。

数据查询
------------

数据查询是 Fili API 的核心功能，数据资源可以：

- 根据[维度](#维度)分组
- 根据[精度](#精度)分组
- 根据维度值[筛选数据](#筛选)
- 用[Having](#having)筛选功能进行度量筛选
- Selecting [metrics](#metrics)
- 返回某个[时间段](#时间段)的数据
- 以某个时间精度进行[排序](#排序)
- 选择数据返回的[格式](#格式)

### 基本介绍 ###

我们先举例讲解一下查询 URL 的格式：

    GET https://sampleapp.fili.io/v1/data/network/week?metrics=pageViews,dayAvgTimeSpent&dateTime=2014-09-01/2014-09-08

这个[基本的查询语句](https://sampleapp.fili.io/v1/data/network/week?metrics=pageViews,dayAvgTimeSpent&dateTime=2014-09-01/2014-09-08)，
返回的是一个星期以内，网页访问数和日均在线时间的数据。我们来具体讲解一下其组成。

- **https\://** - Fili API 用安全连接传输数据, _必须使用 HTTPS_。_HTTP 访问无效_。
- **sampleapp.fili.org** - Fili API 的访问地址。
- **v1** - API 版本。
- **data** - 访问的资源类别，也是所有数据查询必须的起始项。
- **network** - Network 是提供数据的[列表](#列表)。
- **week** - 返回数据的[精度](#精度)。每一条结果结果聚合了一个星期的数据。
- **metrics** - 我们需要的[度量](#度量)，多个度量用逗号隔开（注意：拼写是区分大小写的）。
- **dateTime** - 所有数据所处的[时间段](#时间段)，用[ISO 8601 格式](#日期时间)表示。

### 维度划分 ###
了解了基本的查询语句后，您也许想查询某个维度上的数据，例如[Product Region 维度](https://sampleapp.fili.io/v1/data/network/week/productRegion?metrics=pageViews,dayAvgTimeSpent&dateTime=2014-09-01/2014-09-08)?

    GET https://sampleapp.fili.io/v1/data/network/week/productRegion?metrics=pageViews,dayAvgTimeSpent&dateTime=2014-09-01/2014-09-08

我们在原有的 URL 上添加了一段路径(`productRegion`)。任意数量的维度都可以在[精度](#精度)之后作为额外路径添加。

照此原理，如果我们还想在此基础上[添加 `gender`](https://sampleapp.fili.io/v1/data/network/week/productRegion/gender?metrics=pageViews,dayAvgTimeSpent&dateTime=2014-09-01/2014-09-08)
（两个维度的划分），我们可以用：

    GET https://sampleapp.fili.io/v1/data/network/week/productRegion/gender?metrics=pageViews,dayAvgTimeSpent&dateTime=2014-09-01/2014-09-08

### 筛选举例 ###

除了维度划分，我们还可以在此基础上加上其他筛选数据。比如说我们想得到[美国以外的全球数据](https://sampleapp.fili.io/v1/data/network/week?metrics=pageViews,dayAvgTimeSpent&dateTime=2014-09-01/2014-09-08&filters=productRegion|id-notin[Americas+Region])?

    GET https://sampleapp.fili.io/v1/data/network/week?metrics=pageViews,dayAvgTimeSpent&dateTime=2014-09-01/2014-09-08&filters=productRegion|id-notin[Americas Region]

我们在查询语句里添加了一个[筛选项](#筛选)，过滤掉(`notin`)`productRegion` 维度里 ID 是`Americas Region` 的数据。我们还
去掉了划分维度 `productRegion`，因为我们需要全球的整体数据，加了分组维度只会按地区显示某个地区的数据。

这个例子展示了如何在不指定划分维度的情况下，筛选这个维度里的数据。筛选功能完善且强大，更多功能请参阅[筛选](#筛选)。最后提及一点，
对于数据资源（Data resource），筛选器支持 `in`，`notin`，`eq`，`startswith`，`contains` 筛选，startswith`，`contains` 可以
选择性关闭。

### 数据格式举例 ###

最后，我们想对结果使用
[CSV 表示](https://sampleapp.fili.io/v1/data/network/week?metrics=pageViews,dayAvgTimeSpent&dateTime=2014-09-01/2014-09-08&filters=productRegion|id-notin[Americas Region]&format=csv)，
把结果可以放进 Excel 做下一步处理。Fili API 完全支持 CSV！

    GET https://sampleapp.fili.io/v1/data/network/week?metrics=pageViews,dayAvgTimeSpent&dateTime=2014-09-01/2014-09-08&filters=productRegion|id-notin[Americas Region]&format=csv

Fili 支持其他更多的数据格式，详见[格式](#格式)单元！

查询选项
-------------

Fili API 可供访问的资源支持很多查询选项。以下是这些选项和它们的用法:

- [分页](#分页)
- [数据格式](#格式)
- [筛选](#筛选)
- [Having](#having)
- [维度值域筛选](#维度值域筛选)
- [排序](#排序)

### 分页 ###

分页把数据结果分成多个页面，根据需求每次返回一个页面的数据。您不用一次性从大量返回结果中只抽取少量数据，可以用分页把结果分成很多页，
每一页包含一小部分数据，抽取某一页数据去处理就行。

目前，支持分页的有[维度](#维度)和[数据](#数据查询)访问接口（endpoints）。

每一页除了实际数据，还包含分页参数。维度和数据接口的分页参数目前有所不同，不过我们正在修改维度数据接口，使之和数据接口显示相同的
参数。

如果需要将支持分页的资源进行分页显示，有两个查询参数可供您使用：

- **perPage**：页面显示多少条数据结果/资源。参数值是正整数。

- **page**: 显示第几页（页面显示多少条结果由 `perPage` 指定）。参数值是正整数。

举例，利用这两个参数，我们可以得到[包含三条数据的第二页结果](https://sampleapp.fili.io/v1/data/network/day?metrics=pageViews&dateTime=2014-09-01/2014-09-08&perPage=3&page=2):

    GET https://sampleapp.fili.io/v1/data/network/day?metrics=pageViews&dateTime=2014-09-01/2014-09-08&perPage=3&page=2

所有格式的数据结果里，包含了一些链接。这些链接连到所有数据的第一页，最后一页，下一页，和上一页。四个页面用 `rel` 值区分开来，分别是
`first`，`last`，`next`，`prev`。[上一个例子中](https://sampleapp.fili.io/v1/data/network/day?metrics=pageViews&dateTime=2014-09-01/2014-09-08&perPage=3&page=2),
的链接为：

     Link:
        https://sampleapp.fili.io/v1/data/network/day?metrics=pageViews&dateTime=2014-09-01/2014-09-08&perPage=3&page=1; rel="first",
        https://sampleapp.fili.io/v1/data/network/day?metrics=pageViews&dateTime=2014-09-01/2014-09-08&perPage=3&page=3; rel="last"
        https://sampleapp.fili.io/v1/data/network/day?metrics=pageViews&dateTime=2014-09-01/2014-09-08&perPage=3&page=3; rel="next",
        https://sampleapp.fili.io/v1/data/network/day?metrics=pageViews&dateTime=2014-09-01/2014-09-08&perPage=3&page=1; rel="prev",

维度和数据 API 接口却有不同之处：

#### 数据 #####

JSON (and JSON-API) 数据格式里，返回的结果（response body）包含了一个 `meta` object：

```json
"meta": {
    "pagination": {
        "currentPage": 2,
        "rowsPerPage": 3,
        "numberOfResults": 7
        "first": "https://sampleapp.fili.io/v1/data/network/day?metrics=pageViews&dateTime=2014-09-01/2014-09-08&perPage=3&page=1",
        "previous": "https://sampleapp.fili.io/v1/data/network/day?metrics=pageViews&dateTime=2014-09-01/2014-09-08&perPage=3&page=1",
        "next": "https://sampleapp.fili.io/v1/data/network/day?metrics=pageViews&dateTime=2014-09-01/2014-09-08&perPage=3&page=3",
        "last": "https://sampleapp.fili.io/v1/data/network/day?metrics=pageViews&dateTime=2014-09-01/2014-09-08&perPage=3&page=3"
    }
}
```

`meta` object 包含一个 `pagination` object，里面放了 `first`，`last`，`next`，`previous` 的链接。`meta` object 还包括了其它
信息：

- `currentPage`： 第几页
- `rowsPerPage` ：页面显示了多少条结果
- `numberOfResults`：所有页面包含的结果条数

_注意：要访问数据接口，必须同时提供 `perPage` 和 `page` 参数。数据接口不提供默认分页参数。_

##### 分页链接 #####

分页情况下， `first` 和 `last` 都会显示出来，但是 `next` 和 `previous` 只有在当前页之前或之后多出一页的情况下才会出现在返回结果
中。换句话说，[第一页](https://sampleapp.fili.io/v1/data/network/day?metrics=pageViews&dateTime=2014-09-01/2014-09-08&perPage=3&page=1)
没有 `previous` 的链接，[最后一页](https://sampleapp.fili.io/v1/data/network/day?metrics=pageViews&dateTime=2014-09-01/2014-09-08&perPage=3&page=3)
没有 `next` 的链接。

#### 维度 ####

维度接口同样包含了`first`，`last`，`next`，`prev` 链接。

维度接口和数据接口不同之处在于，维度接口 _永远会_ 分页。默认显示第一页，每页 10000 条数据。每页的数据条数可以修改，用配置变量
`default_per_page` 调节。

_注意，`default_per_page` **只适用于**维度接口，对数据接口不起作用。_

- **perPage**:
    如果只提供 `perPage` 参数，访问会失败

    [Example](https://sampleapp.fili.io/v1/dimensions/productRegion/values?perPage=2): `GET https://sampleapp.fili.io/v1/dimensions/productRegion/values?perPage=2`

    _注意：这个请求会得到一个报错结果，因为分页要求同时提供`perPage` 和 `page`_

- **page**:
    `page` 的默认值是 1，也就是第一页。

    注意: `page` 和 `perPage` 必须同时设定。

    [Example](https://sampleapp.fili.io/v1/dimensions/productRegion/values?perPage=2&page=2): `GET https://sampleapp.fili.io/v1/dimensions/productRegion/values?perPage=2&page=2`

### 响应格式 ###

部分访问资源支持多种数据格式。默认的格式是 JSON，某些资源也支持 CSV 和 JSON-API 格式。

如果需要改变数据格式，请在查询语句中加入 `format` 变量。

[JSON](https://sampleapp.fili.io/v1/data/network/day/gender?metrics=pageViews&dateTime=2014-09-01/2014-09-02&format=json): `GET https://sampleapp.fili.io/v1/data/network/day/gender?metrics=pageViews&dateTime=2014-09-01/2014-09-02&format=json`

```json
{
    "rows": [
        {
            "dateTime": "2014-09-01 00:00:00.000",
            "gender|id": "-1",
            "gender|desc": "Unknown",
            "pageViews": 1681441753
        },{
            "dateTime": "2014-09-01 00:00:00.000",
            "gender|id": "f",
            "gender|desc": "Female",
            "pageViews": 958894425
        },{
            "dateTime": "2014-09-01 00:00:00.000",
            "gender|id": "m",
            "gender|desc": "Male",
            "pageViews": 1304365910
        }
    ]
}
```

[CSV](https://sampleapp.fili.io/v1/data/network/day/gender?metrics=pageViews&dateTime=2014-09-01/2014-09-02&format=csv): `GET https://sampleapp.fili.io/v1/data/network/day/gender?metrics=pageViews&dateTime=2014-09-01/2014-09-02&format=csv`

```csv
dateTime,gender|id,gender|desc,pageViews
2014-09-01 00:00:00.000,-1,Unknown,1681441753
2014-09-01 00:00:00.000,f,Female,958894425
2014-09-01 00:00:00.000,m,Male,1304365910
```

[JSON-API](https://sampleapp.fili.io/v1/data/network/day/gender?metrics=pageViews&dateTime=2014-09-01/2014-09-02&format=jsonapi): `GET https://sampleapp.fili.io/v1/data/network/day/gender?metrics=pageViews&dateTime=2014-09-01/2014-09-02&format=jsonapi`

```json
{
    "rows": [
        {
            "dateTime": "2014-09-01 00:00:00.000",
            "gender": "-1",
            "pageViews": 1681441753
        },{
            "dateTime": "2014-09-01 00:00:00.000",
            "gender": "f",
            "pageViews": 958894425
        },{
            "dateTime": "2014-09-01 00:00:00.000",
            "gender": "m",
            "pageViews": 1304365910
        }
    ],
    "gender": [
        {
            "id": "-1",
            "desc": "Unknown"
        },{
            "id": "f",
            "desc": "Female"
        },{
            "id": "m",
            "desc": "Male"
        }
    ]
}
```

### 筛选 ###

您可以根据[维度](#维度)值去筛选数据。不同的资源有不同的筛选结果，但是筛选的基本用法和原理都是一样的。

单个筛选的一般格式是：

    dimensionName|dimensionField-filterOperation[some,list,of,url,encoded,filter,strings]

多个筛选项用逗号隔开，筛选采用 [URL 编码](http://en.wikipedia.org/wiki/Percent-encoding)，筛选值用逗号隔开：

    myDim|id-contains[foo,bar],myDim|id-notin[baz],yourDim|desc-startsWith[Once%20upon%20a%20time,in%20a%20galaxy]

以下是可供使用的筛选项（某些项只能用于部分接口）:

- **in**: `In` 筛选是一个完全匹配项，只有在筛选项里列出的值列才会被选中
- **notin**: `Not In` 筛选也是一个完全匹配项，只有_没有_在筛选项里列出的值列才会被选中
- **contains**: `Contains` 筛选出值包含在指令内容中的数据，类似于 `in` 筛选
- **startsWith**: `Starts With` 筛选出值以指定内容起始的数据，类似于 `in` 筛选

举个例子解释一下。

[比如](https://sampleapp.fili.io/v1/dimensions/productRegion/values?filters=productRegion|id-notin[Americas%20Region,Europe%20Region],productRegion|desc-contains[Region]):

    GET https://sampleapp.fili.io/v1/dimensions/productRegion/values?filters=productRegion|id-notin[Americas%20Region,Europe%20Region],productRegion|desc-contains[Region]

这个筛选的含义是：

    返回满足以下要求的维度值：
        不包含
            ID是 "Americas Region" 或 "Europe Region" 的 productRegion 维度值
        包含
            描述语句包含 "Region" 单词的 productRegion 维度值


### Having ###

Having 语句可以根据聚合数据条件来筛选返回数据。这和根据维度值过滤的[筛选](#筛选)不尽相同，因此，描述 having 语句的语法与
之类似。

单个 having 语句的一般格式为：

    metricName-operator[x,y,z]

这里的三个参数 `x, y, z` 可以是整数，小数（`3, 3.14159`）或者用科学计数法表示的数字（`4e8`）。实际情况下参数列可以包含任意
数量的参数，但参数列不能为空。

多个 Having 语句用逗号隔开：

    metricName1-greaterThan[w,x],metricName2-equals[y],metricName3-notLessThan[y, z]

该语句的含义是_返回所有 metricName1 的值大于 w 或者 x，metricName2 的值等于 y，metricName3 的值大于 y 和 z 的所有数据列_。

注意，你只能将 having 语句用于出现在 `metrics` 语句中的数据。

以下是所有 having 的运算符。每一个运算符有一个简写形式，简写形式在每一个运算符完整名称后的括号内列出，你可以在查询语句
中选择全名或者简写。

- **equal(eq)**: `Equal` 返回对应的数据值至少等于某一个指定值的数据行。
- **greaterThan(gt)**: `Greater Than` 返回对应的数据值至少大于某一个指定值的数据行。
- **lessThan(lt)**: `Less Than` 返回对应的数据值至少小于某一个指定值的数据行。

每个运算符可以在前面加上一个 `not`，执行相反的操作。所以 `noteq` 返回对应的数据值不等于任何一个指定值的数据行。

举个[例子](https://sampleapp.fili.io/v1/data/network/day?metrics=pageViews,users&dateTime=2014-09-01/2014-09-08&having=pageViews-notgt[4e9],users-lt[1e8])来具体解释。

    GET https://sampleapp.fili.io/v1/data/network/day?metrics=pageViews,users&dateTime=2014-09-01/2014-09-08&having=pageViews-notgt[4e9],users-lt[1e8]

此 having 语句的意思是：

    返回 2014 年九月一号到八号，每天满足以下条件的页面访问量和访问者数量
        最多四亿次页面访问
        大于1亿访问者

#### 附加说明 ####

having 筛选发生在 Druid 数据库，所以，如果 Fili 在 Druid 数据上对被筛选的度量进行任何后续计算，数据可能出现偏差。

### 维度值域筛选 ###

查询的返回结果默认包含关于访问维度的 ID 和描述。但是您可能需要获取关于这些维度更多的信息，或者您不许要这么多信息，那么您可以
[在维度路径后使用一个 `show` 语句](https://sampleapp.fili.io/v1/data/network/week/productRegion;show=desc/userCountry;show=id,regionId/?metrics=pageViews&dateTime=2014-09-01/2014-09-08):

    GET https://sampleapp.fili.io/v1/data/network/week/productRegion;show=desc/userCountry;show=id,regionId/?metrics=pageViews&dateTime=2014-09-01/2014-09-08

这条查询的结果中只会显示产品区域维度（Product Region dimension）中的描述值域（description field）和用户国家维度
（User Country dimension）中的 ID 和 区域 ID值域。使用 `show` 的一般方法是，在维度后面加一个分号，在分号后面加入 `show` 语句 -
`show=<fieldnames>`。用逗号分隔一个 show 语句中的多个值域：

    /<dimension>;show=<field>,<field>,<field>

#### 维度值域筛选关键词 ####

用 `show` 选择值域的时候可以使用一些关键词：

- **All**：列出某个维度的所有值域
- **None**：只列出 key 值域。

`none` 关键词还能将尽量缩小返回的数据量大小，简化返回的数据。简化方式去取决于返回的数据格式：

##### JSON #####
非简化的返回数据里，每一个值域的格式是 `"dimensionName|fieldName":"fieldValue"`，而简化后的只有一项值域，该值域的值是 key 值域
的值：`"dimensionName":"keyFieldValue"`。例如，非简化的返回数据如下

```json
{
    "rows": [
        {
            "dateTime": "2014-09-01 00:00:00.000",
            "gender|id": "-1",
            "gender|desc": "Unknown",
            "pageViews": 1681441753
        },{
            "dateTime": "2014-09-01 00:00:00.000",
            "gender|id": "f",
            "gender|desc": "Female",
            "pageViews": 958894425
        },{
            "dateTime": "2014-09-01 00:00:00.000",
            "gender|id": "m",
            "gender|desc": "Male",
            "pageViews": 1304365910
        }
    ]
}
```

而带了 `show=none`，简化后的结果如下

```json
{
    "rows": [
        {
            "dateTime": "2014-09-01 00:00:00.000",
            "gender": "-1",
            "pageViews": 1681441753
        },{
            "dateTime": "2014-09-01 00:00:00.000",
            "gender": "f",
            "pageViews": 958894425
        },{
            "dateTime": "2014-09-01 00:00:00.000",
            "gender": "m",
            "pageViews": 1304365910
        }
    ]
}
```

我们可以看到，`gender|desc` 消失了， `gender|id` 则变成了 `gender`。

##### CSV #####
非简化的返回数据 **header** 里，每一个值域的格式是 `"dimensionName|fieldName":"fieldValue"`，而简化后的只有一项值域，该值域的
值是 key 值域的值。

##### JSON-API #####
`none` 关键词如果用到这个维度，sidecar object 就不会显示。例如

```json
{
    "rows": [
        {
            "dateTime": "2014-09-01 00:00:00.000",
            "gender": "-1",
            "pageViews": 1681441753
        },{
            "dateTime": "2014-09-01 00:00:00.000",
            "gender": "f",
            "pageViews": 958894425
        },{
            "dateTime": "2014-09-01 00:00:00.000",
            "gender": "m",
            "pageViews": 1304365910
        }
    ],
    "gender": [
        {
            "id": "-1",
            "desc": "Unknown"
        },{
            "id": "f",
            "desc": "Female"
        },{
            "id": "m",
            "desc": "Male"
        }
    ]
}
```

简化之后变成

```json
{
    "rows": [
        {
            "dateTime": "2014-09-01 00:00:00.000",
            "gender": "-1",
            "pageViews": 1681441753
        },{
            "dateTime": "2014-09-01 00:00:00.000",
            "gender": "f",
            "pageViews": 958894425
        },{
            "dateTime": "2014-09-01 00:00:00.000",
            "gender": "m",
            "pageViews": 1304365910
        }
    ],
    "gender": [ ]
}
```


### 排序 ###

返回数据的排序[可以用 `sort` 查询参数实现](https://sampleapp.fili.io/v1/data/network/day/gender?metrics=pageViews&dateTime=2014-09-01/2014-09-02&sort=pageViews|asc)，
例如：

    sort=myMetric

排序默认是 _降序排序_，但 Fili 也支持升序排序。要指定度量的排序方向，您需要同时指定需要被排序的度量和排序方向，用 `|` 隔开，例如：

    sort=myMetric|asc

如果要对多个度量进行不同方向的排序，用逗号讲每一个排序隔开：

    sort=metric1|asc,metric2|desc,metric3|desc

#### 附加说明 ####

使用排序需要注意以下几点：

- 之后数据资源（Data resource）支持排序
- 数据永远是先以时间（`dateTime`）排序，然后才会按照查询中指定的进行排序，所以数据总是显示一定是按照时间先后的
- 只有度量支持排序
- 排序只在 Druid 数据库中进行。所以，如果 Fili 在 Druid 数据上对被排序的度量进行任何后续计算，排序后的数据可能出现偏差。

### TopN ###

假如我们需要知道 2014 年一月到九月之间，每个星期访问量居前三的网页，我们可以用 `topN` 语句快速得到答案。`topN` 计算每一个时间段
（time bucket）的 top 数据，每一次访问最多可以得到 `n` 个 top 结果。不难想到， `topN` 需要数据被排序，所以，每个 `topN` 查询包含
两个过程

1. `topN=n`，`n` 是每个时间段 top 的个数
2. `sort=metricName|desc` 指定 Fili 在筛选 top N 之前如何排序数据。关于排序的语句详解，请参见[排序](#排序)部分

用这个部分开头的问题，我们来看看如何用
[Fili 查询语句](https://sampleapp.fili.io/v1/data/network/week/pages?metrics=pageViews&dateTime=2014-06-01/2014-08-31&topN=3&sort=pageViews|desc)
来解决

    GET https://sampleapp.fili.io/v1/data/network/week/pages?metrics=pageViews&dateTime=2014-06-01/2014-08-31&topN=3&sort=pageViews|desc

我们需要每星期排名前三的网页访问量，所以 `n` 是 3，精度是 week。而且，我们要的是三个 _最大的_ 页访问量，所以我们把 `pageViews` 用
降序排列（第一条数据最大，第二条第二大，以此类推）。

Fili 在 `topN` 的数据基础上，还允许加入其他度量一并查询。比如我们想知道
[一月刘六号到九月一号之间，每星期页访问量排名前三网页的日均访问时间](https://sampleapp.fili.io/v1/data/network/week/pages?metrics=pageViews,dayAvgTimeSpent&dateTime=2014-06-01/2014-08-31&topN=3&sort=pageViews|desc)，
我们只需要在原来的 `topN` 查询语句中添加 `dayAvgTimeSpent`：

    GET https://sampleapp.fili.io/v1/data/network/week/pages?metrics=pageViews,dayAvgTimeSpent&dateTime=2014-06-01/2014-08-31&topN=3&sort=pageViews|desc

#### 附加说明 ####

当 `topN` 语句里包含多个度量时，Fili _只会_ 用已经排好序的度量进行 top N 计算。

因为 `topN` 给出的是 _每一个时间精度范围（each time bucket）_ 的 top N 结果，所以 `topN` 会给出 `n * Bucket数量` 条数据。在
以上两个例子里，我们会得到 `3 * 34 = 102` 条数据（二零一六年一月六号到九月一号之间有 34 个星期）。如果你只需要前 `n` 条数据，请
 参见[分页](#分页)部分。

异步查询
--------------------
Fili 支持数据异步查询（Asynchronous Queries）。数据接口的实现使用了 `asyncAfter` 参数。`asyncAfter` 参数可以决定一个数据访问
是否永远是同步的（synchronous）还是可以在数据查询过程中从同步切换成异步。如果 `asyncAfter=never`，那么 `Fili` 会无限期地等待
数据，在网络允许的情况下和 client 一直保持连接。这是默认配置。但是这个配置可以用 `default_asyncAfter` 配置参数修改。如果
`asyncAfter=always`，查询语句从一开始就会使异步的。如果 `asyncAfter=t`（t 代表一个正整数），那么在查询变成异步之前至少需要等待
`t` 毫秒。这个等待时间不是百分之百精确的。查询语句可能会花费比 `t` 毫秒更长的时间，这段时间里一直会是同步查询。所以说，
`asyncAfter=0` 和 `asyncAfter=always` 不完全一样。有可能 `asyncAfter=0` 最终会是一个同步查询（如果数据结果返回得够快的话）。但
如果是 `asyncAfter=always`，那么查询最终绝对不会是一个同步查询。

如果查询超时，数据仍然没有出来，那么访问者会收到一个 `202 Accepted` 响应和一个[任务元数据（job meta-data）](#任务元数据)。


### 任务接口
任务接口（jobs endpoint）is the one stop shop for queries about asynchronous jobs. This endpoint is responsible for:

1. 显示系统[所有任务](#显示所有任务概要)的列表。
2. 开放 `jobs/TICKET` 接口显示[某个任务](#获取任务状态)的状态。
3. 开放 `jobs/TICKET/results` 接口显示[任务结果](#获取任务结果)。

#### 获取所有任务概要
用户可以向 `jobs` 接口发送一个 `GET` 请求，获取所有任务的状态。

```
https://HOST:PORT/v1/jobs
```

如果系统没有正在进行的任务，会返回一个空集。

`jobs` 接口支持筛选任务值域（job fields, i.e. `userId`, `status`），使用方法和[数据接口的筛选](#筛选)相同。例如：

`userId-eq[greg, joan], status-eq[success]`

转换成如下 boolean 运算：

`(userId = greg OR userId = joan) AND status = success`

最后会返回由 `greg` 和 `joan` 发送的，已经顺利完成的任务。

#### 获取任务状态
当用户向 `jobs/TICKET` 发送一个 `GET` 请求时，Fili 会搜寻指定的 ticket 并返回任务的元数据（job's meta-data）如下：

###### 任务元数据
```json
{
    "query": "https://HOST:PORT/v1/data/QUERY",
    "results": "https://HOST:PORT/v1/jobs/TICKET/results",
    "syncResults": "https://HOST:PORT/v1/jobs/TICKET/results?asyncAfter=never",
    "self": "https://HOST:PORT/v1/jobs/TICKET",
    "status": ONE OF ["pending", "success", "error"],
    "jobTicket": "TICKET",
    "dateCreated": "ISO 8601 DATETIME",
    "dateUpdated": "ISO 8601 DATETIME",
    "userId": "Foo"
}
```

* `query` 是用户的查询语句
* `results` 是数据结果的链接，这个链接可以是完全同步的或者由同步超时后最终切换成异步的，取决于 `asyncAfter` 的默认配置。
* `syncResults` 是一个数据结果的异步链接（因为最后有 `asyncAfter=never` 参数）
* `self` 是一个显示最新任务状态的链接
* `status` 显示当前结果状态
    - `pending` - 任务还在进行中
    - `success` - 任务顺利结束
    - `error` - 任务报错失败
    - `canceled` - 用户终止了任务（开发中）
* `jobTicket` 这个任务的 ID
* `dateCreated` 任务创建日期
* `dateUpdated` 任务上次更新日期
* `userId` 提交这次任务的用户 ID

如果 ticket 在系统中不存在，会得到一个 404 报错，显示 `No job found with job ticket TICKET`。

#### 获取任务结果
用户可以给 `jobs/TICKET/results` 发送 `GET` 请求，获取某次查询的结果。这个接口需要下列参数：

1. **格式（`format`）** - 用户可以指定返回数据的格式： csv 或者 JSON。这和数据接口的[格式（`format`）](#格式) 的用法是
 一样的。

2. **`page`, `perPage`** - [分页](#分页) 参数。 用法和访问数据接口一样，都是得到某页结果。

3. **`asyncAfter`** - 指定用户的最长等待结果的时间。用法和数据接口的[`asyncAfter`](#异步查询)参数一样。

如果 ticket 出结果了，我们就会得到指定格式显示的结果，否则的话，得到[任务元数据（job meta-data）](#任务元数据)。

##### 长轮询
如果访问希望用长轮询（Long Polling）的方式获取结果，可以发送 `GET` 请求到
`https://HOST:PORT/v1/jobs/TICKET/results?asyncAfter=never`（这实际上就是上面 `syncResults` 代表的链）。该请求会以同步请求的
方式发送：Fili 只有在所有数据计算好之后才会往回传结果。

其它
----

### 日期和时间 ###

时间段用 `dateTime` 参数表示，格式是 `dateTime=d1/d2`。 `d1/d2` 里面 `d1` 是起始时间，`d2` 是不包含在内的终止时间。例如，
`dateTime=2015-10-01/2015-10-03` 包含了十月一号，十月二号的数据，不包含十月三号的数据。日期可以是：

1. ISO 8601 格式日期
2. ISO 8601 时间区间（见下文）
3. Date macro（见下文）

我们的 API 遵照 ISO 8601 标准。如果您想深入了解，维基百科上面有[相关文章](http://en.wikipedia.org/wiki/ISO_8601)具体解释
 ISO 8601 日期和时间。

#### 日期区间 ####

日期区间（Date Periods），即 ISO 8601 时间区间，完全遵照 [ISO 8601 standard](https://en.wikipedia.org/wiki/ISO_8601#Durations) 标准实现。表示方式很
简单，一个区间（period）以 `P` 开头，后面跟一个数字，然后一个时间精度（M为月份，W为星期，D为某日，以此类推）。举例，如果您需要 30
天的数据，就用 `P30D`。区间可以重复叠加，例如， `P1Y2M` 代表一年零两个月。

日期区间可以放在查询语句的起始或者终止日期处。

#### Date Macros ####

我们提供一个叫做 `current` 的宏（macro），表示当前精度下的起始时间。例如，如果您的时间就精度是`day`，那么 `current` 就会转换成
今天的日期。如果您的精度是 `month`，那么 `current` 就会转换成本月第一天。

还有一个功能类似的宏叫做 `next`，表示当前精度下的下一个起始时间。例如，如果你的精度是 `day`，那么 `next` 会转换成明天。


#### 时区

时区暂时不能用在时间段的起始或者终止时间。不过查询语查询的时区可以用 `timeZone` 查询参数修改。这会改变时间段里面日期时间
（`dateTime`）所处的时区。查询语句会自动使用默认的时区参数，而任何
[时区识别符](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones#List)可以去覆盖这个默认值。例如，以下查询参数

    dateTime=2016-09-16/2016-09-17&timeZone=America/Los_Angeles
    vs
    dateTime=2016-09-16/2016-09-17&timeZone=America/Chicago

会生成如下时间段

    2016-09-16 00:00:00-07:00/2016-09-17 00:00:00-07:00
    and
    2016-09-16 00:00:00-05:00/2016-09-17 00:00:00-05:00

<sub>注意两个时间段的不同时差</sub>

### 大小写区分 ###

API 的所有语法都是区分大小写的（case-sensitive），例如，`pageViews`，`pageviews`，`PaGeViEwS` 都是有区别的。

### 速率限定 ###

为了防止他人破坏系统，API 限定用户在某一时间段内只能发送一定量的请求。如果您发请求过快，就会收到报错回复，状态码（response status
code）为 429。

### 报错 ###

使用 API 时候难免遇到报错情况，报错都会返回一个报错状态码，大多都还带有额外报错信息。

| 状态码 | 含义                             | 起因                                                                                                                                                                                                          |
| ------ | ------------------------------- | ------------------------------------------------------------------------------------------------------------  |
| 400    | BAD REQUEST                     | 您的请求有语法错误。Fili 没法读懂您请求。                                                                       |
| 401    | UNAUTHORIZED                    | Fili 无法识别您的身份，下次发送请求的时候请附上身份信息。这通常是因为您的请求里缺失安全验证信息                      |
| 403    | FORBIDDEN                       | Fili 可以识别您的身份，但是您没有足够权限访问某些资源                                                            |
| 404    | NOT FOUND                       | 找不到访问的资源，请求 URL 可能有拼写错误                                                                       |
| 416    | REQUESTED RANGE NOT SATISFIABLE | 数据无法在 Druid 里头找到                                                                                      |
| 422    | UNPROCESSABLE ENTITY            | 请求无语法错误，但是其它地方出错了，可能是维度没有匹配上，或者某个度量+维度组合在 API 列表（logical table）里不支持。 |
| 429    | TOO MANY REQUESTS               | 访问超过速率限制。稍等片刻，等您的其他请求完成处理了以后在发新请求                                                 |
| 500    | INTERNAL SERVER ERROR           | 服务器内部处理过程中出错了，我们竭尽全力减少这些情况的发生，但是若有遗漏，您要是收到了 500 报错，请告诉我们。         |
| 502    | BAD GATEWAY                     | Druid 故障响应（Bad Druid response）。                                                                         |
| 503    | SERVICE UNAVAILABLE             | Druid 宕机了。                                                                                                |
| 504    | GATEWAY TIMEOUT                 | Druid 访问超时。                                                                                              |
| 507    | INSUFFICIENT STORAGE            | 查询耗费太多资源了                                                                                             |
