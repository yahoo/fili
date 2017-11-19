Fili
====

[![Download](https://api.bintray.com/packages/yahoo/maven/fili/images/download.svg)](https://bintray.com/yahoo/maven/fili/_latestVersion) [![Gitter](https://img.shields.io/gitter/room/yahoo/fili.svg?maxAge=2592000)](https://gitter.im/yahoo/fili) [![Travis](https://img.shields.io/travis/yahoo/fili/master.svg?maxAge=2592000)](https://travis-ci.org/yahoo/fili/builds/) [![Codacy grade](https://img.shields.io/codacy/grade/91fa6c38f25d4ea0ae3569ee70a33e38.svg?maxAge=21600)](https://www.codacy.com/app/Fili/fili/dashboard) [![Users Google Group](https://img.shields.io/badge/google_group-users-blue.svg?maxAge=2592000)](https://groups.google.com/forum/#!forum/fili-users) [![Developers Google Group](https://img.shields.io/badge/google_group-developers-blue.svg?maxAge=2592000)](https://groups.google.com/forum/#!forum/fili-developers)

Fili 是用来搭建和维护 RESTful web 服务的 Java 框架，主要应用于时间数据的访问和分析。Fili 的[访问 API](docs/end-user-api-zh.md)
基于 HTTP GET，十分简洁，易于使用，大大简化了[度量（metic）和维度（dimension）的定义](docs/end-user-api-zh.md)，数据
存储，以及访问查询优化。Fili 是一个适用于大数据，高拓展性的框架，目前完全支持 [Druid](http://druid.io) 数据库，Fili
有很强的扩展性，可以兼容其他任何数据库。

Fili的数据访问、分析包括以下几个核心概念：

- [度量（Metrics）](docs/end-user-api-zh.md#metrics)
- [维度（Dimensions）](docs/end-user-api-zh.md#dimensions)
- [列表（Tables）](docs/end-user-api-zh.md#tables)
- 时间 ([时间单位](docs/end-user-api-zh.md#time-grain) 和 [时间段](docs/end-user-api-zh.md#interval))

Fili 为了简化终端用户操作，不提供 Views, Partitions, 和 metric formulas 等底层操作。Fili
[风格极简的API](docs/end-user-api-zh.md)让用户轻松从数据中发掘商业价值，Fili 会去负责如何挖掘。

Fili 具备灵活的数据库存储和访问，搭配 Fili 的 web 服务在不影响用户层的情况下可以顺利转移数据，优化访问，切换数据库。

除此之外，Fili还提供其他功能，部分如下：

| 功能                                                         | 操作                                               |
|--------------------------------------------------------------|----------------------------------------------------|
| 高级标准定义  (Complex metric definition)	                   | 访问速率控制  (Rate limiting)                      |
| 高性能 (slice routing  Performance slice routing)	           | 查询权重检查 (Query weight checks)                 |
| 多维度连接 (Dimension joins (both annotation and filtering)) | [Rich usage metrics](monitoring-and-operations.md) |
| 部分区间保护 (Partial interval protection)                   | 健康检查 (Health checks)                           |
| 易变数据处理 (Volatile data handling)                        | 缓存 (Caching)                                     |
| 模块化架构 (Modular architecture)                            |                                                    |


社区 [![Gitter](https://img.shields.io/gitter/room/yahoo/fili.svg?maxAge=2592000)](https://gitter.im/yahoo/fili) [![Users Google Group](https://img.shields.io/badge/google_group-users-blue.svg?maxAge=2592000)](https://groups.google.com/forum/#!forum/fili-users) [![Developers Google Group](https://img.shields.io/badge/google_group-developers-blue.svg?maxAge=2592000)](https://groups.google.com/forum/#!forum/fili-developers)
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Fili 的开源社区设在 [Gitter](https://gitter.im/yahoo/fili)，可以讨论问题，意见，想法，新功能，新需求。我们希望大家使用
Fili 作为处理时间大数据的商业应用，并与我们联系，帮助您让 Fili 在你的商业方案中发挥最大效用。Fili 还有很多未完成的功能，
你的反馈可以帮助 Fili 向更好的方向改进。

如果你有其他问题，例如 Fili 出现故障，无法在 Gitter 上获得解决方案，请使用
 [GitHub Issue](https://github.com/yahoo/fili/issues)。

如果你想参与 Fil 开发，请参阅[CONTRIBUTING](CONTRIBUTING-zh.md)。

使用简介
-----------

Fili 自带一个配置好的[样本应用](fili-wikipedia-example)，你可以将其修改变成你自己的 web 服务。样本应用提供 Wikipedia 的
文章编辑数据，以 [Druid's quick-start tutorial](http://druid.io/docs/0.9.1.1/tutorials/quickstart.html) 为参照。

版本
-----------

Fili 已经发布稳定版，但是扔处于开发阶段，很多大的修改和新功能会持续并入。

开发是在当前的 snapshot 版本上开发。

### @Deprecated

标注了`@Deprecated`的API将在后续的发布中移除。过时的API会被维护一个发布周期，届时请尽快更新API。API不再维护之后随时会被清除。

下载
------------------------

编译好的 Fili 放在 [Bintray](https://bintray.com/yahoo/maven/fili)。Maven, Ivy, Gradle 开发者可以参照
https://bintray.com/yahoo/maven/fili，例如：

Maven:
```xml
<dependency>
    <groupId>com.yahoo.fili</groupId>
    <artifactId>fili</artifactId>
    <version>x.y.z</version>
</dependency>

<repository>
    <id>fili</id>
    <url>http://yahoo.bintray.com/maven</url>
</repository>
```

Gradle:
```groovy
repositories {
    maven { url 'http://yahoo.bintray.com/maven' }
}

dependencies {
    compile 'com.yahoo.fili:fili:x.y.z'
}
```

最新发布版本: [![Bleeding-edge](https://api.bintray.com/packages/yahoo/maven/fili/images/download.svg)](https://bintray.com/yahoo/maven/fili/_latestVersion)

最新稳定版本: [![Stable](https://img.shields.io/badge/Stable-0.7.36-blue.svg)](https://bintray.com/yahoo/maven/fili/0.7.36)


扩展
---------

Fili有很强的扩展性，配备了很多hooks([`AbstractBinderFactory`](https://github.com/yahoo/fili/blob/master/fili-core/src/main/java/com/yahoo/bard/webservice/application/AbstractBinderFactory.java))!

参与开发 [![Developers Google Group](https://img.shields.io/badge/google_group-developers-blue.svg?maxAge=2592000)](https://groups.google.com/forum/#!forum/fili-developers)
------------

请参照[CONTRIBUTING](CONTRIBUTING.md)。


LICENSE
-------

Copyright 2016 Yahoo! Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
language governing permissions and limitations under the License.
