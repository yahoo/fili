<div id="table-of-contents">
<h2>Table of Contents</h2>
<div id="text-table-of-contents">
<ul>
<li><a href="#fili">1. Fili</a>
<ul>
<li><a href="#gitter-users-google-group-developers-google-group">1.1. 社区</a></li>
<li><a href="#">1.2. 使用简介</a></li>
<li><a href="#">1.3. 版本</a>
<ul>
<li><a href="#deprecated">1.3.1. @Deprecated</a></li>
</ul>
</li>
<li><a href="#">1.4. 下载</a></li>
<li><a href="#">1.5. 扩展</a></li>
<li><a href="#developers-google-group">1.6. 参与开发</a></li>
<li><a href="#license">1.7. LICENSE</a></li>
</ul>
</li>
</ul>
</div>
</div>

# Fili<a id="fili" name="fili"></a>


[![img](https://api.bintray.com/packages/yahoo/maven/fili/images/download.svg)](https://bintray.com/yahoo/maven/fili/_latestVersion)
[![img](https://img.shields.io/gitter/room/yahoo/fili.svg)](https://gitter.im/yahoo/fili)
[![img](https://img.shields.io/travis/yahoo/fili/master.svg)](https://travis-ci.org/yahoo/fili/builds/)
[![img](https://img.shields.io/codacy/grade/91fa6c38f25d4ea0ae3569ee70a33e38.svg)](https://www.codacy.com/app/Fili/fili/dashboard)
[![img](https://img.shields.io/badge/google_group-users-blue.svg)](https://groups.google.com/forum/#!forum/fili-users)
[![img](https://img.shields.io/badge/google_group-developers-blue.svg)](https://groups.google.com/forum/#!forum/fili-developers)

Fili是用来搭建和维护RESTful
web服务的Java框架，主要应用于时间数据的访问和分析。Fili的[访问API](docs/end-user-api.md)基于HTTP
GET，十分简洁，易于使用，大大简化了[metic和dimension定义](docs/end-user-api.md)，数据存储，以及访问查询优化。Fili是一个适用于大数据，高拓展性的框架，目前完全支持[Druid](http://druid.io)数据库，Fili有很强的扩展性，可以兼容其他任何数据库。

Fili的数据访问、分析包括以下几个核心概念：

-   [Metrics](docs/end-user-api.md#metrics)
-   [Dimensions](docs/end-user-api.md#dimensions)
-   [列表](docs/end-user-api.md#tables)
-   时间 ([时间单位](docs/end-user-api.md#time-grain) 和
    [时间段](docs/end-user-api.md#interval))

Fili为了简化终端用户操作，不提供Views, Partitions, 和metric
formulas等底层操作。Fili[风格极简的API](docs/end-user-api.md)让用户轻松从数据中发掘商业价值，Fili会去负责如何挖掘。

Fili具备灵活的数据库存储和访问，搭配Fili的web服务在不影响用户层的情况下可以顺利转移数据，优化访问，切换数据库。

除此之外，Fili还提供其他功能，部分如下：

<table border="2" cellspacing="0" cellpadding="6" rules="groups" frame="hsides">


<colgroup>
<col  class="left" />

<col  class="left" />
</colgroup>
<thead>
<tr>
<th scope="col" class="left">功能</th>
<th scope="col" class="left">操作</th>
</tr>
</thead>

<tbody>
<tr>
<td class="left">高级 metric 定义</td>
<td class="left">访问速率控制</td>
</tr>


<tr>
<td class="left">高性能 slice routing</td>
<td class="left">查询权重检查 Query weight checks</td>
</tr>


<tr>
<td class="left">Dimension joins (both annotation and filtering)</td>
<td class="left">[Rich usage metrics](monitoring-and-operations.md)</td>
</tr>


<tr>
<td class="left">部分区间保护 Partial interval protection</td>
<td class="left">健康检查</td>
</tr>


<tr>
<td class="left">易变数据处理Volatile data handling</td>
<td class="left">缓存</td>
</tr>


<tr>
<td class="left">模块化架构</td>
<td class="left">&#xa0;</td>
</tr>
</tbody>
</table>

## 社区<a id="社区-gitter-users-google-group-developers-google-group" name="社区-gitter-users-google-group-developers-google-group"></a>

[![img](https://img.shields.io/gitter/room/yahoo/fili.svg)](https://gitter.im/yahoo/fili)
[![img](https://img.shields.io/badge/google_group-users-blue.svg)](https://groups.google.com/forum/#!forum/fili-users)
[![img](https://img.shields.io/badge/google_group-developers-blue.svg)](https://groups.google.com/forum/#!forum/fili-developers)


Fili的开源社区设在[Gitter](https://gitter.im/yahoo/fili)，可以讨论问题，意见，想法，新功能，新需求。我们希望大家使用Fili作为处理时间大数据的商业应用，并与我们联系，帮助您让Fili在你的商业方案中发挥最大效用。Fili还有很多未完成的功能，你的反馈可以帮助Fili向更好的方向改进。

如果你有其他问题，例如Fili出现故障，无法在Gitter上获得解决方案，请使用
[GitHub Issue](https://github.com/yahoo/fili/issues)。

如果你想参与Fil开发，请参阅[CONTRIBUTING](CONTRIBUTING.md)。

## 使用简介<a id="使用简介" name="使用简介"></a>


Fili自带一个配置好的[样本应用](fili-wikipedia-example)，你可以将其修改变成你自己的web服务。样本应用提供Wikipedia的文章编辑数据，以[Druid's
quick-start tutorial](http://druid.io/docs/0.9.1.1/tutorials/quickstart.html)为参照。

## 版本<a id="版本" name="版本"></a>


Fili已经发布稳定版，但是扔处于开发阶段，很多大的修改和新功能会持续并入。

Active development happens on the patch version of the highest minor
version.

### @Deprecated<a id="deprecated" name="deprecated"></a>


标注了=@Deprecated=的API将在后续的发布中移除。过时的API会被维护一个发布周期，届时请尽快更新API。API不再维护之后随时会被清除。

## 下载<a id="下载" name="下载"></a>


编译好的Fili放在[Bintray](https://bintray.com/yahoo/maven/fili)。Maven,
Ivy, Gradle开发者可以参照<https://bintray.com/yahoo/maven/fili，例如：>

Maven:

    <dependency>
        <groupId>com.yahoo.fili</groupId>
        <artifactId>fili</artifactId>
        <version>x.y.z</version>
    </dependency>
    
    <repository>
        <id>fili</id>
        <url>http://yahoo.bintray.com/maven</url>
    </repository>

Gradle:

    repositories {
        maven { url 'http://yahoo.bintray.com/maven' }
    }
    
    dependencies {
        compile 'com.yahoo.fili:fili:x.y.z'
    }

最新发布版本:
[[![img](//bintray.com/yahoo/maven/fili/_latestVersion][[[https:/api.bintray.com/packages/yahoo/maven/fili/images/download.svg)]]]]

最新稳定版本:
[[![img](//bintray.com/yahoo/maven/fili/0.7.36][[[https:/img.shields.io/badge/Stable-0.7.36-blue.svg)]]]]

## 扩展<a id="扩展" name="扩展"></a>


Fili有很强的扩展性，配备了很多hooks([`AbstractBinderFactory`](https://github.com/yahoo/fili/blob/master/fili-core/src/main/java/com/yahoo/bard/webservice/application/AbstractBinderFactory.java))!

## 参与开发<a id="参与开发-developers-google-group" name="参与开发-developers-google-group"></a>

[![img](https://img.shields.io/badge/google_group-developers-blue.svg)](https://groups.google.com/forum/#!forum/fili-developers)


请参照[CONTRIBUTING](CONTRIBUTING.md)。

## LICENSE<a id="license" name="license"></a>


Copyright 2016 Yahoo! Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may
not use this file except in compliance with the License. You may obtain
a copy of the License at

<http://www.apache.org/licenses/LICENSE-2.0>

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.