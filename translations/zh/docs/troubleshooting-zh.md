Troubleshooting
===============

本文提供配置和使用 Fili 时常见问题的解决方案。如果您发现了诸如此类的问题，欢迎提交您的解决方案。

目录
----

- [常规（General）](#常规)
- [我的应用测试不能通过](#应用测试返回500类别报错)
- [Fili 终止运行并出现 IllegalStateException 报错：Couldn't create dir: path/to/mdbm/dimensionCache/page"](#Fili终止运行并出现IllegalStateException报错)
- [元数据加载器没有正常运作](#服务器日志显示数据段元数据加载器没有正常运作)
- [我的查询语句报错了](#查询语句排错)


常规
----
Fili 的服务器日志实质上是 Jetty 日志，所以 Fili 的日志和 Jetty 实例的日志是放在一个路径下的。

日志涵盖了很多信息，可以提示错误所在。[相关信息请参见日志级别][fili-logging]

应用测试返回500类别报错
----------------------------

可能是您应用中 POM 文件里的依赖应用版本和 Fili 用的不一致。如果是那样的话，解决办法就是在您的应用中使用和 Fili 一样的
版本。


Fili终止运行并出现IllegalStateException报错
-------------------------------------------

Fili 终止运行并出现报错 `IllegalStateException: Couldn't create dir: path/to/mdbm/dimensionCache/page` 的两个可能原因是：
 
 1. `path/to/mdbm` 中的 `dimensionCache` 子文件夹不存在。Fili 须要 `path/to/mdbm/dimensionCache` 文件件事先建好，Fili
 是不会自己去建的。
 
 2. `dimensionCache` 子文件夹存在，但是读写权限没有设对。Jetty 运行下的 user（通常是 `nobody`）需要 `dimensionCache` 的
 读，写，和执行权限。


服务器日志显示数据段元数据加载器没有正常运作
--------------------------------------------

您的维度信息没有更新，没有设 `lastUpdated`。此类问题出现的原因是您没有设置维度加载器（dimension loader）。您可以在
`/healthcheck` API 端点（endpoint）查看详细的信息。如果确实是维度没有被加载，那么请参照[维度加载](#dimension-loading)，
配置维度加载器（或者[将所有维度设成非加载类][non-loaded-dimensions]）。


查询语句排错
------------

如果你给 Fili 发送一条查询语句，结果出现了报错，您可以在查询语句的末尾加上 `format=debug` 帮助您排错，例如

```
GET http://localhost:9998/v1/data/wikipedia/day?metrics=added&dateTime=2000-01-01/3000-01-01&format=debug
```

通过这种方法您可以查看发往 Druid 的查询语句信息。关于 Druid 查询语句相关信息 [请参见 Druid 查询语句文档][druid-docs]。



[druid-docs]: http://druid.io/docs/latest/querying/querying.html
[fili-logging]: contributing/logging-guidelines-zh.md
[dimension-loading]: setup-zh.md#dimension-loading
[non-loaded-dimensions]: setup.md#non-loaded-dimensions
