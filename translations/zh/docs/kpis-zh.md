关键运行情况指标 - Fili Web 服务
================================

以下是 Fili Web 服务的关键运行情况指标，根据类别按照重要程度依次列出。


服务器报错（HTTP 5XX）
---------------------

显示服务出现了多少次报错。

- com.codahale.metrics.servlet.AbstractInstrumentedFilter.responseCodes.serverError.m1_rate


Druid 报错
----------

显示查询访问 druid 时出现了多少次报错。

- druid.errors.exceptions.m1_rate
- druid.errors.http.m1_rate


访问
----

显示服务处理了多少次访问。

- com.codahale.metrics.servlet.AbstractInstrumentedFilter.requests.m1_rate
- com.codahale.metrics.servlet.AbstractInstrumentedFilter.requests.m15_rate


系统运行状况
------------

显示系统底层资源和运行情况的正常与否。

- CPU
- 内存
- 网络 IO
- Garbage Collection 骤停


延时
----

显示整个请求和 druid 请求用时。（m1_rate 和 pN）

- com.codahale.metrics.servlet.AbstractInstrumentedFilter.requests.p50
- com.codahale.metrics.servlet.AbstractInstrumentedFilter.requests.p75
- com.codahale.metrics.servlet.AbstractInstrumentedFilter.requests.p95
- com.codahale.metrics.servlet.AbstractInstrumentedFilter.requests.p98
- com.codahale.metrics.servlet.AbstractInstrumentedFilter.requests.p99
- com.codahale.metrics.servlet.AbstractInstrumentedFilter.requests.p999
- druid.requests.p50
- druid.requests.p75
- druid.requests.p95
- druid.requests.p98
- druid.requests.p99
- druid.requests.p999


访问速率限制
------------

显示用户是否超过最大访问速率。

- ratelimit.meter.reject.ui.m1_rate
- ratelimit.meter.reject.user.m1_rate
- ratelimit.meter.reject.global.m1_rate


处理中的请求
------------

显示某个时间的负载。（距离 Druid 的最大负载能力还差多少）

- com.codahale.metrics.servlet.AbstractInstrumentedFilter.activeRequests.count


错误请求相应（HTTP 4XX）
-----------------------

显示用户发送了多少次不符合 API 语法要求的请求。

- com.codahale.metrics.servlet.AbstractInstrumentedFilter.responseCodes.badRequest.m1_rate
- com.codahale.metrics.servlet.AbstractInstrumentedFilter.responseCodes.notFound.m1_rate
