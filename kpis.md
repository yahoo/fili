Key Performance Indicators - Fili Web Service
=============================================

These are the key performance indicators for the Fili Web Service component, listed in categories by order of importance.


Server Error Responses (HTTP 5XX)
---------------------------------

Shows how much trouble _the service_ is having.

- com.codahale.metrics.servlet.AbstractInstrumentedFilter.responseCodes.serverError.m1_rate


Druid Errors
------------

Shows how much trouble queries are having against druid.

- druid.errors.exceptions.m1_rate
- druid.errors.http.m1_rate


Requests
--------

Shows how many requests the service is serving.

- com.codahale.metrics.servlet.AbstractInstrumentedFilter.requests.m1_rate
- com.codahale.metrics.servlet.AbstractInstrumentedFilter.requests.m15_rate


System Metrics
--------------

Shows the overall health of the system's low-level resources and activities.

- CPU
- Memory
- Network IO
- GC Pauses


Latency
-------

Shows duration of overall requests and druid requests. (m1_rate and pN)

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


Rate Limiting Rejections
------------------------

Shows if users are hitting rate limits.

- ratelimit.meter.reject.ui.m1_rate
- ratelimit.meter.reject.user.m1_rate
- ratelimit.meter.reject.global.m1_rate


Active Requests
---------------

Shows load at a given point in time. (ie. how close are the load is to the limits of Druid)

- com.codahale.metrics.servlet.AbstractInstrumentedFilter.activeRequests.count


Bad Request Responses (HTTP 4XX)
--------------------------------

Shows how much trouble users are having interacting with the API.

- com.codahale.metrics.servlet.AbstractInstrumentedFilter.responseCodes.badRequest.m1_rate
- com.codahale.metrics.servlet.AbstractInstrumentedFilter.responseCodes.notFound.m1_rate

