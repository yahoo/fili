Monitoring and Operations
=========================

Fili was built from the start to be easy to operate and debug. There are 3 primary ways in which Fili makes debugging
and gathering information about how it's running easy.

Logs
----

Fili has [strict guidelines](contributing/logging-guidelines.md) around what information should be logged, when it 
should be logged, and what level it should be logged at. Here's a brief summary of the log levels and what they contain:

| Level | Meaning                                                                                                      |
| ----- | ------------------------------------------------------------------------------------------------------------ |
| Error | System-caused problem preventing correct results for requests. Major, "wake up a human" events.              |
| Warn  | Something's wrong, like no caching, but can still correctly respond to requests. A human should investigate. |
| Info  | Working as expected. Information needed for monitoring. Answers "Are things healthy."                        |
| Debug | High-level data flow and errors. Rich insight, but not overwhelming. Per-request errors, but not happy-path. |
| Trace | Most verbose. Full data flow. Not intended to be used in production, or very rarely.                         |

### Request Log

Fili also has a tracing mechanism that records and collects information about each request as it flows through the
system. This information is called the Request Log and it gets logged at the [INFO](contributing/logging-guidelines.md#info) 
level when the response for a request is sent. 

The Request Log is modular, and has a number of different components, depending on which type of request is being served.
All Request Logs contain a Durations, Threads, and Preface component at the beginning, an Epilogue component at the end,
and other components that are added as requests are processed will get added in between in the order they are added.
If desired, this ordering can be controlled by setting the `requestlog_loginfo_order` property.

As part of the Request Log tracing, each request is assigned a UUID that which shows up in the INFO line for Request Log
log lines. This UUID is also made available on _every_ log line via [MDC](http://www.slf4j.org/api/org/slf4j/MDC.html)
under the key `logid` so that all log lines emitted while processing a request can be easily collected together. To 
surface this UUID in all log lines emitted while processing a request, your log format needs to include a reference to
the UUID value in MDC. Here are some examples for common logging frameworks:

[Logback](http://logback.qos.ch/manual/layouts.html#mdc)
```
%mdc{logid:-unset}
%X{logid:-unset}
```
Note that the `-unset` sets the default value to `unset` if there is no `logid` value present in MDC, which is possible
for logs emitted outside of processing a request, like background processes.

[Log4J](https://logging.apache.org/log4j/1.2/apidocs/org/apache/log4j/PatternLayout.html)
```
%X{logid}
```

[Log4J2](https://logging.apache.org/log4j/2.x/manual/layouts.html)
```
%X{logid}
%mdc{logid}
%MDC{logid}
```


Metrics
-------

Fili uses the [Metrics](http://metrics.dropwizard.io/) library (formerly Dropwizard Metrics) for gathering and reporting
on runtime metrics and indicators. Typically, these are exposed through an admin servlet, and if that's enabled then
there is [a list of Key Performance Indicators (KPIs)](kpis.md) that are exposed through the `/metrics` endpoint. The 
KPI document doesn't go into detail about what each of the KPIs mean, but it gives a rough overview of why they matter.  


Health Checks
-------------

Fili also uses the Metrics library for implementing health checks. These are also exposed through the admin servlet,
just like metrics, at `/status`. Each of the health checks has a message and a status, and if any of the checks fail, 
the HTTP Status Code for that `/status` request will be a 500 instead of a 200 to indicate the system is unhealthy.

These health checks are also used to gate `/data` requests, with Fili returning `503 Service Unavailable` if it doesn't
think it is healthy.
