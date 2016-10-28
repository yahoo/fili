Logging Guidelines
==================

Logs are an important tool for debugging problems, as well as for gaining insight into how something is running under
otherwise normal conditions. Because logs are so useful, it's tempting to always log everything that's going on, but
processing logs is much more work than it would seem and writing too many logs has a significant negative impact on
performance.

Summary
-------

Details and examples are below, but here's a summary of the log levels and their meanings.

| Level | Meaning                                                                                                      |
| ----- | ------------------------------------------------------------------------------------------------------------ |
| Error | System-caused problem preventing correct results for requests. Major, "wake up a human" events.              |
| Warn  | Something's wrong, like no caching, but can still correctly respond to requests. A human should investigate. |
| Info  | Working as expected. Information needed for monitoring. Answers "Are things healthy."                        |
| Debug | High-level data flow and errors. Rich insight, but not overwhelming. Per-request errors, but not happy-path. |
| Trace | Most verbose. Full data flow. Not intended to be used in production, or very rarely.                         |

Log Levels
----------

To be able to balance the trade off between logging more information and having better performance, Fili's logging
framework allows controlling which log messages are processed using Log Levels. Here are some guidelines to help figure
out what level a particular message should be logged at. By following these guidelines, logging will be handled 
consistently across the Fili codebase.

### Error

Should be used when Fili is having trouble and cannot reliably respond to requests. Under normal running conditions, 
there shouldn't be `Error`-level logs emitted by Fili. Any `Error`-level logs that _are_ emitted by Fili should be major
events that likely require urgent intervention. Here are some examples:

- There is a configuration error that will prevent the application from working _correctly_.
- There is an unexpected error (ie. not caused by the user) that is going to result in failing the request with a 500 
  HTTP status code. 
  
  Expected request failure conditions that are _not_ the user's fault which result in 5xx-level HTTP
  status codes should _not_ result in a log at the `Error` level. Examples of these include `503 Service Unavailable` 
  responses when the dimension metadata hasn't been loaded, or `507 Insufficient Storage` when a request exceeds the
  weight limit.

### Warn

Should be used when something isn't right, but Fili can still reliably respond to requests. Under normal running
conditions, there _may_ be `Warn`-level logs emitted by Fili. `Warn`-level logs that are emitted by Fili _should_ be
looked at by a human, but should not typically require urgent intervention. Here are some examples:

- There is a configuration error that will not prevent requests from being correctly satisfied, but requests may be 
  satisfied non-optimally or with some other down-side.
- The app tries to start as indicated, but has to fall back to some default configuration because an error was 
  encountered. The app can still start, but not the way it expected to.
- A non-critical service is unavailable or is having trouble. It's possible that the issue will correct itself, but that
  depends on the service.

  For example, the Druid response cache might be having errors or timing out. This won't prevent Fili from correctly
  responding to the request, but it may not be as fast due to the cache not working.

### Info

Should be used to indicate status and telemetry for each HTTP request processed, as well as non-recurring events and
information about the state of Fili. It is expected that Fili will be running at the `Info` logging level when running
in production. As such, the information logged at this level should be the information needed for monitoring purposes,
meaning that it effectively answers "Is the application healthy?" and "How is the application being used?"

- Startup / shutdown information (success or failure of main components starting or stopping)
- Timing and telemetry about processing each request

### Debug

Should be used to indicate high-level data flow and request handling steps. It's expected that the `Debug` log level
will be used when determining why something is behaving unexpectedly. As such, the information logged at the `Debug`
level should give a fairly rich insight into what is happening, but should not contain so much information as to be
overwhelming. 

One exception to this is that "normal" or "[happy path](https://en.wikipedia.org/wiki/Happy_path)" request processing 
flow should _not_ be logged at the `Debug` level. This is because it's assumed that the bulk of the work will be for
processing requests and we don't want to flood the log with too many "everything is working as normal" events. Here are
some examples of what should be logged at the `Debug` level:

- When the request is going to fail due to user error, like an improper date format or other input validation error.
- Once-per-request information, like which Physical Table is selected.
- Normal execution flow while starting up / shutting down.
- Normal execution flow while running, but not processing a request.

### Trace

`Trace` is the most verbose log level and it's expected that it will only be turned on very rarely in a production
setting. This is the level at which to log "normal" execution flow for request handling.

- Normal execution flow while processing a request.
- Large or tight loops for any kind of execution (app-start, non-request, request processing).
    - If the loop is time-sensitive or particularly hot, however, consider logging outside the loop so as not to impede
      performance.
