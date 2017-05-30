// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.responseprocessors;

/**
 * Response processor for that extracts header information from Druid response and put the information in our own
 * response.
 * <p>
 * This is a "Full" response process in a sense that it extracts and incorporates header information. For example,
 *
 * <pre>
 * {@code
 * Content-Type: application/json
 * 200 OK
 * Date:  Mon, 10 Apr 2017 16:24:24 GMT
 * Content-Type:  application/json
 * X-Druid-Query-Id:  92c81bed-d9e6-4242-836b-0fcd1efdee9e
 * X-Druid-Response-Context: {
 *     "uncoveredIntervals": [
 *         "2016-11-22T00:00:00.000Z/2016-12-18T00:00:00.000Z","2016-12-25T00:00:00.000Z/2017-
 *         01-03T00:00:00.000Z","2017-01-31T00:00:00.000Z/2017-02-01T00:00:00.000Z","2017-02-
 *         08T00:00:00.000Z/2017-02-09T00:00:00.000Z","2017-02-10T00:00:00.000Z/2017-02-
 *         13T00:00:00.000Z","2017-02-16T00:00:00.000Z/2017-02-20T00:00:00.000Z","2017-02-
 *         22T00:00:00.000Z/2017-02-25T00:00:00.000Z","2017-02-26T00:00:00.000Z/2017-03-
 *         01T00:00:00.000Z","2017-03-04T00:00:00.000Z/2017-03-05T00:00:00.000Z","2017-03-
 *         08T00:00:00.000Z/2017-03-09T00:00:00.000Z"
 *     ],
 *     "uncoveredIntervalsOverflowed": true
 * }
 * Content-Encoding:  gzip
 * Vary:  Accept-Encoding, User-Agent
 * Transfer-Encoding:  chunked
 * Server:  Jetty(9.2.5.v20141112)
 * }
 * </pre>
 *
 * The JSON nesting strategy is to extract status code ("200" in this case) and "X-Druid-Response-Context" and put them
 * along side with JSON response
 *
 */
public interface FullResponseProcessor extends ResponseProcessor {
}
