// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.responseprocessors

import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.data.cache.HashDataCache.Pair
import com.yahoo.bard.webservice.druid.client.FailureCallback
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback
import com.yahoo.bard.webservice.druid.model.query.GroupByQuery
import com.yahoo.bard.webservice.logging.RequestLog
import com.yahoo.bard.webservice.web.DataApiRequest

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper

import org.joda.time.DateTimeZone
import org.joda.time.Interval

import spock.lang.Specification

import java.util.concurrent.atomic.AtomicInteger

public class SplitQueryResponseProcessorSpec extends Specification {
    private static final ObjectMapper MAPPER = new ObjectMappersSuite().getMapper()

    SplitQueryResponseProcessor sqrp
    DataApiRequest apiRequest = Mock(DataApiRequest)

    private final ResponseProcessor next = Mock(ResponseProcessor)
    GroupByQuery groupByQuery1 = Mock(GroupByQuery)
    GroupByQuery groupByQuery2 = Mock(GroupByQuery)

    Map<Interval, Integer> expectedIntervals = new LinkedHashMap<>()
    List<Pair<JsonNode, LoggingContext>> completedIntervals = new ArrayList<>()

    String json1 = """ [ {"cow": 1}, {"dog": 2} ]"""
    String json2 = """ [ {"elephant": 3}, {"pig": 4} ] """
    String merged = """ [ {"cow": 1}, {"dog": 2}, {"elephant": 3}, {"pig": 4} ] """

    JsonNode node1 = MAPPER.readValue(json1, JsonNode.class)
    JsonNode node2 = MAPPER.readValue(json2, JsonNode.class)
    JsonNode nodeExpected = MAPPER.readValue(merged, JsonNode.class)

    Interval interval1 = new Interval(0, 1)
    Interval interval2 = new Interval(2, 4)

    FailureCallback nextFail = Mock(FailureCallback)

    def setup() {
        apiRequest.getTimeZone() >> DateTimeZone.UTC
        expectedIntervals.put(interval1, new AtomicInteger(0))
        expectedIntervals.put(interval2, new AtomicInteger(1))
        sqrp = new SplitQueryResponseProcessor(
                next,
                apiRequest,
                groupByQuery1,
                expectedIntervals,
                RequestLog.dump()
        )
    }

    def "Test constructor "() {
        expect:
        sqrp.next == next
        sqrp.queryBeforeSplit == groupByQuery1
        sqrp.expectedIntervals == expectedIntervals
        sqrp.completedIntervals instanceof List
        sqrp.completedIntervals.size() == 2
    }


    def "Test get response context delegates"() {
        when:
        sqrp.getResponseContext()

        then:
        1 * next.getResponseContext()
    }

    def "Test create failure callback"() {
        setup:
        FailureCallback fc
        Throwable t = new Throwable("foo")
        when:
        fc = sqrp.getFailureCallback(groupByQuery2)
        fc.invoke(t)

        then:
        1 * next.getFailureCallback(groupByQuery2) >> nextFail
        1 * nextFail.invoke(t)
        sqrp.failed.get()
    }

    def "Test create http errror callback"() {
        setup:
        HttpErrorCallback ec
        int statusCode = 50
        String reason = "reason"
        String body = "body"

        HttpErrorCallback nextError = Mock(HttpErrorCallback)
        when:
        ec = sqrp.getErrorCallback(groupByQuery2)
        ec.invoke(statusCode, reason, body)

        then:
        1 * next.getErrorCallback(groupByQuery2) >> nextError
        1 * nextError.invoke(statusCode, reason, body)
        sqrp.failed.get()
    }

    def "Test stitch Json"() {
        setup:
        completedIntervals.add(new Pair<>(node1, new LoggingContext(RequestLog.dump())))
        completedIntervals.add(new Pair<>(node2, new LoggingContext(RequestLog.dump())))

        when:
        Pair<JsonNode, LoggingContext> result = sqrp.mergeResponses(completedIntervals)

        then:
        result.getKey().equals(nodeExpected)
    }

    def "Test process response with good Data"() {
        setup:
        groupByQuery2.getIntervals() >> [interval1] >> [interval2]

        when:
        sqrp.processResponse(node1, groupByQuery2, new LoggingContext(RequestLog.dump()))

        then:
        sqrp.completed.get() == 1
        !sqrp.failed.get()
        sqrp.completedIntervals.size() == 2
        0 * next.processResponse(_, _, _)

        when:
        sqrp.processResponse(node2, groupByQuery2, new LoggingContext(RequestLog.dump()))
        then:
        sqrp.completed.get() == 0
        !sqrp.failed.get()
        1 * next.processResponse(nodeExpected, groupByQuery1, _)
    }

    def "Test error on response with unexpected data and fails after"() {
        setup:
        Interval i = new Interval(5, 10)
        groupByQuery2.getIntervals() >> [i]
        String expectedError = String.format(SplitQueryResponseProcessor.UNEXPECTED_INTERVAL_FORMAT, i)
        Throwable captureT = null

        when:
        sqrp.processResponse(node1, groupByQuery2, null)

        then:
        sqrp.completed.get() > 0
        sqrp.failed.get()
        sqrp.completedIntervals.size() == 2
        0 * next.processResponse(_, _, _)
        1 * next.getFailureCallback(groupByQuery2) >> nextFail
        1 * nextFail.invoke() { it -> captureT = it }
        captureT.getMessage() == expectedError

        when:
        sqrp.processResponse(node1, groupByQuery2, null)

        then:
        // No other mocks interact
        0 * _._
    }

    def "Test process response with too much Data"() {
        setup:
        groupByQuery2.getIntervals() >> [interval1] >> [interval1]
        String expectedError = String.format(SplitQueryResponseProcessor.EXTRA_RETURN_FORMAT, interval1)
        Throwable captureT = null

        when:
        sqrp.processResponse(node1, groupByQuery2, null)
        sqrp.processResponse(node2, groupByQuery2, null)

        then:
        sqrp.completed.get() > 0
        sqrp.failed.get()
        sqrp.completedIntervals.size() == 2
        0 * next.processResponse(_, _, _)
        1 * next.getFailureCallback(groupByQuery2) >> nextFail
        1 * nextFail.invoke() { it -> captureT = it }
        captureT.getMessage() == expectedError
    }
}
