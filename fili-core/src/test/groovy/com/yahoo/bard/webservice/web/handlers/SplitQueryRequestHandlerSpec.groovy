// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.MONTH

import com.yahoo.bard.webservice.druid.client.HttpErrorCallback
import com.yahoo.bard.webservice.druid.model.query.GroupByQuery
import com.yahoo.bard.webservice.web.DataApiRequest
import com.yahoo.bard.webservice.web.responseprocessors.ResponseProcessor
import com.yahoo.bard.webservice.web.responseprocessors.SplitQueryResponseProcessor

import org.joda.time.DateTime
import org.joda.time.Duration
import org.joda.time.Interval

import spock.lang.Specification
import spock.lang.Unroll

import java.util.concurrent.atomic.AtomicLong

class SplitQueryRequestHandlerSpec extends Specification {

    DataRequestHandler next = Mock(DataRequestHandler)

    RequestContext rc = Mock(RequestContext)
    DataApiRequest apiRequest = Mock(DataApiRequest)
    GroupByQuery groupByQuery = Mock(GroupByQuery)
    GroupByQuery groupByQuerySplit = Mock(GroupByQuery)
    ResponseProcessor response = Mock(ResponseProcessor)

    SplitQueryRequestHandler handler = new SplitQueryRequestHandler(next)

    static DateTime startInstant = new DateTime(2015, 1, 1, 0, 0)

    static Interval week = new Interval(startInstant, Duration.standardDays(7))
    static Interval month = new Interval(startInstant, Duration.standardDays(31))
    static Interval year = new Interval(startInstant, new DateTime(2016, 1, 1, 0, 0))


    GroupByQuery query

    def setup() {
        groupByQuery.getInnermostQuery() >> groupByQuery
        groupByQuerySplit.getInnermostQuery() >> groupByQuerySplit
    }

    @Unroll
    def "Handler splits an interval by a time grain"() {
        groupByQuery.granularity >> timeGrain
        groupByQuery.intervals >> [interval]
        rc.numberOfIncoming >> new AtomicLong(1)
        rc.numberOfOutgoing >> new AtomicLong(1)

        when:
        handler.handleRequest(rc, apiRequest, groupByQuery, response)

        then:
        (intervals) * groupByQuery.withAllIntervals(_) >> groupByQuerySplit
        (intervals) * next.handleRequest(rc, apiRequest, groupByQuerySplit, _ as SplitQueryResponseProcessor)

        where:
        intervals | timeGrain | interval
        7         | DAY       | week
        31        | DAY       | month
        1         | MONTH     | month
        12        | MONTH     | year
    }

    def "Handler sends error on no duration request"() {
        setup:
        Interval none = new Interval(startInstant, startInstant)
        HttpErrorCallback hec = Mock(HttpErrorCallback)

        when:
        handler.handleRequest(rc, apiRequest, groupByQuery, response)

        then:
        groupByQuery.granularity >> DAY
        groupByQuery.intervals >> [none]
        response.getErrorCallback(groupByQuery) >> hec
        apiRequest.getIntervals() >> [none]
        1 * hec.dispatch(400, _ as String, _ as String)
        0 * _._
    }
}
