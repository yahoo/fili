// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.responseprocessors

import static com.yahoo.bard.webservice.web.handlers.workflow.DruidWorkflow.REQUEST_WORKFLOW_TIMER

import com.yahoo.bard.webservice.druid.client.FailureCallback
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback
import com.yahoo.bard.webservice.druid.model.query.GroupByQuery
import com.yahoo.bard.webservice.logging.RequestLog

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory

import spock.lang.Specification

class WeightCheckResponseProcessorSpec extends Specification{

    ResponseProcessor next = Mock(ResponseProcessor)
    GroupByQuery groupByQuery = Mock(GroupByQuery)

    FailureCallback nextFail = Mock(FailureCallback)
    HttpErrorCallback nextError = Mock(HttpErrorCallback)
    WeightCheckResponseProcessor wcrp = new WeightCheckResponseProcessor(next)

    JsonNodeFactory jsonFactory = new JsonNodeFactory()
    JsonNode json = jsonFactory.arrayNode()

    def setup() {
        // Artificial starting of the request workflow timer before each test
        RequestLog.dump()
        RequestLog.startTiming(REQUEST_WORKFLOW_TIMER)
    }

    def "Test Constructor"() {
        expect:
        wcrp.next == next
    }

    def "Test that REQUEST_WORKFLOW_TIMER is not stopped by processResponse"() {
        when: "no errors have occurred"
        wcrp.processResponse(json, groupByQuery, null)

        then: "no timer is stopped and proccessing continues to the next processor"
        // This check is artifial. Is meant to check that there is no call to stopTiming() for this timer in the
        // regular execution path of the WeightCheckResponseProcessor
        RequestLog.isStarted(REQUEST_WORKFLOW_TIMER) == true
        1 * next.processResponse(json, groupByQuery, null)

    }

    def "Test that REQUEST_WORKFLOW_TIMER is stopped when invoking the failure callback"() {
        setup:
        Throwable t = new Throwable("foo")

        when: "Failure occurs"
        wcrp.getFailureCallback(groupByQuery).invoke(t)

        then: "The REQUEST_WORKFLOW_TIMER is stopped"
        RequestLog.isStarted(REQUEST_WORKFLOW_TIMER) == false

        then: "and the failure callback of the next processor is called"
        1 * next.getFailureCallback(groupByQuery) >> nextFail
        1 * nextFail.invoke(t)
    }

    def "Test that REQUEST_WORKFLOW_TIMER is stopped when invoking the http error callback"() {
        setup:
        int statusCode = 50
        String reason = "reason"
        String body = "body"
        FailureCallback fc = Mock(FailureCallback)
        HttpErrorCallback ec
        RequestLog.startTiming(REQUEST_WORKFLOW_TIMER)

        when: "Http error occurs"
        wcrp.getErrorCallback(groupByQuery).invoke(statusCode, reason, body)

        then: "The REQUEST_WORKFLOW_TIMER is stopped"
        RequestLog.isStarted(REQUEST_WORKFLOW_TIMER) == false

        then: "and the http error callback of the next processor is called"
        1 * next.getErrorCallback(groupByQuery) >> nextError
        1 * nextError.invoke(statusCode, reason, body)
    }
}
