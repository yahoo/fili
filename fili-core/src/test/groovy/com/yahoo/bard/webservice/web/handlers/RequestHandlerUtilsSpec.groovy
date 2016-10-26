// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers

import com.yahoo.bard.webservice.druid.model.query.GroupByQuery
import com.yahoo.bard.webservice.util.GroovyTestUtils
import com.yahoo.bard.webservice.util.JsonSlurper
import com.yahoo.bard.webservice.web.RequestUtils

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.ObjectWriter
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module

import spock.lang.Specification
import spock.lang.Unroll

import javax.ws.rs.ProcessingException
import javax.ws.rs.core.Response

class RequestHandlerUtilsSpec extends Specification {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new Jdk8Module().configureAbsentsAsNulls(false))

    ObjectWriter writer = MAPPER.writer()

    GroupByQuery groupByQuery = RequestUtils.buildGroupByQuery()
    String groupByQueryJson = RequestUtils.defaultQueryJson()

    int status = 5
    String statusName = "5"
    String reason = "Default reason"
    String description = "Default description"
    String expectedDocument


    def setup() {
        expectedDocument = """ {
                "status" : $status,
                "statusName": "$statusName",
                "reason": "$reason",
                "description": "$description",
                "druidQuery" : $groupByQueryJson,
                "requestId": "SOME UUID"
        }
        """
    }

    def "Test error response simple"() {
        when:
        Response response = RequestHandlerUtils.makeErrorResponse(
                status,
                reason,
                description,
                groupByQuery,
                writer
        )

        then:
        response.status == status
        GroovyTestUtils.compareErrorPayload(response.entity, expectedDocument)
    }

    def "Test error response status error"() {
        setup:
        String expectedStatusName = "Bad Request"
        Response.StatusType st = Response.Status.BAD_REQUEST
        String expectedDocument = """{
            "status" : ${st.getStatusCode()},
            "statusName": "${expectedStatusName}",
            "reason": "java.lang.Throwable",
            "description": "$description",
            "druidQuery" : null,
            "requestId": "SOME UUID"
        }
        """
        when:
        javax.ws.rs.core.Response response = RequestHandlerUtils.makeErrorResponse(
                st,
                new Throwable(description),
                writer
        )

        then:
        response.status == st.getStatusCode()
        GroovyTestUtils.compareErrorPayload(response.entity, expectedDocument)
    }

    @Unroll
    def "Test make error response statuses"() {
        when:

        String expectedDocument = """{
            "status" : ${expectedStatus},
            "statusName": "${expectedStatusName}",
            "reason": "$reason",
            "description": "$description",
            "druidQuery" : $groupByQueryJson,
            "requestId": "SOME UUID"
        }
        """
        Response response = RequestHandlerUtils.makeErrorResponse(
                status,
                reason,
                description,
                groupByQuery,
                writer
        )

        then:
        response.status == expectedStatus
        GroovyTestUtils.compareErrorPayload(response.entity, expectedDocument)

        where:
        status | expectedStatus | expectedStatusName
        100    | 100            | "100"
        200    | 200            | "OK"
        300    | 500            | "Internal Server Error"
        400    | 400            | "Bad Request"
        404    | 404            | "Not Found"
        419    | 419            | "419"
        499    | 499            | "499"
        500    | 500            | "Internal Server Error"
    }

    def "Test make error response serialize error"() {
        setup:
        String exceptionText = "Bad Text"
        ObjectWriter writer = Mock(ObjectWriter)
        writer.withDefaultPrettyPrinter() >> writer
        writer.writeValueAsString(_) >> { throw new JsonProcessingException(exceptionText) }

        when:
        Response response = RequestHandlerUtils.makeErrorResponse(
                status,
                reason,
                description,
                groupByQuery,
                writer
        )

        then:
        response.status == status
        response.entity == exceptionText
    }

    @Unroll
    def "Test make error response from throwable"() {
        setup:
        Response.Status statusType = Response.Status.OK
        int status = statusType.getStatusCode()
        String statusName = statusType.getReasonPhrase()

        if (reason != null) {
            reason = "\"$reason\""
        }
        if (description != null) {
            description = "\"$description\""
        }

        expectedDocument = """
        {
            "status" : $status,
            "statusName": "$statusName",
            "reason": $reason,
            "description": $description,
            "druidQuery" : $groupByQueryJson,
            "requestId": "SOME UUID"
        }
        """

        when:
        Response response = RequestHandlerUtils.makeErrorResponse(
                statusType,
                groupByQuery,
                cause,
                writer
        )

        then:
        response.status == status
        GroovyTestUtils.compareErrorPayload(response.entity, expectedDocument)

        //@formatter:off
        where:
        cause                                                    | reason                              | description
        new Exception("Test")                                    | Exception.class.getName()           | "Test"
        new Exception(new Exception("Test"))                     | Exception.class.getName()           | "java.lang" + ".Exception: Test"
        null                                                     | null                                | null
        new ProcessingException("Test")                          | ProcessingException.class.getName() | "Test"
        new ProcessingException("Test1", new Exception("Test2")) | Exception.class.getName()           | "Test2"
        //@formatter:on

    }

    @Unroll
    def "Test make error response from throwable without groupBy"() {
        setup:
        Response.Status statusType = Response.Status.OK
        int status = statusType.getStatusCode()
        String statusName = statusType.getReasonPhrase()

        if (reason != null) {
            reason = "\"$reason\""
        }
        if (description != null) {
            description = "\"$description\""
        }

        expectedDocument = """
        {
            "status" : $status,
            "statusName": "$statusName",
            "reason": $reason,
            "description": $description,
            "druidQuery" : $groupByQueryJson,
            "requestId": "SOME UUID"
        }
        """

        when:
        Response response = RequestHandlerUtils.makeErrorResponse(
                statusType,
                groupByQuery,
                cause,
                writer
        )

        then:
        response.status == status
        GroovyTestUtils.compareErrorPayload(response.entity, expectedDocument)

        //@formatter:off
        where:
        cause                                                    | reason                              | description
        new Exception("Test")                                    | Exception.class.getName()           | "Test"
        new Exception(new Exception("Test"))                     | Exception.class.getName()           | "java.lang.Exception: Test"
        null                                                     | null                                | null
        new ProcessingException("Test")                          | ProcessingException.class.getName() | "Test"
        new ProcessingException("Test1", new Exception("Test2")) | Exception.class.getName()           | "Test2"
        //@formatter:on

    }
}
