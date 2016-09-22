// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.exceptionmappers

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.logging.TestLogAppender
import com.yahoo.bard.webservice.web.endpoints.TestLoggingServlet
import com.yahoo.bard.webservice.web.filters.BardLoggingFilter

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Timeout

import javax.ws.rs.core.Response

/**
 * Tests simple initialization of the outmost timing wrapper and the mega log line
 */
@Timeout(30)
// Fail test if hangs
class LogExceptionMapperSpec extends Specification {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new Jdk8Module().configureAbsentsAsNulls(false))

    @Shared
    TestLogAppender logAppender

    def setupSpec() {
        // Hook with test appender
        logAppender = new TestLogAppender()
    }

    def cleanupSpec() {
        logAppender.close()
    }

    JerseyTestBinder jtb

    def setup() {
        // Create the test web container to test the resources
        jtb = new JerseyTestBinder(BardLoggingFilter.class, LogExceptionMapper.class, TestLoggingServlet.class)
        logAppender.clear()
    }

    def cleanup() {
        jtb.tearDown()
        // Cleanup appender after each test
        logAppender.clear()
    }

    def extractResultsFromLogs() {
        String method
        String status
        String code
        String logMsg
        for (String logLine : logAppender.getMessages()) {
            if (logLine.contains("\"uuid\"")) {
                JsonNode json = MAPPER.readValue(logLine, JsonNode.class)
                method = json.findValues("method").get(0).toString()
                status = json.findValues("status").get(0).toString()
                code = json.findValues("code").get(0).toString()
                logMsg = json.findValues("logMessage").get(0).toString();
                return [method: method, status: status, code: code, logMsg: logMsg]
            }
        }
    }

    def "Check successful GET request"() {
        when: "An a servlet is hit that behaves correctly"
        jtb.getHarness().target("test/log")
                .queryParam("metrics", "pageViews")
                .queryParam("dateTime", "2014-06-11%2F2014-06-12")
                .request().get(Response.class)

        Map res = extractResultsFromLogs()

        then: "One log line is produced reflecting a successful request"
        res.method == '"GET"'
        res.status == '"OK"'
        res.code == '200'
        res.logMsg == '"Successful request"'
    }

    def "Check GET request that throws WebApplicationException"() {
        when: "A servlet is hit that throws a webapp exception"
        jtb.getHarness().target("test/webbug")
                .queryParam("metrics", "pageViews")
                .queryParam("dateTime", "2014-06-11%2F2014-06-12")
                .request().get(Response.class)

        Map res = extractResultsFromLogs()

        then: "One log line is produced reflecting the error status caused by the exception"
        res.method == '"GET"'
        res.status == '"Internal Server Error"'
        res.code == '500'
        res.logMsg == '"Oops! Web App Exception"'
    }

    def "Check GET request that throws RuntimeException"() {
        when: "A servlet is hit that throws a generic runtime exception"
        jtb.getHarness().target("test/genericbug")
                .queryParam("metrics", "pageViews")
                .queryParam("dateTime", "2014-06-11%2F2014-06-12")
                .request().get(Response.class)

        Map res = extractResultsFromLogs()

        then: "One log line is produced reflecting the error status caused by the exception"
        res.method == '"GET"'
        res.status == '"Internal Server Error"'
        res.code == '500'
        res.logMsg == '"Oops! Generic Exception"'
    }
}
