// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.filters

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.logging.TestLogAppender
import com.yahoo.bard.webservice.web.LoggingTestUtils
import com.yahoo.bard.webservice.web.endpoints.TestFilterServlet

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Timeout

import javax.ws.rs.client.Entity
import javax.ws.rs.core.Response

/**
 * Uses TestFilterServlet to validate BardLoggingFilter works correctly
 */
@Timeout(30)
// Fail test if hangs
class LogFilterSpec extends Specification {
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
        jtb = new JerseyTestBinder(BardLoggingFilter.class, TestFilterServlet.class)
        logAppender.clear()
    }

    def cleanup() {
        jtb.tearDown()
        // Cleanup appender after each test
        logAppender.clear()
    }

    def "log Options"() {
        /*
            Request: OPTIONS, length=-1
            http://localhost:9998/test/data?metrics=pageViews&dateTime=2014-06-11%2F2014-06-12
            > user-agent=Jersey/2.9 (HttpUrlConnection 1.7.0_55)
            > host=localhost:9998
            > accept=text/html, image/gif, image/jpeg, *; q=.2
            > connection=keep-alive
            < Allow=POST,GET,OPTIONS,HEAD
            < Content-Length=0
            < Content-Type=text/html
            Response: 200, OK, 14.488 ms
            length=0
         */
        when:
        Response r = jtb.getHarness().target("test/data")
                .queryParam("metrics", "pageViews")
                .queryParam("dateTime", "2014-06-11%2F2014-06-12")
                .request().options()

        Map res = LoggingTestUtils.extractResultsFromLogs(logAppender, MAPPER)

        then: "OPTIONS passes"
        r.status == 200

        and: "Message logs correctly"
        res.method == '"OPTIONS"'
        res.status == '"OK"'
        res.code == '200'
        res.logMessage == '"Successful request"'
    }

    def "log Get"() {
        /*
            Request: GET, length=-1
            http://localhost:9998/test/data?metrics=pageViews&dateTime=2014-06-11%2F2014-06-12
            > user-agent=Jersey/2.9 (HttpUrlConnection 1.7.0_55)
            > host=localhost:9998
            > accept=text/html, image/gif, image/jpeg, *; q=.2,
            > connection=keep-alive
            < Content-Type=application/json
            Response: 200, OK, 1020.253 ms
            length=2
         */
        when: "make GET request"
        jtb.getHarness().target("test/data")
                .queryParam("metrics", "pageViews")
                .queryParam("dateTime", "2014-06-11%2F2014-06-12")
                .request().get(String.class)

        Map res = LoggingTestUtils.extractResultsFromLogs(logAppender, MAPPER)

        then: "Message logs correctly"
        res.method == '"GET"'
        res.status == '"OK"'
        res.code == '200'
        res.logMessage == '"Successful request"'
    }

    def "log Post"() {
        /*
            Request: POST, length=7
            http://localhost:9998/test/data
            > content-type=application/json
            > user-agent=Jersey/2.9 (HttpUrlConnection 1.7.0_55)
            > host=localhost:9998
            > accept=text/html, image/gif, image/jpeg, *; q=.2
            > connection=keep-alive
            > content-length=7
            < Content-Type=application/json
            Response: 200, OK, 18.642 ms
            length=65536
         */
        given: "Some test JSON request"
        String inputJson = MAPPER.writer().writeValueAsString([1, 2, 3])

        when: "make POST request"
        String s = jtb.getHarness().target("test/data").request().post(Entity.json(inputJson), String.class)
        Map res = LoggingTestUtils.extractResultsFromLogs(logAppender, MAPPER)

        then: "POST returns test data"
        s.startsWith("ABCDEF") == true

        and: "Message logs correctly"
        res.method == '"POST"'
        res.status == '"OK"'
        res.code == '200'
        res.logMessage == '"Successful request"'
    }

    def "log Put"() {
        /*
            Request: PUT, length=11
            http://localhost:9998/test/data
            > content-type=application/json
            > user-agent=Jersey/2.9 (HttpUrlConnection 1.7.0_55)
            > host=localhost:9998
            > accept=text/html, image/gif, image/jpeg, *; q=.2
            > connection=keep-alive
            > content-length=11
            < Allow=POST,GET,OPTIONS
            Response: 405, Method Not Allowed, 1.114 ms
            length=-1
         */
        given:
        String inputJson = MAPPER.writer().writeValueAsString([1, 2, 3, 4, 5])

        when: "PUT request"
        Response r = jtb.getHarness().target("test/data").request().put(Entity.json(inputJson))
        Map res = LoggingTestUtils.extractResultsFromLogs(logAppender, MAPPER)

        then: "PUT request fails"
        r.status == 405

        and: "Message logs correctly"
        res.method == '"PUT"'
        res.status == '"Method Not Allowed"'
        res.code == '405'
        res.logMessage == '"Request without entity failed"'
    }

    def "client reset by peer still logs"() {
        when:
        URL url = new URL("http://localhost:9998/test/fail")
        HttpURLConnection con = url.openConnection()
        con.setUseCaches(false)
        con.setInstanceFollowRedirects(false)

        then: "Jersey will fail"
        con.getResponseCode() == 500
        Map res = LoggingTestUtils.extractResultsFromLogs(logAppender, MAPPER)

        and: "Request still logs"
        res.method == '"GET"'
        res.status == '"OK"'
        res.code == '200'
        res.logMessage == '"Successful request"'
    }
}
