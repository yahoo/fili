// Copyright 2018 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.exception

import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.data.dimension.TimeoutException
import com.yahoo.bard.webservice.table.resolver.NoMatchFoundException
import com.yahoo.bard.webservice.util.JsonSlurper
import com.yahoo.bard.webservice.web.RequestValidationException

import com.fasterxml.jackson.databind.ObjectWriter

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import javax.ws.rs.container.AsyncResponse
import javax.ws.rs.container.ContainerRequestContext
import javax.ws.rs.core.Response
import javax.ws.rs.core.UriInfo

class FiliDataExceptionHandlerSpec extends Specification {

    @Shared
    ObjectMappersSuite MAPPERS = new ObjectMappersSuite()
    @Shared
    ObjectWriter writer = MAPPERS.getMapper().writer()
    @Shared
    JsonSlurper json = new JsonSlurper()

    AsyncResponse response = Mock(AsyncResponse)
    ContainerRequestContext containerRequestContext = Mock(ContainerRequestContext)

    FiliDataExceptionHandler dataExceptionHandler = new FiliDataExceptionHandler()

    def setup() {
        UriInfo uriInfo = Mock(UriInfo)
        URI uri = new URI("http://fakeUri.com")
        uriInfo.getRequestUri() >> uri
        containerRequestContext.getUriInfo() >> uriInfo;
    }

    @Unroll
    def "The handler forwards the #status from a RequestValidationException as the status of the request"() {
        given: "The ResponseValidationException to validate"
        RequestValidationException exception = new RequestValidationException(
                status,
                "Error",
                "Error"
        )
        and: "A mock AsyncResponse to resume"

        when: "We handle the exception"
        dataExceptionHandler.handleThrowable(exception, response, Optional.empty(), containerRequestContext,  writer)

        then: "The response is resumed"
        1 * response.resume(_) >> {
            assert it[0].status == status.statusCode
            assert json.parseText(it[0].entity).description == "Error"
            return true
        }
        where:
        status << [Response.Status.BAD_REQUEST, Response.Status.NOT_IMPLEMENTED]
    }

    def "NoMatchFoundException returns an Internal Server Error"() {
        given:
        NoMatchFoundException exception = new NoMatchFoundException("NoMatch")

        when:
        dataExceptionHandler.handleThrowable(exception, response, Optional.empty(), containerRequestContext,  writer)

        then:
        1 * response.resume(_) >> {
            assert it[0].status == Response.Status.INTERNAL_SERVER_ERROR.statusCode
            assert json.parseText(it[0].entity).description == "NoMatch"
            return true
        }
    }

    def "TimeoutException returns a Gateway Timeout"() {
        given:
        TimeoutException exception = new TimeoutException("Timeout")

        when:
        dataExceptionHandler.handleThrowable(exception, response, Optional.empty(), containerRequestContext,  writer)

        then:
        1 * response.resume(_) >> {
            assert it[0].status == Response.Status.GATEWAY_TIMEOUT.statusCode
            assert json.parseText(it[0].entity).description == "Timeout"
            return true
        }
    }

    def "A Throwable returns a Bad Request"() {
        given:
        Throwable throwable = new Throwable("Throw")

        when:
        dataExceptionHandler.handleThrowable(throwable, response, Optional.empty(), containerRequestContext,  writer)

        then:
        1 * response.resume(_) >> {
            assert it[0].status == Response.Status.BAD_REQUEST.statusCode
            assert json.parseText(it[0].entity).description == "Throw"
            return true
        }
    }
}
