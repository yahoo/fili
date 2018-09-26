// Copyright 2018 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.exception

import com.yahoo.bard.webservice.web.RequestValidationException
import com.yahoo.bard.webservice.web.apirequest.SlicesApiRequest

import spock.lang.Specification
import spock.lang.Unroll

import javax.ws.rs.core.Response


class FiliSlicesExceptionHandlerSpec extends Specification {

    FiliSlicesExceptionHandler slicesExceptionHandler = new FiliSlicesExceptionHandler()

    Optional<SlicesApiRequest> request = Optional.of(Stub(SlicesApiRequest))

    @Unroll
    def "The handler forwards the #status from a RequestValidationException as the status of the request"() {
        given: "The ResponseValidationException to validate"
        RequestValidationException exception = new RequestValidationException(status, "Error", "Error")

        when:
        Response response = slicesExceptionHandler.handleThrowable(exception, request, null)

        then:
        response.status == status.statusCode
        response.entity == "Error"

        where:
        status << [Response.Status.BAD_REQUEST, Response.Status.NOT_IMPLEMENTED]
    }

    def "IOException returns an Internal Server Error"() {
        given:
        IOException exception = new IOException("IOException")

        when:
        Response response = slicesExceptionHandler.handleThrowable(exception, request, null)

        then:
        response.status == Response.Status.INTERNAL_SERVER_ERROR.statusCode
        response.entity == "Internal server error. IOException : $exception.message"
    }

    def "A Throwable returns an Internal Server Error"() {
        given:
        Throwable throwable = new Throwable("Throw")

        when:
        Response response = slicesExceptionHandler.handleThrowable(throwable, request, null)

        then:
        response.status == Response.Status.INTERNAL_SERVER_ERROR.statusCode
        response.entity == "Throw"
    }
}
