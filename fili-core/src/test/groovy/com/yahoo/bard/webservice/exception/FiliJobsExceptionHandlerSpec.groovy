// Copyright 2018 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.exception

import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.util.JsonSlurper
import com.yahoo.bard.webservice.web.RequestValidationException
import com.yahoo.bard.webservice.web.apirequest.JobsApiRequest
import com.yahoo.bard.webservice.web.apirequest.JobsApiRequestImpl

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import javax.ws.rs.core.Response

class FiliJobsExceptionHandlerSpec extends Specification {

    @Shared
    ObjectMappersSuite objectMappersSuite = new ObjectMappersSuite()

    @Shared
    JsonSlurper json = new JsonSlurper()

    FiliJobsExceptionHandler jobsExceptionHandler = new FiliJobsExceptionHandler(objectMappersSuite)

    Optional<JobsApiRequest> request = Optional.of(
            new JobsApiRequestImpl(
                    "json",
                    "500",
                    "",
                    "",
                    "",
                    null,
                    null,
                    null
            )
    )

    @Unroll
    def "The handler forwards the #status from a RequestValidationException as the status of the request"() {
        given: "The ResponseValidationException to validate"
        RequestValidationException exception = new RequestValidationException(status, "Error", "Error")

        when:
        Response response = jobsExceptionHandler.handleThrowable(exception, request, null)

        then:
        response.status == status.statusCode
        json.parseText(response.entity).description == "Error"

        where:
        status << [Response.Status.BAD_REQUEST, Response.Status.NOT_IMPLEMENTED]
    }

    def "A Throwable returns an Internal Server Error"() {
        given:
        Throwable throwable = new Throwable("Throw")

        when:
        Response response = jobsExceptionHandler.handleThrowable(throwable, request, null)

        then:
        response.status == Response.Status.INTERNAL_SERVER_ERROR.statusCode
        response.entity == "Throw"
    }
}
