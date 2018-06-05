// Copyright 2018 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.exception

import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.web.ErrorMessageFormat
import com.yahoo.bard.webservice.web.RequestValidationException
import com.yahoo.bard.webservice.web.ResponseCode
import com.yahoo.bard.webservice.web.RowLimitReachedException
import com.yahoo.bard.webservice.web.apirequest.DimensionsApiRequest
import com.yahoo.bard.webservice.web.apirequest.DimensionsApiRequestImpl

import com.fasterxml.jackson.core.JsonProcessingException

import spock.lang.Specification
import spock.lang.Unroll

import javax.ws.rs.core.Response

class FiliDimensionExceptionHandlerSpec extends Specification {

    FiliDimensionExceptionHandler dimensionExceptionHandler = new FiliDimensionExceptionHandler()

    Dimension dimension = Stub(Dimension) {
        getApiName() >> "dimension"
        toString() >> "dimension"
    }

    Optional<DimensionsApiRequest> request = Optional.of(new DimensionsApiRequestImpl(
            "dimension",
            "",
            "json",
            "",
            "",
            new DimensionDictionary([dimension] as Set)
    ))

    @Unroll
    def "The handler forwards the #status from a RequestValidationException as the status of the request"() {
        given: "The ResponseValidationException to validate"
        RequestValidationException exception = new RequestValidationException(status, "Error", "Error")

        when:
        Response response = dimensionExceptionHandler.handleThrowable(exception, request, null)

        then:
        response.status == status.statusCode
        response.entity == "Error"

        where:
        status << [Response.Status.BAD_REQUEST, Response.Status.NOT_IMPLEMENTED]
    }

    def "RowLimitReached returns an Insufficient Storage"() {
        given:
        RowLimitReachedException exception = new RowLimitReachedException("RowLimit")

        when:
        Response response = dimensionExceptionHandler.handleThrowable(exception, request, null)

        then:
        response.status == ResponseCode.INSUFFICIENT_STORAGE.statusCode
        response.entity == "Row limit exceeded for dimension dimension: RowLimit"
    }

    def "JsonProcessing returns an Internal Server Error"() {
        given:
        JsonProcessingException exception = new JsonProcessingException("JsonError")

        when:
        Response response = dimensionExceptionHandler.handleThrowable(exception, request, null)

        then:
        response.status == Response.Status.INTERNAL_SERVER_ERROR.statusCode
        response.entity == ErrorMessageFormat.INTERNAL_SERVER_ERROR_ON_JSON_PROCESSING.format("JsonError")
    }

    def "A Throwable returns a Bad Request"() {
        given:
        Throwable throwable = new Throwable("Throw")

        when:
        Response response = dimensionExceptionHandler.handleThrowable(throwable, request, null)

        then:
        response.status == Response.Status.BAD_REQUEST.statusCode
        response.entity == ErrorMessageFormat.REQUEST_PROCESSING_EXCEPTION.format("Throw")
    }
}
