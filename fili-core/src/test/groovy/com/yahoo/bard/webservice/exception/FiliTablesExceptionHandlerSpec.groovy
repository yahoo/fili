// Copyright 2018 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.exception

import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.table.TableGroup
import com.yahoo.bard.webservice.web.DefaultResponseFormatType
import com.yahoo.bard.webservice.web.ErrorMessageFormat
import com.yahoo.bard.webservice.web.RequestValidationException
import com.yahoo.bard.webservice.web.apirequest.TablesApiRequest
import com.yahoo.bard.webservice.web.apirequest.TablesApiRequestImpl
import com.yahoo.bard.webservice.web.filters.ApiFilters

import com.fasterxml.jackson.core.JsonProcessingException

import spock.lang.Specification
import spock.lang.Unroll

import javax.ws.rs.core.Response

class FiliTablesExceptionHandlerSpec extends Specification {

    FiliTablesExceptionHandler tablesExceptionHandler = new FiliTablesExceptionHandler()

    Optional<TablesApiRequest> request = Optional.of(new TablesApiRequestImpl(
            DefaultResponseFormatType.JSON,
            Optional.empty(),
            [] as LinkedHashSet,
            new LogicalTable(
                    "table",
                    DefaultTimeGrain.DAY,
                    new TableGroup([] as LinkedHashSet, [] as Set, [] as Set), Stub(MetricDictionary)
            ),
            DefaultTimeGrain.DAY,
            [] as LinkedHashSet,
            [] as LinkedHashSet,
            [], // intervals
            new ApiFilters()
    ))


    @Unroll
    def "The handler forwards the #status from a RequestValidationException as the status of the request"() {
        given: "The ResponseValidationException to validate"
        RequestValidationException exception = new RequestValidationException(status, "Error", "Error")

        when:
        Response response = tablesExceptionHandler.handleThrowable(exception, request, null)

        then:
        response.status == status.statusCode
        response.entity == "Error"

        where:
        status << [Response.Status.BAD_REQUEST, Response.Status.NOT_IMPLEMENTED]
    }

    def "JsonProcessing returns an Internal Server Error"() {
        given:
        JsonProcessingException exception = new JsonProcessingException("JsonError")

        when:
        Response response = tablesExceptionHandler.handleThrowable(exception, request, null)

        then:
        response.status == Response.Status.INTERNAL_SERVER_ERROR.statusCode
        response.entity == ErrorMessageFormat.INTERNAL_SERVER_ERROR_ON_JSON_PROCESSING.format("JsonError")
    }

    def "A Throwable returns a Bad Request"() {
        given:
        Throwable throwable = new Throwable("Throw")

        when:
        Response response = tablesExceptionHandler.handleThrowable(throwable, request, null)

        then:
        response.status == Response.Status.BAD_REQUEST.statusCode
        response.entity == ErrorMessageFormat.REQUEST_PROCESSING_EXCEPTION.format("Throw")
    }
}
