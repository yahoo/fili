// Copyright 2018 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.exception

import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.LogicalMetricImpl
import com.yahoo.bard.webservice.data.metric.LogicalMetricInfo
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery
import com.yahoo.bard.webservice.data.metric.mappers.NoOpResultSetMapper
import com.yahoo.bard.webservice.web.ErrorMessageFormat
import com.yahoo.bard.webservice.web.RequestValidationException
import com.yahoo.bard.webservice.web.apirequest.MetricsApiRequest
import com.yahoo.bard.webservice.web.apirequest.MetricsApiRequestImpl

import spock.lang.Specification
import spock.lang.Unroll

import javax.ws.rs.core.Response

class FiliMetricExceptionHandlerSpec extends Specification {

    FiliMetricExceptionHandler metricsExceptionHandler = new FiliMetricExceptionHandler()

    LogicalMetric metric = new LogicalMetricImpl(
            Stub(TemplateDruidQuery),
            new NoOpResultSetMapper(),
            new LogicalMetricInfo("metric")
    )

    MetricDictionary dictionary  = new MetricDictionary()

    Optional<MetricsApiRequest> request

    def setup() {
        dictionary.put("metric", metric)
        request = Optional.of(new MetricsApiRequestImpl(
                "metric",
                "json",
                "",
                "",
                dictionary
        ))
    }


    @Unroll
    def "The handler forwards the #status from a RequestValidationException as the status of the request"() {
        given: "The ResponseValidationException to validate"
        RequestValidationException exception = new RequestValidationException(status, "Error", "Error")

        when:
        Response response = metricsExceptionHandler.handleThrowable(exception, request, null)

        then:
        response.status == status.statusCode
        response.entity == "Error"

        where:
        status << [Response.Status.BAD_REQUEST, Response.Status.NOT_IMPLEMENTED]
    }

    def "IOException returns an Internal Server Errror"() {
        given:
        IOException exception = new IOException("IOException")

        when:
        Response response = metricsExceptionHandler.handleThrowable(exception, request, null)

        then:
        response.status == Response.Status.INTERNAL_SERVER_ERROR.statusCode
        response.entity == "Internal server error. IOException : $exception.message"
    }

    def "A Throwable returns a Bad Request"() {
        given:
        Throwable throwable = new Throwable("Throw")

        when:
        Response response = metricsExceptionHandler.handleThrowable(throwable, request, null)

        then:
        response.status == Response.Status.BAD_REQUEST.statusCode
        response.entity == ErrorMessageFormat.REQUEST_PROCESSING_EXCEPTION.format("Throw")
    }
}
