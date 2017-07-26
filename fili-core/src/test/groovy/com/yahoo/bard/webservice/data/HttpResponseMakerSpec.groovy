// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.web.responseprocessors.ResponseContextKeys.API_METRIC_COLUMN_NAMES
import static com.yahoo.bard.webservice.web.responseprocessors.ResponseContextKeys.REQUESTED_API_DIMENSION_FIELDS

import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.MetricColumn
import com.yahoo.bard.webservice.data.metric.mappers.ResultSetMapper
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery
import com.yahoo.bard.webservice.web.DataApiRequest
import com.yahoo.bard.webservice.web.PreResponse
import com.yahoo.bard.webservice.web.ResponseFormatType
import com.yahoo.bard.webservice.web.responseprocessors.ResponseContext
import com.yahoo.bard.webservice.web.responseprocessors.ResultSetResponseProcessor

import rx.subjects.PublishSubject
import rx.subjects.Subject
import spock.lang.Specification

import javax.ws.rs.container.AsyncResponse
import javax.ws.rs.core.HttpHeaders
import javax.ws.rs.core.MultivaluedMap
import javax.ws.rs.core.PathSegment
import javax.ws.rs.core.Response
import javax.ws.rs.core.UriInfo

class HttpResponseMakerSpec extends Specification {
    private static final ObjectMappersSuite MAPPERS = new ObjectMappersSuite()

    DataApiRequest apiRequest
    HttpResponseChannel httpResponseChannel
    DruidResponseParser druidResponseParser
    UriInfo uriInfo
    PathSegment pathSegment
    MultivaluedMap paramMap
    ResponseContext responseContext
    ResultSet resultSet
    ResultSetResponseProcessor resultSetResponseProcessor
    Subject<PreResponse, PreResponse> responseEmitter
    HttpResponseMaker httpResponseMaker

    def setup() {
        apiRequest = Mock(DataApiRequest)
        druidResponseParser = Mock(DruidResponseParser)
        uriInfo = Mock(UriInfo)
        pathSegment = Mock(PathSegment)
        paramMap = Mock(MultivaluedMap)
        DimensionDictionary dimensionDictionary = Mock(DimensionDictionary)
        Dimension dim1 = Mock(Dimension)

        httpResponseChannel = new HttpResponseChannel(Mock(AsyncResponse), apiRequest, new HttpResponseMaker(MAPPERS, dimensionDictionary))
        responseEmitter = PublishSubject.create()
        responseEmitter.subscribe(httpResponseChannel)

        responseContext = new ResponseContext(apiRequest.dimensionFields)

        paramMap.getFirst("dateTime") >> "a/b,c/d"
        pathSegment.getPath() >> "theMockPath"
        uriInfo.getPathSegments() >>[pathSegment]
        uriInfo.getQueryParameters() >> paramMap

        dimensionDictionary.findByApiName("dim1") >> dim1

        ResultSetMapper resultSetMapper = Mock(ResultSetMapper)
        LogicalMetric logicalMetric = Mock(LogicalMetric)
        logicalMetric.getCalculation() >> resultSetMapper
        Set<LogicalMetric> metricSet = [logicalMetric] as Set

        apiRequest.getLogicalMetrics() >> metricSet
        apiRequest.getGranularity() >> DAY
        apiRequest.getFormat() >> ResponseFormatType.JSON
        apiRequest.getUriInfo() >> uriInfo

        ResultSetSchema schema = new ResultSetSchema(DAY, [new MetricColumn("lm1")] as Set)
        resultSet = Mock(ResultSet)
        resultSet.getSchema() >> schema

        httpResponseMaker = new HttpResponseMaker(MAPPERS, dimensionDictionary)
        resultSetResponseProcessor = new ResultSetResponseProcessor(
                apiRequest,
                responseEmitter,
                druidResponseParser,
                MAPPERS,
                httpResponseMaker
        )

        responseContext.put(API_METRIC_COLUMN_NAMES.name, metricSet.collect {it.name} as LinkedHashSet)
        responseContext.put(REQUESTED_API_DIMENSION_FIELDS.name, [dim1: [BardDimensionField.ID] as LinkedHashSet])
    }

    def "buildResponse correctly builds a Response"() {

        setup:
        MultivaluedMap<String, Object> headers = resultSetResponseProcessor.getHeaders()
        headers.add("one", 1)
        headers.add("two", "2")

        responseContext.put("headers", headers)
        PreResponse preResponse = new PreResponse(resultSet, responseContext)

        when:
        Response actual = httpResponseMaker.buildResponse(preResponse, apiRequest)

        then:
        actual.getStatus() == 200
        actual.getHeaders().size() == 3
        actual.getHeaders().get("Content-Type").toString() == "[application/json; charset=utf-8]"
    }

    def "A CSV DataApiRequest builds a response with the CSV content type header set"() {

        setup: "A ResultSetResponseProcessor"
        responseContext.put("headers", resultSetResponseProcessor.getHeaders())
        PreResponse preResponse = new PreResponse(resultSet, responseContext)

        when: "The Response is built"
        Response actual = httpResponseMaker.buildResponse(preResponse, apiRequest)

        then: "The header is set correctly"
        actual.getHeaderString(HttpHeaders.CONTENT_TYPE) == "text/csv; charset=utf-8"
        actual.getHeaderString(HttpHeaders.CONTENT_DISPOSITION) == "attachment; filename=theMockPath_a_b__c_d.csv"

        and: "Mock override: A CSV-formatted request"
        apiRequest.getFormat() >> ResponseFormatType.CSV
    }

    def "createResponseBuilder() returns a non-null ResponseBuilder"() {

        when:
        Response.ResponseBuilder responseBuilder
        responseBuilder = httpResponseMaker.createResponseBuilder(resultSet, responseContext, apiRequest)

        then:
        responseBuilder != null
    }

    def "Prepare Response object for error case with different types of error arguments"() {

        setup:
        Response.StatusType statusType = Mock(Response.StatusType)
        statusType.getStatusCode() >> 400

        when:
        def response = httpResponseMaker.buildErrorResponse(400, "reason", "description", Mock(DruidAggregationQuery))

        then:
        response instanceof Response
        response.getStatusInfo().statusCode == 400
    }
}
