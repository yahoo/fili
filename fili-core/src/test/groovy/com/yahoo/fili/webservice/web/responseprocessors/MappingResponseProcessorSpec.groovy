// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.fili.webservice.web.responseprocessors

import com.yahoo.fili.webservice.application.ObjectMappersSuite
import com.yahoo.fili.webservice.data.HttpResponseChannel
import com.yahoo.fili.webservice.data.HttpResponseMaker
import com.yahoo.fili.webservice.data.ResultSet
import com.yahoo.fili.webservice.data.dimension.DimensionDictionary
import com.yahoo.fili.webservice.data.metric.LogicalMetric
import com.yahoo.fili.webservice.data.metric.mappers.ResultSetMapper
import com.yahoo.fili.webservice.druid.client.FailureCallback
import com.yahoo.fili.webservice.druid.client.HttpErrorCallback
import com.yahoo.fili.webservice.druid.model.query.DruidAggregationQuery
import com.yahoo.fili.webservice.druid.model.query.GroupByQuery
import com.yahoo.fili.webservice.web.DataApiRequest
import com.yahoo.fili.webservice.web.PreResponse

import com.fasterxml.jackson.databind.JsonNode

import rx.subjects.PublishSubject
import rx.subjects.Subject

import spock.lang.Specification

import javax.ws.rs.container.AsyncResponse

class MappingResponseProcessorSpec extends Specification{

    GroupByQuery groupByQuery
    DataApiRequest apiRequest
    ObjectMappersSuite objectMappers
    Subject<PreResponse, PreResponse> mappingResponseChannel

    def setup() {
        groupByQuery = Mock(GroupByQuery)
        apiRequest = Mock(DataApiRequest)
        objectMappers = new ObjectMappersSuite()

        AsyncResponse asyncResponse = Mock(AsyncResponse)
        HttpResponseChannel httpResponseChannel = new HttpResponseChannel(asyncResponse, apiRequest, new HttpResponseMaker(objectMappers, Mock(DimensionDictionary)))
        mappingResponseChannel = PublishSubject.create()
        mappingResponseChannel.subscribe(httpResponseChannel)
    }

    MappingResponseProcessor buildSimpleMRP() {
        return new MappingResponseProcessor(apiRequest, objectMappers) {
            public FailureCallback getFailureCallback(DruidAggregationQuery<?> query) {return null;}
            public HttpErrorCallback getErrorCallback(DruidAggregationQuery<?> query) {return null;}
            public void processResponse(JsonNode json, DruidAggregationQuery<?> query, LoggingContext metadata) {}
        }
    }

    def "Test constructor"() {

        setup:
        ResultSetMapper rsm1 = Mock(ResultSetMapper)
        LogicalMetric lm1 = Mock(LogicalMetric)
        LogicalMetric lm2 = Mock(LogicalMetric)
        lm1.getCalculation() >> rsm1
        lm2.getCalculation() >> null
        Set<LogicalMetric> metricSet = [lm1, lm2] as Set
        1 * apiRequest.getLogicalMetrics() >> metricSet
        def expectedContext =[:]

        when:
        def mrp = buildSimpleMRP()

        then:
        mrp.getResponseContext() == expectedContext
        mrp.getDataApiRequest() == apiRequest
        mrp.getHeaders() == [:]
        mrp.getMappers() == [rsm1] as List
    }

    def "Test buildMapperList: null logical metric set "() {

        setup:
        1 * apiRequest.getLogicalMetrics() >> null

        when:
        buildSimpleMRP().buildResultSetMapperList(apiRequest)

        then:
        thrown NullPointerException

    }

    def "Test buildMapperList: empty metric set"() {
        setup:
        apiRequest.getLogicalMetrics() >> []

        expect:
        buildSimpleMRP().buildResultSetMapperList(apiRequest) == []
    }

    def "Test mapResultSet basic mapping"() {

        setup:
        ResultSet rs1 = Mock(ResultSet)
        ResultSet rs2 = Mock(ResultSet)
        ResultSet expected = Mock(ResultSet)
        ResultSet actual

        ResultSetMapper rsm1 = Mock(ResultSetMapper)
        ResultSetMapper rsm2 = Mock(ResultSetMapper)

        LogicalMetric lm1 = Mock(LogicalMetric)
        LogicalMetric lm2 = Mock(LogicalMetric)
        lm1.getCalculation() >> rsm1
        lm2.getCalculation() >> rsm2

        rsm1.map(rs1) >> rs2
        rsm2.map(rs2) >> expected

        Set<LogicalMetric> metricSet = [lm1, lm2] as Set
        1 * apiRequest.getLogicalMetrics() >> metricSet

        when:
        def mrp = buildSimpleMRP()
        actual = mrp.mapResultSet(rs1)

        then:
        actual == expected
    }

    def "Test get standard error callback"() {

        setup:
        ResultSetMapper resultSetMapper1 = Mock(ResultSetMapper)
        ResultSetMapper resultSetMapper2 = Mock(ResultSetMapper)

        LogicalMetric logicalMetric1 = Mock(LogicalMetric)
        LogicalMetric logicalMetric2 = Mock(LogicalMetric)
        logicalMetric1.getCalculation() >> resultSetMapper1
        logicalMetric2.getCalculation() >> resultSetMapper2

        Set<LogicalMetric> metricSet = [logicalMetric1, logicalMetric2] as Set
        1 * apiRequest.getLogicalMetrics() >> metricSet

        expect:
        buildSimpleMRP().getStandardError(mappingResponseChannel, groupByQuery) instanceof HttpErrorCallback
    }

    def "Test get standard failure callback"() {
        setup:
        ResultSetMapper resultSetMapper1 = Mock(ResultSetMapper)
        ResultSetMapper resultSetMapper2 = Mock(ResultSetMapper)

        LogicalMetric logicalMetric1 = Mock(LogicalMetric)
        LogicalMetric logicalMetric2 = Mock(LogicalMetric)
        logicalMetric1.getCalculation() >> resultSetMapper1
        logicalMetric2.getCalculation() >> resultSetMapper2

        Set<LogicalMetric> metricSet = [logicalMetric1, logicalMetric2] as Set
        1 * apiRequest.getLogicalMetrics() >> metricSet

        expect:
        buildSimpleMRP().getStandardFailure(mappingResponseChannel, groupByQuery) instanceof FailureCallback
    }
}
