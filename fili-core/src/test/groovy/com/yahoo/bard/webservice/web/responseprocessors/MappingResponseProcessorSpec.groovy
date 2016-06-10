// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.web.responseprocessors

import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.data.ResultSet
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.mappers.ResultSetMapper
import com.yahoo.bard.webservice.druid.client.FailureCallback
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery
import com.yahoo.bard.webservice.druid.model.query.GroupByQuery
import com.yahoo.bard.webservice.web.DataApiRequest

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectWriter

import spock.lang.Specification

import javax.ws.rs.container.AsyncResponse

class MappingResponseProcessorSpec extends Specification{

    GroupByQuery groupByQuery
    DataApiRequest apiRequest
    ObjectMappersSuite objectMappers
    def setup() {
        groupByQuery = Mock(GroupByQuery)
        apiRequest = Mock(DataApiRequest)
        objectMappers = new ObjectMappersSuite()
    }

    MappingResponseProcessor buildSimpleMRP() {
        return new MappingResponseProcessor(apiRequest, objectMappers) {
            public FailureCallback getFailureCallback(DruidAggregationQuery<?> query) {return null;}
            public HttpErrorCallback getErrorCallback(DruidAggregationQuery<?> query) {return null;}
            public void processResponse(JsonNode json, DruidAggregationQuery<?> query, ResponseContext metadata) {}
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
        mrp.writer != null
    }

    def "Test buildMapperList: null logical metric set "() {
        setup:
        1 * apiRequest.getLogicalMetrics() >> null

        when:
        MappingResponseProcessor.buildResultSetMapperList(apiRequest)

        then:
        thrown NullPointerException

    }

    def "Test buildMapperList: empty metric set"() {
        setup:
        1 * apiRequest.getLogicalMetrics() >> []

        when:
        def mapperList = MappingResponseProcessor.buildResultSetMapperList(apiRequest)

        then:
        mapperList == []
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
        apiRequest.getLogicalMetrics() >> metricSet

        when:
        def mrp = buildSimpleMRP()
        actual = mrp.mapResultSet(rs1)

        then:
        actual == expected
    }

    def "Test get standard error callback"() {
        setup:
        AsyncResponse asyncResponse = Mock(AsyncResponse)
        ObjectWriter writer = Mock(ObjectWriter)

        def mapperList = MappingResponseProcessor.getStandardError(asyncResponse, groupByQuery, writer)

        expect:
        mapperList instanceof HttpErrorCallback
    }

    def "Test get standard failure callback"() {
        setup:
        AsyncResponse asyncResponse = Mock(AsyncResponse)
        ObjectWriter writer = Mock(ObjectWriter)

        def mapperList = MappingResponseProcessor.getStandardFailure(asyncResponse, groupByQuery, writer)

        expect:
        mapperList instanceof FailureCallback
    }
}
