// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers

import static com.yahoo.bard.webservice.config.BardFeatureFlag.PARTIAL_DATA
import static com.yahoo.bard.webservice.web.responseprocessors.ResponseContextKeys.MISSING_INTERVALS_CONTEXT_KEY

import com.yahoo.bard.webservice.data.PartialDataHandler
import com.yahoo.bard.webservice.data.metric.mappers.PartialDataResultSetMapper
import com.yahoo.bard.webservice.data.metric.mappers.ResultSetMapper
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.druid.model.datasource.DataSource
import com.yahoo.bard.webservice.druid.model.query.GroupByQuery
import com.yahoo.bard.webservice.table.ConstrainedTable
import com.yahoo.bard.webservice.table.PhysicalTable
import com.yahoo.bard.webservice.table.PhysicalTableDictionary
import com.yahoo.bard.webservice.util.SimplifiedIntervalList
import com.yahoo.bard.webservice.web.DataApiRequest
import com.yahoo.bard.webservice.web.responseprocessors.MappingResponseProcessor
import com.yahoo.bard.webservice.web.responseprocessors.ResponseContext
import com.yahoo.bard.webservice.web.responseprocessors.ResponseProcessor

import org.joda.time.Interval

import spock.lang.Specification

import javax.ws.rs.core.MultivaluedHashMap
import javax.ws.rs.core.MultivaluedMap

class PartialDataRequestHandlerSpec extends Specification {

    static boolean partialData = PARTIAL_DATA.isOn()

    DataRequestHandler next = Mock(DataRequestHandler)
    PhysicalTableDictionary physicalTableDictionary = Mock(PhysicalTableDictionary)
    PhysicalTable physicalTable = Mock(ConstrainedTable)
    PartialDataHandler partialDataHandler = Mock(PartialDataHandler)

    RequestContext rc = Mock(RequestContext)
    DataApiRequest apiRequest = Mock(DataApiRequest)
    GroupByQuery groupByQuery = Mock(GroupByQuery)
    DataSource dataSource = Mock(DataSource)
    MappingResponseProcessor response = Mock(MappingResponseProcessor)
    SimplifiedIntervalList availableIntervals

    PartialDataRequestHandler handler = new PartialDataRequestHandler(
            next,
            partialDataHandler
    )

    def setup() {
        availableIntervals = new SimplifiedIntervalList([new Interval(0, 15), new Interval(30, 1000)])
        physicalTable.getAvailableIntervals() >> availableIntervals
        apiRequest.getDimensions() >> Collections.emptySet()
        apiRequest.getFilterDimensions() >> Collections.emptySet()
        apiRequest.getFilters() >> Collections.emptyMap()
        apiRequest.getGranularity() >> DefaultTimeGrain.DAY
        groupByQuery.getMetricDimensions() >> Collections.emptySet()
        groupByQuery.getDependentFieldNames() >> Collections.emptySet()
        groupByQuery.getInnermostQuery() >> groupByQuery
        groupByQuery.getDataSource() >> dataSource
        dataSource.getPhysicalTable() >> physicalTable
    }

    def cleanup() {
        PARTIAL_DATA.setOn(partialData)
    }

    def "Test no change to response or response context with no missing intervals"() {
        setup:
        PARTIAL_DATA.setOn(true)
        boolean success
        Set intervals = [] as TreeSet
        apiRequest.intervals >> []

        when:
        success = handler.handleRequest(rc, apiRequest, groupByQuery, response)

        then:
        success
        1 * partialDataHandler.findMissingTimeGrainIntervals(
                availableIntervals,
                new SimplifiedIntervalList(apiRequest.intervals),
                apiRequest.granularity
        ) >> intervals
        0 * response.getResponseContext()
        0 * response.getMappers()
        0 * response.getHeaders()
        1 * next.handleRequest(rc, apiRequest, groupByQuery, response) >> true
    }

    def "Test changes to response or response context with missing intervals"() {
        setup:
        PARTIAL_DATA.setOn(true)

        boolean success
        Interval i = new Interval(0, 1)
        SimplifiedIntervalList nonEmptyIntervals = new SimplifiedIntervalList([i])

        ResponseContext responseContext = new ResponseContext([:])
        MultivaluedMap responseHeaders = new MultivaluedHashMap<>()
        ResultSetMapper unusedMapper = Mock(ResultSetMapper)
        List resultSetMappers = [unusedMapper]
        apiRequest.intervals >> [new Interval(0, 2)]

        when:
        success = handler.handleRequest(rc, apiRequest, groupByQuery, response)
        PartialDataResultSetMapper mapper = resultSetMappers[0]

        then:
        success
        1 * partialDataHandler.findMissingTimeGrainIntervals(
                availableIntervals,
                new SimplifiedIntervalList(apiRequest.intervals),
                apiRequest.granularity
        ) >> nonEmptyIntervals
        1 * response.getResponseContext() >> responseContext
        1 * response.getMappers() >> resultSetMappers
        1 * next.handleRequest(rc, apiRequest, groupByQuery, response) >> true
        1 * response.getHeaders() >> responseHeaders
        mapper instanceof PartialDataResultSetMapper
        responseContext[MISSING_INTERVALS_CONTEXT_KEY.getName()] == nonEmptyIntervals
        responseContext.get(PartialDataRequestHandler.PARTIAL_DATA_HEADER)
    }

    def "Test exception handling a request with non mapping response"() {
        setup:
        ResponseProcessor response = Mock(ResponseProcessor)
        when:
        handler.handleRequest(rc, apiRequest, groupByQuery, response)

        then:
        thrown IllegalStateException
    }


    def "Test no mapper is added with config turned off"() {
        setup:
        PARTIAL_DATA.setOn(false)
        boolean success
        Interval i = new Interval(0, 1)
        SimplifiedIntervalList nonEmptyIntervals = new SimplifiedIntervalList([i])
        apiRequest.intervals >> [new Interval(0, 2)]

        ResponseContext responseContext = new ResponseContext([:])
        MultivaluedMap responseHeaders = new MultivaluedHashMap<>()
        ResultSetMapper unusedMapper = Mock(ResultSetMapper)
        List resultSetMappers = [unusedMapper]

        when:
        success = handler.handleRequest(rc, apiRequest, groupByQuery, response)
        ResultSetMapper mapper = resultSetMappers[0]

        then:
        success
        1 * partialDataHandler.findMissingTimeGrainIntervals(
                availableIntervals,
                new SimplifiedIntervalList(apiRequest.intervals),
                apiRequest.granularity
        ) >> nonEmptyIntervals
        1 * response.getResponseContext() >> responseContext
        1 * next.handleRequest(rc, apiRequest, groupByQuery, response) >> true
        1 * response.getHeaders() >> responseHeaders

        mapper == unusedMapper
        responseContext[MISSING_INTERVALS_CONTEXT_KEY.getName()] == nonEmptyIntervals
        responseContext.get(PartialDataRequestHandler.PARTIAL_DATA_HEADER)
    }
}
