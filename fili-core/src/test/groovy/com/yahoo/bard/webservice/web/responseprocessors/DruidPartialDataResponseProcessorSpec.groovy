// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.responseprocessors

import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback
import com.yahoo.bard.webservice.druid.model.datasource.DataSource
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery
import com.yahoo.bard.webservice.druid.model.query.QueryContext
import com.yahoo.bard.webservice.table.ConstrainedTable
import com.yahoo.bard.webservice.util.SimplifiedIntervalList
import com.yahoo.bard.webservice.web.ErrorMessageFormat

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode

import org.joda.time.Interval

import spock.lang.Specification

import java.util.stream.Collectors

class DruidPartialDataResponseProcessorSpec extends Specification {
    private static final ObjectMapper MAPPER = new ObjectMappersSuite().getMapper()
    private static final int ERROR_STATUS_CODE = 500
    private static final String REASON_PHRASE = 'The server encountered an unexpected condition which ' +
            'prevented it from fulfilling the request.'
    private static final String FIRST_INTERVAL = "2016-11-22T00:00:00.000Z/2016-12-18T00:00:00.000Z"
    private static final String SECOND_INTERVAL = "2016-12-25T00:00:00.000Z/2017-01-03T00:00:00.000Z"

    ResponseProcessor next
    HttpErrorCallback httpErrorCallback
    DruidAggregationQuery druidAggregationQuery
    DruidPartialDataResponseProcessor druidPartialDataResponseProcessor

    def setup() {
        next = Mock(ResponseProcessor)
        httpErrorCallback = Mock(HttpErrorCallback)
        druidAggregationQuery = Mock(DruidAggregationQuery)
        next.getErrorCallback(druidAggregationQuery) >> httpErrorCallback
        druidPartialDataResponseProcessor = new DruidPartialDataResponseProcessor(next)
    }

    def "getOverlap returns intersection between Druid intervals and Fili intervals in case of #caseDescription"() {
        given:
        JsonNode json = MAPPER.readTree(constructJSON(missingIntervals))

        DataSource dataSource = Mock(DataSource)
        ConstrainedTable constrainedTable = Mock(ConstrainedTable)

        constrainedTable.getAvailableIntervals() >> new SimplifiedIntervalList(
                availableIntervals.collect{it -> new Interval(it)}
        )
        dataSource.getPhysicalTable() >> constrainedTable
        druidAggregationQuery.getDataSource() >> dataSource

        expect:
        druidPartialDataResponseProcessor.getOverlap(json, druidAggregationQuery) == new SimplifiedIntervalList(
                expected.collect{it -> new Interval(it)}
        )

        where:
        missingIntervals | availableIntervals | expected | caseDescription
        [FIRST_INTERVAL, SECOND_INTERVAL] | [FIRST_INTERVAL, SECOND_INTERVAL] | [FIRST_INTERVAL, SECOND_INTERVAL] |
                "completely overlapped"
        [FIRST_INTERVAL, SECOND_INTERVAL] | [FIRST_INTERVAL] | [FIRST_INTERVAL] |
                "partially overlapped (Fili's intervals contained inside Druid's)"
        [FIRST_INTERVAL] | [FIRST_INTERVAL, SECOND_INTERVAL] | [FIRST_INTERVAL] |
                "partially overlapped (Druid's intervals contained inside Fili's)"
        [FIRST_INTERVAL, SECOND_INTERVAL] | ["2019-11-22T00:00:00.000Z/2019-12-18T00:00:00.000Z"] | [] |
                "no overlapping"
        [FIRST_INTERVAL, SECOND_INTERVAL] | [] | [] | "no overlapping (Fili has no emtpy intervals)"
    }

    def "checkOverflow recognizes interval overflow correctly"() {
        given:
        QueryContext queryContext = Mock(QueryContext)
        queryContext.getUncoveredIntervalsLimit() >> 10
        druidAggregationQuery.getContext() >> queryContext
        JsonNode json = MAPPER.readTree(
                '''
                {
                    "response": [{"k1":"v1"}],
                    "X-Druid-Response-Context": {
                        "uncoveredIntervals": [
                            "2016-11-22T00:00:00.000Z/2016-12-18T00:00:00.000Z",
                            "2016-12-25T00:00:00.000Z/2017-01-03T00:00:00.000Z"
                        ],
                        "uncoveredIntervalsOverflowed": true
                    },
                    "status-code": 200
                }
                '''
        )

        when:
        druidPartialDataResponseProcessor.checkOverflow(json, druidAggregationQuery)

        then:
        1 * httpErrorCallback.dispatch(
                ERROR_STATUS_CODE,
                REASON_PHRASE,
                ErrorMessageFormat.TOO_MANY_INTERVALS_MISSING.format("10")
        )
    }

    def "processResponse logs and invokes error callback on data availability mismatch"() {
        given:
        JsonNode json = MAPPER.readTree(
                constructJSON(
                        [FIRST_INTERVAL, SECOND_INTERVAL]
                )
        )

        DataSource dataSource = Mock(DataSource)
        ConstrainedTable constrainedTable = Mock(ConstrainedTable)

        Interval interval = new Interval(FIRST_INTERVAL)

        constrainedTable.getAvailableIntervals() >> new SimplifiedIntervalList([interval])

        dataSource.getPhysicalTable() >> constrainedTable
        druidAggregationQuery.getDataSource() >> dataSource

        when:
        druidPartialDataResponseProcessor.processResponse(json, druidAggregationQuery, Mock(LoggingContext))

        then:
        1 * httpErrorCallback.dispatch(
                ERROR_STATUS_CODE,
                REASON_PHRASE,
                ErrorMessageFormat.DATA_AVAILABILITY_MISMATCH.format([interval])
        )
    }

    def "validateJsonResponse recognizes missing component"() {
        given:
        ArrayNode arrayNode = MAPPER.createArrayNode()
        JsonNode druidResponseContext = Mock(JsonNode)
        JsonNode json = Mock(JsonNode)
        json.get(DruidJsonResponseContentKeys.DRUID_RESPONSE_CONTEXT.getName()) >> druidResponseContext

        when:
        // Druid returns response in an ArrayNode, which doesn't contain X-Druid-Response-Context or status code
        druidPartialDataResponseProcessor.validateJsonResponse(arrayNode, druidAggregationQuery)
        then:
        1 * httpErrorCallback.dispatch(
                ERROR_STATUS_CODE,
                REASON_PHRASE,
                ErrorMessageFormat.CONTEXT_AND_STATUS_MISSING_FROM_RESPONSE.format()
        )

        when:
        // Druid response is missing X-Druid-Response-Context
        json.has(DruidJsonResponseContentKeys.DRUID_RESPONSE_CONTEXT.getName()) >> false
        druidPartialDataResponseProcessor.validateJsonResponse(json, druidAggregationQuery)
        then:
        1 * httpErrorCallback.dispatch(
                ERROR_STATUS_CODE,
                REASON_PHRASE,
                ErrorMessageFormat.DRUID_RESPONSE_CONTEXT_MISSING_FROM_RESPONSE.format()
        )

        when:
        // Druid response has X-Druid-Response-Context,
        // but the X-Druid-Response-Context is missing uncoveredIntervals
        json.has(DruidJsonResponseContentKeys.DRUID_RESPONSE_CONTEXT.getName()) >> true
        druidResponseContext.has(DruidJsonResponseContentKeys.UNCOVERED_INTERVALS.getName()) >> false
        druidPartialDataResponseProcessor.validateJsonResponse(json, druidAggregationQuery)
        then:
        1 * httpErrorCallback.dispatch(
                ERROR_STATUS_CODE,
                REASON_PHRASE,
                ErrorMessageFormat.UNCOVERED_INTERVALS_MISSING_FROM_RESPONSE.format()
        )

        when:
        // Druid response has X-Druid-Response-Context,
        // but the X-Druid-Response-Context is missing uncoveredIntervalsOverflowed
        json.has(DruidJsonResponseContentKeys.DRUID_RESPONSE_CONTEXT.getName()) >> true
        druidResponseContext.has(DruidJsonResponseContentKeys.UNCOVERED_INTERVALS.getName()) >> true
        druidResponseContext.has(DruidJsonResponseContentKeys.UNCOVERED_INTERVALS_OVERFLOWED.getName()) >> false
        druidPartialDataResponseProcessor.validateJsonResponse(json, druidAggregationQuery)
        then:
        1 * httpErrorCallback.dispatch(
                ERROR_STATUS_CODE,
                REASON_PHRASE,
                ErrorMessageFormat.UNCOVERED_INTERVALS_OVERFLOWED_MISSING_FROM_RESPONSE.format()
        )

        when:
        // Druid response has X-Druid-Response-Context,
        // but the response is missing status code
        json.has(DruidJsonResponseContentKeys.DRUID_RESPONSE_CONTEXT.getName()) >> true
        druidResponseContext.has(DruidJsonResponseContentKeys.UNCOVERED_INTERVALS.getName()) >> true
        druidResponseContext.has(DruidJsonResponseContentKeys.UNCOVERED_INTERVALS_OVERFLOWED.getName()) >> true
        json.has(DruidJsonResponseContentKeys.STATUS_CODE.getName()) >> false
        druidPartialDataResponseProcessor.validateJsonResponse(json, druidAggregationQuery)
        then:
        1 * httpErrorCallback.dispatch(
                ERROR_STATUS_CODE,
                REASON_PHRASE,
                ErrorMessageFormat.STATUS_CODE_MISSING_FROM_RESPONSE.format()
        )
    }

    /**
     * Constructs a JSON response using a template and a list of provided intervals in String representation.
     *
     * @param intervals the list of intervals to be used by the template
     */
    static String constructJSON(List<String> intervals) {
        return String.format(
                '''
                {
                    "response": [{"k1":"v1"}],
                    "X-Druid-Response-Context": {
                        "uncoveredIntervals": [
                            %s
                        ],
                        "uncoveredIntervalsOverflowed": false
                    },
                    "status-code": 200
                }
                ''',
                intervals.stream()
                        .map{it -> "\"" + it + "\""}
                        .collect(Collectors.joining(","))
        )
    }
}
