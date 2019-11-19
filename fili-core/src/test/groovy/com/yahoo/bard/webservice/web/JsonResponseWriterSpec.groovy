// Copyright 2017 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import static com.yahoo.bard.webservice.config.BardFeatureFlag.PARTIAL_DATA

import com.yahoo.bard.webservice.data.ResultSet
import com.yahoo.bard.webservice.data.metric.MetricColumn
import com.yahoo.bard.webservice.util.DateTimeFormatterFactory
import com.yahoo.bard.webservice.util.GroovyTestUtils
import com.yahoo.bard.webservice.util.JsonSlurper
import com.yahoo.bard.webservice.util.JsonSortStrategy
import com.yahoo.bard.webservice.util.Pagination
import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import org.joda.time.Interval
import org.joda.time.format.DateTimeFormatter

import spock.lang.Unroll

class JsonResponseWriterSpec extends ResponseWriterSpec {

    @Unroll
    def "test for requested numeric metrics in the API response with the #linkNames links"() {
        setup:
        formattedDateTime = dateTime.toString(getDefaultFormat())

        ResponseData paginatedResponse = new ResponseData(
                resultSet,
                apiRequest,
                new SimplifiedIntervalList(),
                volatileIntervals,
                pagination,
                bodyLinks,
        )
        GString metaBlock = """{
                        "pagination": {
                            $bodyLinksAsJson,
                            "currentPage": $PAGE,
                            "rowsPerPage": $PER_PAGE,
                            "numberOfResults": 6
                        }
                    }"""
        String expectedJson = withMetaObject(defaultJsonFormat, metaBlock)

        ByteArrayOutputStream os = new ByteArrayOutputStream()

        jsonResponseWriter = new JsonResponseWriter(MAPPERS)
        jsonResponseWriter.write(apiRequest, paginatedResponse, os)

        expect:
        GroovyTestUtils.compareJson(os.toString(), expectedJson)

        where:
        linkNames << LINK_NAMES_LIST
        bodyLinks << BODY_LINKS_LIST
        bodyLinksAsJson << BODY_LINKS_AS_JSON_LIST
    }

    @Unroll
    def "test for existence of missing intervals in response when #arePaginating"() {
        setup:
        boolean partialDataSetting = PARTIAL_DATA.isOn()
        PARTIAL_DATA.setOn(true)

        formattedDateTime = dateTime.toString(getDefaultFormat())
        GString paginationBlock = """, "pagination": {
                        $bodyLinksAsJson,
                        "currentPage": $PAGE,
                        "rowsPerPage": $PER_PAGE,
                        "numberOfResults": 6
                    }"""
        GString metaBlock = """{
                "missingIntervals" : [
                    "2014-07-01 00:00:00.000/2014-07-08 00:00:00.000",
                    "2014-07-15 00:00:00.000/2014-07-22 00:00:00.000"
                 ]
                 ${paginating ? paginationBlock : ""}
                  }
                }"""

        SimplifiedIntervalList missingIntervals = [new Interval("2014-07-01/2014-07-08"), new Interval(
                "2014-07-15/2014-07-22"
        )] as SimplifiedIntervalList
        ResponseData response1 = new ResponseData(
                resultSet,
                apiRequest,
                missingIntervals,
                volatileIntervals,
                paginating ? pagination : null,
                bodyLinks
        )

        String expectedJson = withMetaObject(defaultJsonFormat, metaBlock)

        jsonResponseWriter = new JsonResponseWriter(MAPPERS)
        jsonResponseWriter.write(apiRequest, response1, os)

        expect:
        GroovyTestUtils.compareJson(os.toString(), expectedJson)

        cleanup:
        PARTIAL_DATA.setOn(partialDataSetting)

        where:
        paginating << [false] + [true] * LINK_NAMES_LIST.size()
        linkNames << [''] + LINK_NAMES_LIST
        bodyLinks << [[:] as Map] + BODY_LINKS_LIST
        bodyLinksAsJson << [''] + BODY_LINKS_AS_JSON_LIST
        arePaginating = paginating ? "paginating with pagination links $linkNames" : "not paginating"
    }

    @Unroll
    def "Response properly serializes #type metrics in JSON format"() {
        given:
        formattedDateTime = dateTime.toString(getDefaultFormat())

        and: "The expected response, containing metric fields as the type under test"
        Map json = new JsonSlurper(JsonSortStrategy.SORT_NONE).parseText(defaultJsonFormat)
        json["rows"].each {
            it["luckyNumbers"] = firstValue
            it["unluckyNumbers"] = secondValue
        }
        String expectedJson = MAPPERS.getMapper().writeValueAsString(json)

        and: "A test result set with a few complex metrics"
        metricColumnsMap.put(new MetricColumn("luckyNumbers"), firstValue)
        metricColumnsMap.put(new MetricColumn("unluckyNumbers"), secondValue)
        defaultRequestedMetrics.addAll([new MetricColumn("luckyNumbers"), new MetricColumn("unluckyNumbers")])
        ResultSet resultSetWithComplexMetrics = buildTestResultSet(metricColumnsMap, defaultRequestedMetrics)

        when: "We serialize the response"
        ResponseData complexResponse = new ResponseData(
                resultSetWithComplexMetrics,
                apiRequest,
                new SimplifiedIntervalList(),
                new SimplifiedIntervalList(),
                (Pagination) null,
                [:]
        )
        ByteArrayOutputStream os = new ByteArrayOutputStream()
        jsonResponseWriter = new JsonResponseWriter(MAPPERS)
        jsonResponseWriter.write(apiRequest, complexResponse, os)

        then: "The response is serialized correctly"
        GroovyTestUtils.compareJson(os.toString(), expectedJson)

        where:
        type      | firstValue                           | secondValue
        "String"  | "1, 3, 7"                            | "2"
        "boolean" | true                                 | false
        "JsonNode"| '{"values": "1, 3, 7", "length": 3}' | '{"values": "2", "length": 1}'
        "null"    | null                                 | null
    }

    String getDefaultFormat() {
        return "YYYY-MM-dd HH:mm:ss.SSS"
    }

    @Unroll
    def "test json format with format #outputDateTimeFormat"() {
        setup:
        formattedDateTime = dateTime.toString(outputDateTimeFormat)
        DateTimeFormatter originalFormat = DateTimeFormatterFactory.datetimeOutputFormatter
        DateTimeFormatterFactory.datetimeOutputFormatter = null
        systemConfig.setProperty(
                DateTimeFormatterFactory.OUTPUT_DATETIME_FORMAT,
                outputDateTimeFormat
        )
        response = new ResponseData(
                buildTestResultSet(metricColumnsMap, defaultRequestedMetrics),
                apiRequest,
                new SimplifiedIntervalList(),
                volatileIntervals,
                (Pagination) null,
                [:]
        )

        jsonResponseWriter = new JsonResponseWriter(MAPPERS)
        jsonResponseWriter.write(apiRequest, response, os)


        expect:
        GroovyTestUtils.compareJson(os.toString(), defaultJsonFormat)

        cleanup:
        systemConfig.clearProperty(DateTimeFormatterFactory.OUTPUT_DATETIME_FORMAT)
        DateTimeFormatterFactory.datetimeOutputFormatter = originalFormat

        where:
        outputDateTimeFormat << [
            "YYYY-MM-dd HH:mm:ss.SSSZZZ",
            "YYYY-MM-dd HH:mm:ss.SSSZ",
            "YYYY-MM-dd HH:mm:ss.SSS",
            "YYYY-MM-dd HH:mm",
            "YYYY-MM-dd"
        ]
    }
}
