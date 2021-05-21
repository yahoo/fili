// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.TABLE_GRANULARITY_MISMATCH

import com.yahoo.bard.webservice.config.SystemConfig
import com.yahoo.bard.webservice.config.SystemConfigProvider
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.data.time.Granularity
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.table.LogicalTableDictionary
import com.yahoo.bard.webservice.table.TableIdentifier
import com.yahoo.bard.webservice.web.DefaultResponseFormatType
import com.yahoo.bard.webservice.web.ResponseFormatType
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException
import com.yahoo.bard.webservice.web.ErrorMessageFormat
import com.yahoo.bard.webservice.web.apirequest.generator.DefaultAsyncAfterGenerator
import com.yahoo.bard.webservice.web.apirequest.utils.TestingApiRequestProvider
import com.yahoo.bard.webservice.web.util.PaginationParameters

import spock.lang.Specification
import spock.lang.Unroll

abstract class ApiRequestImplSpec extends Specification {

    ApiRequest apiRequestImpl

    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    abstract ApiRequest buildApiRequestImpl(String format, String filename, String async, String perPage, String page)

    @Unroll
    def "getFormat retrieves a reasonable format for #format"() {
        when:
        ApiRequest apiRequestImpl = buildApiRequestImpl(format.toString(), "", "-1", "1", "1")

        then:
        apiRequestImpl.getFormat() == format

        where:
        format << (Arrays.asList(DefaultResponseFormatType.values()))
    }

    def "getFileName retrieves expected filename"() {
        ApiRequest apiRequestImpl = buildApiRequestImpl(DefaultResponseFormatType.JSON.toString(), name, "-1", "1", "1")

        expect:
        apiRequestImpl.getDownloadFilename() == value

        where:
        name    | value
        "test1" | Optional.of("test1")
        null    | Optional.empty()
    }

    @Unroll
    def "getAsyncAfter with #name retrieves expected value #expected"() {
        ApiRequest apiRequestImpl = buildApiRequestImpl(DefaultResponseFormatType.JSON.toString(), "foo", name, "1", "1")

        expect:
        apiRequestImpl.getAsyncAfter() == expected

        where:
        name     | expected
        "-1"     | -1
        null     | DefaultAsyncAfterGenerator.generateAsyncAfter(null)
        "never"  | Long.MAX_VALUE
        "always" | 0
    }

    @Unroll
    def "getPaginationParameters with perPage, Page (#perPage, #page) retrieves expected #value"() {
        ApiRequest apiRequestImpl = buildApiRequestImpl(DefaultResponseFormatType.JSON.toString(), "foo", null as String, perPage, page)

        expect:
        apiRequestImpl.getPaginationParameters() == value

        where:
        perPage | page | value
        "1"  | "1"  | Optional.of(new PaginationParameters("1", "1"))
        ""   | ""   | Optional.empty()
        null | null | Optional.empty()
    }
/*
    These tests need to be moved or deleted
    def "generateLogicalMetrics() returns existing LogicalMetrics"() {
        given: "two LogicalMetrics in MetricDictionary"
        LogicalMetric logicalMetric1 = Mock(LogicalMetric)
        LogicalMetric logicalMetric2 = Mock(LogicalMetric)
        MetricDictionary metricDictionary = Mock(MetricDictionary)
        metricDictionary.get("logicalMetric1") >> logicalMetric1
        metricDictionary.get("logicalMetric2") >> logicalMetric2

        expect: "the two metrics are returned on request"
        apiRequestImpl.generateLogicalMetrics("logicalMetric1,logicalMetric2", metricDictionary) ==
                [logicalMetric1, logicalMetric2] as LinkedHashSet
    }

    def "generateLogicalMetrics() throws BadApiRequestException on non-existing LogicalMetric"() {
        given: "a MetricDictionary"
        LogicalMetric logicalMetric = Mock(LogicalMetric)
        MetricDictionary metricDictionary = Mock(MetricDictionary)
        metricDictionary.get("logicalMetric") >> logicalMetric

        when: "a non-existing metrics request"
        apiRequestImpl.generateLogicalMetrics("nonExistingMetric", metricDictionary)

        then: "BadApiRequestException is thrown"
        BadApiRequestException exception = thrown()
        exception.message == ErrorMessageFormat.METRICS_UNDEFINED.logFormat(["nonExistingMetric"])
    }*/
}
