// Copyright 2021 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.TABLE_GRANULARITY_MISMATCH

import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.data.time.Granularity
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.table.LogicalTableDictionary
import com.yahoo.bard.webservice.table.TableIdentifier
import com.yahoo.bard.webservice.web.apirequest.beanimpl.ApiRequestBeanImpl
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException
import com.yahoo.bard.webservice.web.ErrorMessageFormat
import com.yahoo.bard.webservice.web.apirequest.utils.TestingApiRequestProvider
import com.yahoo.bard.webservice.web.util.BardConfigResources

import spock.lang.Specification

abstract class TablesApiRequestImplSpec extends Specification {

    BardConfigResources bardConfigResources = Mock(BardConfigResources)

    TablesApiRequestImpl tablesApiRequestImpl
    LogicalTableDictionary logicalTableDictionary
    abstract ApiRequest buildApiRequestImpl(String tableName, String granularity)

    def "generateTable() returns existing LogicalTable"() {
        given: "we insert a LogicalTable into LogicalTableDictionary"
        String tableName = "tableName"
        Granularity granularity = Mock(Granularity)
        LogicalTableDictionary logicalTableDictionary = Mock(LogicalTableDictionary)
        TableIdentifier tableIdentifier = new TableIdentifier(tableName, granularity)
        LogicalTable logicalTable = Mock(LogicalTable)
        logicalTableDictionary.get(tableIdentifier) >> logicalTable

        expect: "we can fetch the LogicalTable from it"
        TablesApiRequestImpl.generateTable(tableName, granularity, logicalTableDictionary) == logicalTable
    }

    def "generateTable() throws BadApiRequestException on non-existing LogicalTable"() {
        given: "we insert a LogicalTable into LogicalTableDictionary"
        String tableName = "tableName"
        Granularity granularity = Mock(Granularity)
        LogicalTableDictionary logicalTableDictionary = Mock(LogicalTableDictionary)
        TableIdentifier tableIdentifier = new TableIdentifier(tableName, granularity)
        LogicalTable logicalTable = Mock(LogicalTable)
        logicalTableDictionary.get(tableIdentifier) >> logicalTable

        String nonExistingTableName = "I don't exist"

        when: "we try to fetch a non-existing LogicalTable"
        TablesApiRequestImpl.generateTable(nonExistingTableName, granularity, logicalTableDictionary)

        then: "we get a BadApiRequestException"
        BadApiRequestException exception = thrown()
        exception.message == TABLE_GRANULARITY_MISMATCH.logFormat(granularity, nonExistingTableName)
    }
/*
    def "generateLogicalMetrics() returns existing LogicalMetrics"() {
        given: "two LogicalMetrics in MetricDictionary"
        LogicalMetric logicalMetric1 = Mock(LogicalMetric)
        LogicalMetric logicalMetric2 = Mock(LogicalMetric)
        MetricDictionary metricDictionary = Mock(MetricDictionary)
        metricDictionary.get("logicalMetric1") >> logicalMetric1
        metricDictionary.get("logicalMetric2") >> logicalMetric2

        expect: "the two metrics are returned on request"
        tablesApiRequestImpl.generateLogicalMetrics("logicalMetric1,logicalMetric2", metricDictionary) ==
                [logicalMetric1, logicalMetric2] as LinkedHashSet
    }

    def "generateLogicalMetrics() throws BadApiRequestException on non-existing LogicalMetric"() {
        given: "a MetricDictionary"
        LogicalMetric logicalMetric = Mock(LogicalMetric)
        MetricDictionary metricDictionary = Mock(MetricDictionary)
        metricDictionary.get("logicalMetric") >> logicalMetric
        TablesApiRequestImpl tablesApiRequest = buildApiRequestImpl(final String tableName, String granularity)

        when: "a non-existing metrics request"
        tablesApiRequest.generateLogicalMetrics("nonExistingMetric", metricDictionary)

        then: "BadApiRequestException is thrown"
        BadApiRequestException exception = thrown()
        exception.message == ErrorMessageFormat.METRICS_UNDEFINED.logFormat(["nonExistingMetric"])
    }*/
}
