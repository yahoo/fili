// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import com.yahoo.bard.webservice.table.PhysicalTable
import org.joda.time.DateTimeZone

import static com.yahoo.bard.webservice.config.BardFeatureFlag.INTERSECTION_REPORTING
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY

import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionColumn
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.dimension.DimensionField
import com.yahoo.bard.webservice.data.dimension.MapStoreManager
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProviderManager
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.table.TableGroup

import org.joda.time.DateTime

import spock.lang.Specification

class IntersectionReportingFlagOffSpec extends Specification {

    DimensionDictionary dimensionDict
    MetricDictionary metricDict
    LogicalTable table
    boolean intersectionReportingState

    def setup() {
        intersectionReportingState = INTERSECTION_REPORTING.isOn()
        INTERSECTION_REPORTING.setOn(false)
        LinkedHashSet<DimensionField> dimensionFields = [BardDimensionField.ID, BardDimensionField.DESC] as LinkedHashSet

        dimensionDict = new DimensionDictionary()
        [ "locale", "one", "two", "three" ].each { String name ->
            KeyValueStoreDimension keyValueStoreDimension = new KeyValueStoreDimension(name, "desc"+name, dimensionFields, MapStoreManager.getInstance(name), ScanSearchProviderManager.getInstance(name))
            keyValueStoreDimension.setLastUpdated(new DateTime(10000))
            dimensionDict.add(keyValueStoreDimension)
        }

        metricDict = new MetricDictionary()
        [ "met1", "met2", "met3", "met4" ].each { String name ->
            metricDict.put(name, new LogicalMetric(null, null, name))
        }
        TableGroup tg = Mock(TableGroup)
        tg.getDimensions() >> dimensionDict.apiNameToDimension.values()
        table = new LogicalTable("name", DAY, tg)
        dimensionDict.apiNameToDimension.values().each {
            DimensionColumn.addNewDimensionColumn(table, it, new PhysicalTable("abc", DAY.buildZonedTimeGrain(DateTimeZone.UTC), [:]))
        }

    }

    def cleanup() {
        INTERSECTION_REPORTING.setOn(intersectionReportingState)
    }

    def "When INTERSECTION_REPORTING feature flag is off, query with valid unfiltered metrics returns the correct metrics from the metric dictionary"() {
        when: "The metric string contains valid unfiltered metrics"
        new DataApiRequest().generateLogicalMetrics("met1,met2,met3", metricDict, dimensionDict, table)

        then: "The metrics generated are the same ones as in the dictionary"
        ["met1", "met2", "met3" ].collect { metricDict.get(it) }
    }

    def "When INTERSECTION_REPORTING feature flag is off, query with valid filtered metrics throws BadApiException"() {
        when:
        new DataApiRequest().generateLogicalMetrics("met1(AND(app1,app2)),met2,met3", metricDict, dimensionDict, table)

        then:
        Exception e = thrown(BadApiRequestException)
        e.getMessage() == ErrorMessageFormat.METRICS_UNDEFINED.format("[met1(AND(app1, app2))]")
    }
}
