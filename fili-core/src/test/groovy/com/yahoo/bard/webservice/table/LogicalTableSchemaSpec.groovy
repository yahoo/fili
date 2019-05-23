// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table

import com.yahoo.bard.webservice.data.config.names.ApiMetricName
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.LogicalMetricColumn
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain

import spock.lang.Specification

class LogicalTableSchemaSpec extends Specification {

    def "Build metric called the apiMetricName is Valid and filters metrics as indicated" () {
        String metricNameGood = "nameGood"
        String metricNameBad = "nameBad"

        LogicalMetric logicalMetricGood = Mock(LogicalMetric) {
            getName() >> metricNameGood
        }
        LogicalMetric logicalMetricBad = Mock(LogicalMetric) {
            getName() >> metricNameBad
        }

        ApiMetricName apiNameGood = Mock(ApiMetricName) {
            getApiName() >> metricNameGood
            asName() >> metricNameGood
        }
        ApiMetricName apiNameBad = Mock(ApiMetricName) {
            getApiName() >> metricNameBad
            asName() >> metricNameBad
        }
        1 * apiNameGood.isValidFor(_, logicalMetricGood) >> true
        1 * apiNameBad.isValidFor(_, logicalMetricBad) >> false
        0 * apiNameGood.isValidFor(_)
        0 * apiNameBad.isValidFor(_)

        List<LogicalMetricColumn> columns = [new LogicalMetricColumn(logicalMetricGood)]
        MetricDictionary metricDictionary = new MetricDictionary()
        metricDictionary.put(metricNameGood, logicalMetricGood)
        metricDictionary.put(metricNameBad, logicalMetricBad)

        expect:
        LogicalTableSchema.buildMetricColumns([apiNameGood, apiNameBad], DefaultTimeGrain.DAY, metricDictionary).collect() {it} == columns
    }
}
