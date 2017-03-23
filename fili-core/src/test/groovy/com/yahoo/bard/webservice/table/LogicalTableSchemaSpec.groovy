package com.yahoo.bard.webservice.table

import com.yahoo.bard.webservice.data.config.names.ApiMetricName
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.LogicalMetricColumn
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain

import spock.lang.Specification

class LogicalTableSchemaSpec extends Specification {

    def "Build metric called the apiMetricName is Valid and filters metrics as indicated" () {
        String metricName1 = "name1"
        String metricName2 = "name2"

        LogicalMetric logicalMetric1 = Mock(LogicalMetric)
        LogicalMetric logicalMetric2 = Mock(LogicalMetric)

        ApiMetricName apiName1 = Mock(ApiMetricName) {
            getApiName() >> metricName1
        }
        ApiMetricName apiName2 = Mock(ApiMetricName) {
            getApiName() >> metricName2
        }
        1 * apiName1.isValidFor(logicalMetric1, _) >> true
        1 * apiName2.isValidFor(logicalMetric2, _) >> false
        0 * apiName1.isValidFor(_)
        0 * apiName2.isValidFor(_)

        List<LogicalMetricColumn> columns = [new LogicalMetricColumn(logicalMetric1)]
        MetricDictionary metricDictionary = new MetricDictionary()
        metricDictionary.put(metricName1, logicalMetric1)
        metricDictionary.put(metricName2, logicalMetric2)

        expect:
        LogicalTableSchema.buildMetricColumns([apiName1, apiName2], DefaultTimeGrain.DAY, metricDictionary).collect() {it} == columns
    }
}
