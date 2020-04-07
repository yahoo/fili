package com.yahoo.bard.webservice.data.metric

import com.yahoo.bard.webservice.data.metric.mappers.NoOpResultSetMapper
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation
import com.yahoo.bard.webservice.druid.model.aggregation.LongSumAggregation

import spock.lang.Specification

class LogicalMetricImplSpec extends Specification {

    LogicalMetricImpl testMetric

    def setup() {
        testMetric = new LogicalMetricImpl(
                new LogicalMetricInfo("name"),
                null,
                new NoOpResultSetMapper()
        )
    }

    def "Attempting to rename field on null query simply returns null"() {
        expect:
        testMetric.renameTemplateDruidQuery(null, "unused", "unused") == null
    }

    def "Error is thrown if attempt to rename MetricField that does not exist on TDQ"() {
        when:
        testMetric.renameTemplateDruidQuery(new TemplateDruidQuery([],[]), "notPresent", "unused")

        then:
        thrown(IllegalArgumentException)
    }

    def "Rename is attempted with proper values on correctly formed TDQ"() {
        setup:
        String targetName = "targetName"
        String newName = "newName"
        TemplateDruidQuery testTdq = Mock(TemplateDruidQuery)

        when:
        testMetric.renameTemplateDruidQuery(testTdq, targetName, newName)

        then:
        1 * testTdq.containsMetricField(targetName) >> true
        1 * testTdq.renameMetricField(targetName, newName)
    }
}
