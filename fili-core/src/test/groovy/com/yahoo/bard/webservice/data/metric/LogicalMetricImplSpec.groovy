// Copyright 2020 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric

import com.yahoo.bard.webservice.data.Result
import com.yahoo.bard.webservice.data.ResultSetSchema
import com.yahoo.bard.webservice.data.metric.mappers.NoOpResultSetMapper
import com.yahoo.bard.webservice.data.metric.mappers.RenamableResultSetMapper
import com.yahoo.bard.webservice.data.metric.mappers.ResultSetMapper

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

    def "Attempting to rename renamable and non-renamable result set mappers functions succeeds"() {
        setup:
        String newName = "newName"
        ResultSetMapper nonRenamable = Mock()
        TestRenamableResultSetMapper renamableResultSetMapper = Spy()

        when: "Attempt to rename non-renamable result set mapper"
        ResultSetMapper result = testMetric.renameResultSetMapper(nonRenamable, newName)

        then: "The input mapper is returned unchanged"
        result == nonRenamable

        when: "Attempt to rename renamable result set mapper"
        testMetric.renameResultSetMapper(renamableResultSetMapper, newName)

        then: "Rename method is called with input name"
        1 * renamableResultSetMapper.withColumnName(newName)
        0 * renamableResultSetMapper._
    }

    /**
     * Does nothing. Just used as a base for a Spy mock to test interactions
     */
    class TestRenamableResultSetMapper extends ResultSetMapper implements RenamableResultSetMapper {

        @Override
        ResultSetMapper withColumnName(String newColumnName) {
            return this
        }

        @Override
        protected Result map(Result result, ResultSetSchema schema) {
            return result
        }

        @Override
        protected ResultSetSchema map(ResultSetSchema schema) {
            return schema
        }
    }
}
