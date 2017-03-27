// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability

import com.google.common.collect.Sets
import com.yahoo.bard.webservice.data.metric.MetricColumn
import com.yahoo.bard.webservice.table.PhysicalTable
import com.yahoo.bard.webservice.table.PhysicalTableSchema
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint
import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import org.joda.time.Interval

import spock.lang.Specification
import spock.lang.Unroll

/**
 * Test for metric union availability behavior.
 */
class MetricUnionAvailabilitySpec extends Specification {
    Availability availability1
    Availability availability2

    MetricColumn metricColumn1
    MetricColumn metricColumn2

    Interval interval1
    Interval interval2
    Interval interval3
    Interval interval4

    PhysicalTableSchema schema1
    PhysicalTableSchema schema2

    PhysicalTable physicalTable1
    PhysicalTable physicalTable2

    MetricUnionAvailability metricUnionAvailability

    def setup() {
        availability1 = Mock(Availability)
        availability2 = Mock(Availability)

        metricColumn1 = new MetricColumn('metric1')
        metricColumn2 = new MetricColumn('metric2')

        interval1 = new Interval('2018-01-01/2018-02-01')
        interval2 = new Interval('2018-02-01/2018-03-01')
        interval3 = new Interval('2019-01-01/2019-02-01')
        interval4 = new Interval('2018-11-01/2018-12-01')

        schema1 = Mock(PhysicalTableSchema)
        schema2 = Mock(PhysicalTableSchema)

        physicalTable1 = Mock(PhysicalTable)
        physicalTable2 = Mock(PhysicalTable)

        physicalTable1.getAvailability() >> availability1
        physicalTable2.getAvailability() >> availability2

        physicalTable1.getSchema() >> schema1
        physicalTable2.getSchema() >> schema2
    }

    @Unroll
    def "checkDuplicateValue returns duplicates of metric column names from 2 physical tables in the case of #caseDescription"() {
        when:
        Map<MetricColumn, Set<Availability>> actual = MetricUnionAvailability.getDuplicateValue(
                [
                        (availability1): metricColumnNames1.collect{it -> new MetricColumn(it)} as Set,
                        (availability2): metricColumnNames2.collect{it -> new MetricColumn(it)} as Set
                ]
        )

        then:
        actual.size() == expected.size()
        for (Map.Entry<String, Integer> entry : expected.entrySet()) {
            MetricColumn duplicate = new MetricColumn(entry.key)
            actual.containsKey(duplicate)
            actual.get(duplicate).size() == entry.value
        }

        where:
        metricColumnNames1     | metricColumnNames2 | expected       | caseDescription
        []                     | []                 | [:]            | 'both physical tables having empty metric column names'
        ['metric1']            | []                 | [:]            | 'one empty table and one non-empty table'
        ['metric1']            | ['metric1']        | ['metric1': 2] | 'both physical tables having the same metric column names'
        ['metric1', 'metric2'] | ['metric1']        | ['metric1': 2] | 'two tables having intersections'
        ['metric1']            | ['metric2']        | [:]            | 'two tables having no intersections'
    }

    def "getAllAvailableIntervals returns configured metric columns with their corresponding available intervals in union"() {
        given:
        availability1.getAllAvailableIntervals() >> [
                (new MetricColumn('metric1')): [interval1] as List,
                (new MetricColumn('metric3')): [interval3] as List
        ]
        availability2.getAllAvailableIntervals() >> [
                (new MetricColumn('metric1')): [interval4] as List,
                (new MetricColumn('metric2')): [interval2] as List
        ]

        schema1.getColumns(_) >> Collections.emptySet()
        schema2.getColumns(_) >> Collections.emptySet()

        metricUnionAvailability = new MetricUnionAvailability(
                [physicalTable1, physicalTable2] as Set,
                [metricColumn1, metricColumn2] as Set
        )

        expect:
        metricUnionAvailability.getAllAvailableIntervals() == [
                (metricColumn1): [interval1, interval4] as List,
                (metricColumn2): [interval2] as List,
        ]
    }

    def "test getAvailableIntervals"() {
        given:
        availability1.getAvailableIntervals(_ as DataSourceConstraint) >> new SimplifiedIntervalList(
                Sets.newHashSet(interval1)
        )
        availability2.getAvailableIntervals(_ as DataSourceConstraint) >> new SimplifiedIntervalList(
                Sets.newHashSet(interval2, interval4)
        )

        schema1.getColumns(MetricColumn.class) >> Sets.newHashSet(metricColumn1)
        schema2.getColumns(MetricColumn.class) >> Sets.newHashSet(metricColumn2)

        metricUnionAvailability = new MetricUnionAvailability(
                [physicalTable1, physicalTable2] as Set,
                [metricColumn1, metricColumn2] as Set
        )

        DataSourceConstraint dataSourceConstraint = new DataSourceConstraint(
                Collections.emptySet(),
                Collections.emptySet(),
                Collections.emptySet(),
                ['metric1', 'metric2'] as Set,
                Collections.emptySet(),
                Collections.emptySet(),
                Collections.emptySet(),
                Collections.emptyMap()
        )

        expect:
        metricUnionAvailability.constructSubConstraint(dataSourceConstraint).size() == 2
        metricUnionAvailability.getAvailabilityToConstraintMapping(dataSourceConstraint).size() == 2
        metricUnionAvailability.getAvailableIntervals(dataSourceConstraint) == new SimplifiedIntervalList(
                Collections.emptySet()
        )
    }
}
