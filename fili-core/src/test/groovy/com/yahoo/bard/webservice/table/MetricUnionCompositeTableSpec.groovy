// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table

import com.yahoo.bard.webservice.data.config.names.TableName
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain

import spock.lang.Specification

class MetricUnionCompositeTableSpec extends Specification {
    TableName tableName

    def setup() {
        tableName =  TableName.of("table1")
    }

    def "Constructor throws illegal argument exception on empty physical tables"() {
        when:
        MetricUnionCompositeTable metricUnionCompositeTable = new MetricUnionCompositeTable(
                tableName,
                [] as Set,
                [] as Set,
                [:]
        )

        then:
        IllegalArgumentException illegalArgumentException = thrown()
        illegalArgumentException.message == 'At least 1 physical table needs to be provided in order to calculate coarsest time grain for table1'
    }

    def "verifyGrainSatisfiesAllTables throws illegal argument exception on non-mutually satisfying grain among physical tables"() {
        given:
        ZonedTimeGrain satisfyingGrain = Mock(ZonedTimeGrain)
        ZonedTimeGrain nonSatisfyingGrain = Mock(ZonedTimeGrain)
        ZonedTimeGrain coarsestTimeGrain = Mock(ZonedTimeGrain)

        satisfyingGrain.satisfiedBy(coarsestTimeGrain) >> true
        nonSatisfyingGrain.satisfiedBy(coarsestTimeGrain) >> false

        PhysicalTableSchema schema1 = Mock(PhysicalTableSchema)
        PhysicalTableSchema schema2 = Mock(PhysicalTableSchema)

        schema1.getTimeGrain() >> satisfyingGrain
        schema2.getTimeGrain() >> nonSatisfyingGrain

        PhysicalTable physicalTable1 = Mock(PhysicalTable)
        PhysicalTable physicalTable2 = Mock(PhysicalTable)

        physicalTable1.getSchema() >> schema1
        physicalTable2.getSchema() >> schema2

        when:
        MetricUnionCompositeTable.verifyGrainSatisfiesAllTables(coarsestTimeGrain, [physicalTable1, physicalTable2] as Set, tableName)

        then:
        IllegalArgumentException illegalArgumentException = thrown()
        illegalArgumentException.message.startsWith("There is no mutually satisfying grain among")
    }
}
