// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table

import com.yahoo.bard.webservice.data.config.names.TableName
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain
import com.yahoo.bard.webservice.table.availability.Availability

import spock.lang.Specification

class BaseCompositePhysicalTableSpec extends Specification {

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
        new BaseCompositePhysicalTable(
                TableName.of("test"),
                coarsestTimeGrain,
                Collections.emptySet(),
                [physicalTable1, physicalTable2] as Set,
                [:],
                Mock(Availability)
                ) {}

        then:
        IllegalArgumentException illegalArgumentException = thrown()
        illegalArgumentException.message.startsWith("There is no mutually satisfying grain among")
    }
}
