// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.physicaltables

import com.yahoo.bard.webservice.data.config.names.TableName
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain
import com.yahoo.bard.webservice.table.PhysicalTable
import com.yahoo.bard.webservice.table.PhysicalTableSchema
import com.yahoo.bard.webservice.table.availability.Availability

import spock.lang.Specification

class BaseCompositePhysicalTableSpec extends Specification {

    def "verifyGrainSatisfiesAllTables throws illegal argument exception on non-mutually satisfying grain among physical tables"() {
        given:

        ZonedTimeGrain coarsestTimeGrain = Mock(ZonedTimeGrain) {
            toString() >> "MockTimeGrain"
        }
        String tableName1 = "satisfyingTable"
        String tableName2 = "notMatchingTimeGrainTable"

        ZonedTimeGrain satisfyingGrain = Mock(ZonedTimeGrain) {
            satisfies(coarsestTimeGrain) >> true
        }
        ZonedTimeGrain nonSatisfyingGrain = Mock(ZonedTimeGrain) {
            satisfies(coarsestTimeGrain) >> false
        }

        PhysicalTableSchema schema1 = Mock(PhysicalTableSchema) {
            getTimeGrain() >> satisfyingGrain
        }
        PhysicalTableSchema schema2 = Mock(PhysicalTableSchema) {
            getTimeGrain() >> nonSatisfyingGrain
        }

        PhysicalTable physicalTable1 = Mock(PhysicalTable) {
            getSchema() >> schema1
            getName() >> tableName1
        }
        PhysicalTable physicalTable2 = Mock(PhysicalTable) {
            getSchema() >> schema2
            getName() >> tableName2
        }

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
        illegalArgumentException.message.contains("cannot be satisfied by")
        illegalArgumentException.message.contains("notMatchingTimeGrainTable")
        ! illegalArgumentException.message.contains("satisfyingTable")
    }
}
