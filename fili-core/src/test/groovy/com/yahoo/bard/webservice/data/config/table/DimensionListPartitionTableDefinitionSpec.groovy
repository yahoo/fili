// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.table

import static com.yahoo.bard.webservice.data.time.ZonedTimeGrainSpec.DAY_UTC

import com.yahoo.bard.webservice.data.config.ResourceDictionaries
import com.yahoo.bard.webservice.data.config.names.TableName
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.dimension.DimensionField
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService
import com.yahoo.bard.webservice.table.physicaltables.BaseCompositePhysicalTable
import com.yahoo.bard.webservice.table.ConfigPhysicalTable
import com.yahoo.bard.webservice.table.PhysicalTable
import com.yahoo.bard.webservice.table.PhysicalTableSchema
import com.yahoo.bard.webservice.table.availability.Availability
import com.yahoo.bard.webservice.table.availability.PartitionAvailability
import com.yahoo.bard.webservice.table.resolver.DimensionIdFilter

import spock.lang.Specification

class DimensionListPartitionTableDefinitionSpec extends Specification {

    DimensionField keyField = Mock(DimensionField)

    String dimensionName = "testDimension"

    Dimension testDimension = Mock(Dimension) {
        getApiName() >> dimensionName
        getKey() >> keyField
    }

    DimensionDictionary dimensionDictionary = new DimensionDictionary()
    ResourceDictionaries resourceDictionaries = new ResourceDictionaries()

    TableName part1Name = TableName.of("part1")
    TableName part2Name = TableName.of("part2")

    TableName dataSource1 = TableName.of("part1_ds1")
    TableName dataSource2 = TableName.of("part1_ds2")
    TableName dataSource3 = TableName.of("part2_ds1")


    Map<TableName, Map<String, Set<String>>> mappings =
            [(part1Name): [(dimensionName): ["part1Value"] as Set],
             (part2Name): [(dimensionName): ["part2Value"] as Set]]

    Availability availability1 = Mock(Availability) {
        getDataSourceNames() >> ([dataSource1, dataSource2] as Set)
    }
    Availability availability2 = Mock(Availability) {
        getDataSourceNames() >> ([dataSource3] as Set)
    }
    PhysicalTableSchema schema = Mock(PhysicalTableSchema) {
        getTimeGrain() >> DAY_UTC
    }

    PhysicalTable part1 = Mock(ConfigPhysicalTable) {
        getAvailability() >> availability1
        getSchema() >> schema
    }

    PhysicalTable part2 = Mock(ConfigPhysicalTable) {
        getAvailability() >> availability2
        getSchema() >> schema
    }

    def setup() {
        resourceDictionaries.dimensionDictionary.add(testDimension)

        resourceDictionaries.physical.put(part1Name.asName(), part1)
        resourceDictionaries.physical.put(part2Name.asName(), part2)
    }

    def "Build method duplicates the effects of the constructor"() {
        setup:
        DimensionListPartitionTableDefinition definition = new DimensionListPartitionTableDefinition(
                TableName.of("partition"),
                DAY_UTC,
                [] as Set,
                [] as Set,
                mappings
        )

        DataSourceMetadataService service = Mock(DataSourceMetadataService)

        BaseCompositePhysicalTable expected = new BaseCompositePhysicalTable(
                TableName.of("partition"),
                DAY_UTC,
                [] as Set,
                [part1, part2] as Set,
                [:],
                PartitionAvailability.build(
                        [
                                (part1): new DimensionIdFilter([(testDimension): (["part1Value"] as Set)]),
                                (part2): new DimensionIdFilter([(testDimension): (["part2Value"] as Set)])
                        ] as Map
                )
        )

        expect:
        definition.build(resourceDictionaries, service) == expected
    }
}
