package com.yahoo.bard.webservice.table

import com.yahoo.bard.webservice.data.config.ResourceDictionaries
import com.yahoo.bard.webservice.data.config.names.TableName
import com.yahoo.bard.webservice.data.config.table.MetricUnionCompositeTableDefinition
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain
import com.yahoo.bard.webservice.data.time.ZonedTimeGrainSpec

import spock.lang.Specification

class MetricUnionCompositeTableDefinitionSpec extends Specification {
    
    def "getPhysicalTables filters out dependent tables that does not exist in ResourceDictionaries"() {
        given:
        TableName name = TableName.of('table')
        ZonedTimeGrain timeGrain = ZonedTimeGrainSpec.DAY_UTC

        TableName existing = TableName.of('existing')
        TableName nonExisting = TableName.of('nonExisting')

        MetricUnionCompositeTableDefinition metricUnionCompositeTableDefinition = new MetricUnionCompositeTableDefinition(
                name,
                timeGrain,
                [] as Set,
                [existing, nonExisting] as Set,
                [] as Set
        )

        ConfigPhysicalTable physicalTable = Mock(ConfigPhysicalTable)
        ResourceDictionaries resourceDictionaries = Mock(ResourceDictionaries)
        resourceDictionaries.getPhysicalDictionary() >> [(existing.asName()) : physicalTable]

        expect:
        metricUnionCompositeTableDefinition.getPhysicalTables(resourceDictionaries) == [physicalTable] as Set
    }
}
