package com.yahoo.bard.webservice.table

import com.yahoo.bard.webservice.data.config.ResourceDictionaries
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig
import com.yahoo.bard.webservice.data.config.names.DataSourceName
import com.yahoo.bard.webservice.data.config.names.FieldName
import com.yahoo.bard.webservice.data.config.names.TableName
import com.yahoo.bard.webservice.data.config.table.MetricUnionCompositeTableDefinition
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionColumn
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain
import com.yahoo.bard.webservice.data.time.ZonedTimeGrainSpec
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService
import com.yahoo.bard.webservice.table.availability.Availability
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

    def "test if getAvailabilitiesToMetrics maps availabilities to metrics"() {
        given:
        TableName name = TableName.of('table')
        ZonedTimeGrain timeGrain = ZonedTimeGrainSpec.DAY_UTC

        TableName firstTable = TableName.of('firstTable')
        TableName secondTable = TableName.of('secondTable')

        FieldName metric1 = Mock(FieldName)
        metric1.asName() >> "metric-1"

        FieldName metric2 = Mock(FieldName)
        metric2.asName() >> "metric-2"

        Dimension dimension = Mock(Dimension)
        dimension.apiName >> "dimension"
        
        DimensionConfig dimensionConfig = Mock(DimensionConfig)
        dimensionConfig.apiName >> "dimension"

        DimensionDictionary dimensionDictionary = new DimensionDictionary([dimension] as Set)

        MetricUnionCompositeTableDefinition metricUnionCompositeTableDefinition = new MetricUnionCompositeTableDefinition(
                name,
                timeGrain,
                [metric1, metric2] as Set,
                [firstTable, secondTable] as Set,
                [dimensionConfig] as Set
        )

        ConfigPhysicalTable firstPhysicalTable = Mock(ConfigPhysicalTable)
        ConfigPhysicalTable secondPhysicalTable = Mock(ConfigPhysicalTable)

        firstPhysicalTable.getName() >> "FirstTable"
        secondPhysicalTable.getName() >> "SecondTable"

        DataSourceName firstDataSourceName = Mock(DataSourceName)
        firstDataSourceName.asName() >> "source-1"

        DataSourceName secondDataSourceName = Mock(DataSourceName)
        secondDataSourceName.asName() >> "source-2"

        Availability firstAvailability = Mock(Availability)
        Availability secondAvailability = Mock(Availability)

        firstAvailability.dataSourceNames >> ([firstDataSourceName] as Set)
        secondAvailability.dataSourceNames >> ([secondDataSourceName] as Set)

        firstPhysicalTable.availability >> firstAvailability
        secondPhysicalTable.availability >> secondAvailability

        PhysicalTableSchema firstSchema = Mock(PhysicalTableSchema)
        PhysicalTableSchema secondSchema = Mock(PhysicalTableSchema)

        ResourceDictionaries resourceDictionaries = Mock(ResourceDictionaries)
        resourceDictionaries.getDimensionDictionary() >> dimensionDictionary

        LinkedHashSet firstMetricSet = [metric1.asName()]
        LinkedHashSet secondMetricSet = [metric2.asName()]

        firstPhysicalTable.getSchema() >> firstSchema
        secondPhysicalTable.getSchema() >> secondSchema

        firstPhysicalTable.getSchema().getTimeGrain() >> timeGrain
        secondPhysicalTable.getSchema().getTimeGrain() >> timeGrain

        firstPhysicalTable.getSchema().getMetricColumnNames() >> firstMetricSet
        secondPhysicalTable.getSchema().getMetricColumnNames() >> secondMetricSet

        resourceDictionaries.getPhysicalDictionary() >> [(firstTable.asName()) : firstPhysicalTable, (secondTable.asName()) : secondPhysicalTable]

        Map<Availability, Set<String>> expectedAvailabilitiesToMetricNames = new HashMap<>()
        expectedAvailabilitiesToMetricNames.put(firstAvailability, [metric1.asName()] as Set)
        expectedAvailabilitiesToMetricNames.put(secondAvailability, [metric2.asName()] as Set)

        Map<Availability, Set<String>> availabilitiesToMetricNames = metricUnionCompositeTableDefinition.getAvailabilitiesToMetrics(resourceDictionaries)

        expect:
        availabilitiesToMetricNames == expectedAvailabilitiesToMetricNames
    }
}
