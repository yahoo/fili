package com.yahoo.bard.webservice.table

import com.yahoo.bard.webservice.data.config.ResourceDictionaries
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig
import com.yahoo.bard.webservice.data.config.names.ApiMetricName
import com.yahoo.bard.webservice.data.config.names.TableName
import com.yahoo.bard.webservice.data.config.table.MetricUnionCompositeTableDefinition
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionColumn
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.metric.MetricColumn
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain
import com.yahoo.bard.webservice.data.time.ZonedTimeGrainSpec
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService
import com.yahoo.bard.webservice.table.availability.Availability

import spock.lang.Shared
import spock.lang.Specification

class MetricUnionCompositeTableDefinitionSpec extends Specification {

    static String metricName1 = "metric-1"
    static String metricName2 = "metric-2"
    static String metricName3 = "metric-3"


    static String tableName1 = "table1"
    static String tableName2 = "table2"
    static String tableName12 = "table12"
    static String tableName3 = "table3"

    @Shared
    ApiMetricName apiMetricName1 = ApiMetricName.of(metricName1)
    @Shared
    ApiMetricName apiMetricName2 = ApiMetricName.of(metricName2)
    @Shared
    ApiMetricName apiMetricName3 = ApiMetricName.of(metricName3)

    ZonedTimeGrain timeGrain = ZonedTimeGrainSpec.DAY_UTC

    TableName metric1Table = TableName.of(tableName1)
    TableName metric2Table = TableName.of(tableName2)
    TableName metric12Table = TableName.of(tableName12)
    TableName metric3Table = TableName.of(tableName3)

    ConfigPhysicalTable table1 = Mock(ConfigPhysicalTable)
    ConfigPhysicalTable table2 = Mock(ConfigPhysicalTable)
    ConfigPhysicalTable table12 = Mock(ConfigPhysicalTable)
    ConfigPhysicalTable table3 = Mock(ConfigPhysicalTable)

    MetricColumn metricColumn1 = new MetricColumn(metricName1)
    MetricColumn metricColumn2 = new MetricColumn(metricName2)
    MetricColumn metricColumn3 = new MetricColumn(metricName3)

    DimensionDictionary dimensionDictionary
    PhysicalTableDictionary physicalTableDictionary
    ResourceDictionaries resourceDictionaries = Mock(ResourceDictionaries)

    DataSourceMetadataService dataSourceMetadataService = Mock(DataSourceMetadataService)
    def setup() {
        resourceDictionaries.getDimensionDictionary() >> dimensionDictionary

        ApiMetricName.of(metricName1)
        TableName name = TableName.of('table')

        Dimension dimension = Mock(Dimension)
        dimension.apiName >> "dimension"

        DimensionConfig dimensionConfig = Mock(DimensionConfig)
        dimensionConfig.apiName >> "dimension"
        DimensionColumn dimensionColumn = new DimensionColumn(dimension)


        Schema table1Schema = new PhysicalTableSchema(timeGrain, [dimensionColumn, metricColumn1], [:])
        Schema table2Schema = new PhysicalTableSchema(timeGrain, [dimensionColumn, metricColumn2], [:])
        Schema table12Schema = new PhysicalTableSchema(timeGrain, [dimensionColumn, metricColumn1, metricColumn2], [:])
        Schema table3Schema = new PhysicalTableSchema(timeGrain, [dimensionColumn, metricColumn3], [:])

        dimensionDictionary = new DimensionDictionary([dimension] as Set)
        table1.getName() >> metric1Table.asName()
        table1.getSchema() >> table1Schema

        table2.getName() >> metric2Table.asName()
        table2.getSchema() >> table2Schema

        table12.getName() >> metric12Table.asName()
        table12.getSchema() >> table12Schema

        table3.getName() >> metric3Table.asName()
        table3.getSchema() >> table3Schema

        Availability availability = Mock(Availability)

        Set<ConfigPhysicalTable> tables = [table1, table2, table3, table12]

        tables.each {
            it.availability >> availability
        }

        resourceDictionaries.getDimensionDictionary() >> dimensionDictionary
        Map<String, ConfigPhysicalTable> tableMap = tables.collectEntries {
            [(it.getName()): it ]
        }

        physicalTableDictionary = new PhysicalTableDictionary(tableMap)
        resourceDictionaries.getPhysicalDictionary() >> physicalTableDictionary
    }

    def "mapNamestoTables throws exception if dependent tables do not exist in ResourceDictionaries"() {
        given:

        when:
        MetricUnionCompositeTableDefinition.mapNamestoTables(
                [metric1Table.asName(), 'nonExisting'],
                resourceDictionaries.getPhysicalDictionary()
        )

        then:
        thrown(IllegalArgumentException)
    }

    def "mapNamestoTables binds to dictionary tables"() {
        given:
        Set<Table> tables = [table1, table2]

        expect:
        MetricUnionCompositeTableDefinition.mapNamestoTables(tables.collect {it.getName()}, physicalTableDictionary) == tables
    }


    def "getTableToMetricsMap groups #metrics to #expected"() {
        // TODO
        setup:

        expect:
        true

        where:
        metrics                     | tables              | expected
        [metricName1, metricName2]  | [tableName1, tableName2]    | [(tableName1): [metricName1], (tableName2): [metricName2]]
        [metricName1, metricName2]  | [tableName12, tableName3]   | [(tableName1): [metricName1, metricName2], (tableName3): []]

    }

    def "validateDependentMetrics throws exception"() {
        // TODO
        setup:

        expect:
        true

        where:
        metrics                                                         | comment
        [(tableName1): [metricName1, metricName2]]                      | "Not all metrics on tables"
        [(tableName1): [metricName1], (tableName12): [metricName1] ]    | "Metrics are on more than one table"
    }


    def "build produces expected metric union"() {
        //TODO
    }
}
