// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.table

import static com.yahoo.bard.webservice.data.config.names.TestLogicalTableName.SHAPES

import com.yahoo.bard.webservice.data.config.ResourceDictionaries
import com.yahoo.bard.webservice.data.config.dimension.TestDimensions
import com.yahoo.bard.webservice.data.config.names.ApiMetricName
import com.yahoo.bard.webservice.data.config.names.FieldName
import com.yahoo.bard.webservice.data.config.names.TableName
import com.yahoo.bard.webservice.data.config.names.TestApiDimensionName
import com.yahoo.bard.webservice.data.config.names.TestApiMetricName
import com.yahoo.bard.webservice.data.config.names.TestDruidMetricName
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionColumn
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService
import com.yahoo.bard.webservice.table.ConfigPhysicalTable
import com.yahoo.bard.webservice.table.PhysicalTableSchema
import com.yahoo.bard.webservice.table.physicaltables.StrictPhysicalTable
import com.yahoo.bard.webservice.table.TableGroup

import org.joda.time.DateTimeZone

import spock.lang.Specification

/**
 * Testing basic table loader functionality.
 */
class BaseTableLoadTaskSpec extends Specification {
    private static class SimpleBaseTableLoadTask extends BaseTableLoader {

        SimpleBaseTableLoadTask(DataSourceMetadataService metadataService) {
            super(metadataService)
        }

        @Override
        void loadTableDictionary(ResourceDictionaries dictionaries) {
            // stub to allow testing of abstract class in unit tests
        }
    }

    private static class SimpleDependencyPhysicalTableDefinition extends PhysicalTableDefinition {
        String dependentTableName
        ConfigPhysicalTable physicalTable

        SimpleDependencyPhysicalTableDefinition(String tableName, String dependentTableName) {
            super(TableName.of(tableName), DefaultTimeGrain.DAY.buildZonedTimeGrain(DateTimeZone.UTC), [] as Set, [] as Set)
            this.dependentTableName = dependentTableName
        }

        SimpleDependencyPhysicalTableDefinition(String tableName, ConfigPhysicalTable physicalTable) {
            super(TableName.of(tableName), DefaultTimeGrain.DAY.buildZonedTimeGrain(DateTimeZone.UTC), [] as Set, [] as Set)
            this.physicalTable = physicalTable
        }

        @Override
        Set<TableName> getDependentTableNames() {
            Objects.isNull(dependentTableName) ? [] : [TableName.of(dependentTableName)]
        }

        @Override
        ConfigPhysicalTable build(ResourceDictionaries dictionaries, DataSourceMetadataService metadataService) {
            physicalTable ?:
                    new StrictPhysicalTable(
                            TableName.of(getName().asName()),
                            DefaultTimeGrain.DAY.buildZonedTimeGrain(DateTimeZone.UTC),
                            [] as Set,
                            [:],
                            metadataService
                    )
        }
    }

    BaseTableLoader loader
    ResourceDictionaries dicts
    Set<ApiMetricName> apiNames
    Set<FieldName> metricNames
    Set<PhysicalTableDefinition> physDefs
    Set<TestApiDimensionName> dimNames
    Collection<Dimension> dims
    Set<TableName> tableNames
    Set<PhysicalTableDefinition> allDependentDefinitions

    PhysicalTableDefinition dependentDefinition1
    PhysicalTableDefinition satisfiedDefinition2
    PhysicalTableDefinition dependentDefinition3
    PhysicalTableDefinition selfDependentDefinition4
    PhysicalTableDefinition circularDependentDefinition5
    PhysicalTableDefinition circularDependentDefinition6

    ConfigPhysicalTable physicalTable
    PhysicalTableSchema physicalTableSchema

    def setup() {
        loader = new SimpleBaseTableLoadTask(Mock(DataSourceMetadataService))
        dicts = new ResourceDictionaries()
        apiNames = TestApiMetricName.getByLogicalTable(SHAPES)
        metricNames = TestDruidMetricName.getByLogicalTable(SHAPES)
        physDefs = TestPhysicalTableDefinitionUtils.buildShapeTableDefinitions(new TestDimensions(), metricNames)
        dimNames = TestApiDimensionName.getByLogicalTable(SHAPES)
        tableNames = physDefs.name as Set

        dims = dimNames.collect {
            name -> new KeyValueStoreDimension(TestDimensions.buildStandardDimensionConfig(name))
        }
        dicts.dimensionDictionary.addAll(dims)

        physicalTableSchema = Mock(PhysicalTableSchema)
        physicalTableSchema.getColumns(DimensionColumn.class) >> []

        physicalTable = Mock(ConfigPhysicalTable)
        physicalTable.name >> 'definition2'
        physicalTable.schema >> physicalTableSchema

        dependentDefinition1 = new SimpleDependencyPhysicalTableDefinition('definition1', 'definition2')
        satisfiedDefinition2 = new SimpleDependencyPhysicalTableDefinition('definition2', physicalTable)
        dependentDefinition3 = new SimpleDependencyPhysicalTableDefinition('definition3', 'definition1')
        selfDependentDefinition4 = new SimpleDependencyPhysicalTableDefinition('definition4', 'definition4')
        circularDependentDefinition5 = new SimpleDependencyPhysicalTableDefinition('definition5', 'definition6')
        circularDependentDefinition6 = new SimpleDependencyPhysicalTableDefinition('definition6', 'definition5')

        allDependentDefinitions = [dependentDefinition1, satisfiedDefinition2, dependentDefinition3, selfDependentDefinition4, circularDependentDefinition5, circularDependentDefinition6]

    }

    def "table group has correct contents after being build"() {
        when:
        TableGroup group = loader.buildDimensionSpanningTableGroup(
                tableNames,
                physDefs,
                dicts,
                apiNames
        )

        then:
        group.physicalTables.size() == physDefs.size()
        group.apiMetricNames == apiNames
    }

    def "loading distinct physical tables without dependency results in correct tables in dictionary and table group"() {
        when:
        TableGroup group = loader.buildDimensionSpanningTableGroup(
                tableNames,
                physDefs,
                dicts,
                apiNames
        )

        then:
        dicts.physicalDictionary.size() == 6
        group.physicalTables.name == tableNames*.asName()
    }

    def "loading physical tables with dependency loads all satisfied dependency physical tables"() {
        given:
        Set<TableName> currentTableNames = [dependentDefinition1, satisfiedDefinition2, dependentDefinition3].name

        when:
        TableGroup group = loader.buildDimensionSpanningTableGroup(
                currentTableNames,
                allDependentDefinitions,
                dicts,
                apiNames
        )

        then:
        dicts.physicalDictionary.size() == 3
        group.physicalTables.name == currentTableNames*.asName()
    }

    def "loading a physical table with dependency outside of the current table group will be loaded successfully"() {
        given:
        Set<TableName> currentTableNames = [satisfiedDefinition2, dependentDefinition3].name

        when:
        TableGroup group = loader.buildDimensionSpanningTableGroup(
                currentTableNames,
                allDependentDefinitions,
                dicts,
                apiNames
        )

        then:
        dicts.physicalDictionary.size() == 3
        group.physicalTables.collect { it.name } as Set == currentTableNames*.asName() as Set
    }

    def "unsatisfied dependency physical table definition loading will throw an exception"() {
        given:
        Set<TableName> currentTableNames = [satisfiedDefinition2, dependentDefinition3].name

        when:
        loader.buildDimensionSpanningTableGroup(
                currentTableNames,
                [satisfiedDefinition2, dependentDefinition3] as Set,
                dicts,
                apiNames
        )

        then:
        RuntimeException e = thrown()
        e.message == 'Unable to resolve physical table dependency for physical table: definition1, might be missing or circular dependency'
    }

    def "circular dependency physical table definition loading will throw an exception"() {
        given:
        Set<TableName> currentTableNames = [
                dependentDefinition1,
                satisfiedDefinition2,
                dependentDefinition3,
                circularDependentDefinition5,
                circularDependentDefinition6
        ].name

        when:
        loader.buildDimensionSpanningTableGroup(
                currentTableNames,
                allDependentDefinitions,
                dicts,
                apiNames
        )

        then:
        RuntimeException e = thrown()
        e.message == 'Unable to resolve physical table dependency for physical table: definition5, might be missing or circular dependency'
    }

    def "self dependency physical table definition loading will throw an exception"() {
        given:
        Set<TableName> currentTableNames = [
                dependentDefinition1,
                satisfiedDefinition2,
                dependentDefinition3,
                selfDependentDefinition4
        ].name

        when:
        loader.buildDimensionSpanningTableGroup(
                currentTableNames,
                allDependentDefinitions,
                dicts,
                apiNames
        )

        then:
        RuntimeException e = thrown()
        e.message == 'Unable to resolve physical table dependency for physical table: definition4, might be missing or circular dependency'
    }

    def "load duplicate physical tables results in sharing definitions"() {
        when:
        TableGroup group1 = loader.buildDimensionSpanningTableGroup(
                tableNames,
                physDefs,
                dicts,
                apiNames
        )
        TableGroup group2 = loader.buildDimensionSpanningTableGroup(
                tableNames,
                physDefs,
                dicts,
                apiNames
        )

        then:
        dicts.physicalDictionary.size() == 6
        group1.physicalTables.name == tableNames*.asName()
        group2.physicalTables.name == tableNames*.asName()
    }
}
