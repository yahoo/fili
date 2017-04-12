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
import com.yahoo.bard.webservice.table.ConcretePhysicalTable
import com.yahoo.bard.webservice.table.PhysicalTable
import com.yahoo.bard.webservice.table.PhysicalTableSchema
import com.yahoo.bard.webservice.table.TableGroup

import org.joda.time.DateTimeZone

import spock.lang.Specification

import java.util.stream.Collectors

/**
 * Testing basic table loader functionality.
 */
class BaseTableLoaderSpec extends Specification {
    private static class SimpleBaseTableLoader extends BaseTableLoader {

        SimpleBaseTableLoader(DataSourceMetadataService metadataService) {
            super(metadataService)
        }

        @Override
        void loadTableDictionary(ResourceDictionaries dictionaries) {
            // stub to allow testing of abstract class in unit tests
        }
    }

    private static class SimpleDependencyPhysicalTableDefinition extends PhysicalTableDefinition {
        String dependentTableName
        PhysicalTable physicalTable

        SimpleDependencyPhysicalTableDefinition(String tableName, String dependentTableName) {
            super(TableName.of(tableName), DefaultTimeGrain.DAY.buildZonedTimeGrain(DateTimeZone.UTC), [] as Set, [] as Set)
            this.dependentTableName = dependentTableName
        }

        SimpleDependencyPhysicalTableDefinition(String tableName, PhysicalTable physicalTable) {
            super(TableName.of(tableName), DefaultTimeGrain.DAY.buildZonedTimeGrain(DateTimeZone.UTC), [] as Set, [] as Set)
            this.physicalTable = physicalTable
        }

        @Override
        Set<TableName> getDependentTableNames() {
            if (Objects.isNull(dependentTableName)) {
                return Collections.emptySet()
            }
            return Collections.singleton(TableName.of(dependentTableName));
        }

        @Override
        Optional<PhysicalTable> build(
                ResourceDictionaries dictionaries,
                DataSourceMetadataService metadataService
        ) {
            if (!Objects.isNull(physicalTable)) {
                return Optional.of(physicalTable)
            }
            return dictionaries.getPhysicalDictionary().containsKey(dependentTableName) ?
                    Optional.of(
                            new ConcretePhysicalTable(
                                    TableName.of(getName().asName()),
                                    DefaultTimeGrain.DAY.buildZonedTimeGrain(DateTimeZone.UTC),
                                    [] as Set,
                                    [:],
                                    metadataService
                            )
                    ) : Optional.empty()
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

    PhysicalTableDefinition definition1
    PhysicalTableDefinition definition2
    PhysicalTableDefinition definition3
    PhysicalTableDefinition definition4
    PhysicalTableDefinition definition5
    PhysicalTableDefinition definition6

    PhysicalTable physicalTable
    PhysicalTableSchema physicalTableSchema

    def setup() {
        loader = new SimpleBaseTableLoader(Mock(DataSourceMetadataService))
        dicts = new ResourceDictionaries()
        apiNames = TestApiMetricName.getByLogicalTable(SHAPES)
        metricNames = TestDruidMetricName.getByLogicalTable(SHAPES)
        physDefs = TestPhysicalTableDefinitionUtils.buildShapeTableDefinitions(new TestDimensions(), metricNames)
        dimNames = TestApiDimensionName.getByLogicalTable(SHAPES)
        tableNames = physDefs.stream().map({it -> it.getName()}).collect(Collectors.toSet())

        dims = dimNames.collect {
            name -> new KeyValueStoreDimension(TestDimensions.buildStandardDimensionConfig(name))
        }
        dicts.getDimensionDictionary().addAll(dims)

        physicalTableSchema = Mock(PhysicalTableSchema)
        physicalTableSchema.getColumns(DimensionColumn.class) >> []

        physicalTable = Mock(PhysicalTable)
        physicalTable.getTableName() >> TableName.of('definition2')
        physicalTable.getSchema() >> physicalTableSchema

        definition1 = new SimpleDependencyPhysicalTableDefinition('definition1', 'definition2')
        definition2 = new SimpleDependencyPhysicalTableDefinition('definition2', physicalTable)
        definition3 = new SimpleDependencyPhysicalTableDefinition('definition3', 'definition1')
        definition4 = new SimpleDependencyPhysicalTableDefinition('definition4', 'definition4')
        definition5 = new SimpleDependencyPhysicalTableDefinition('definition5', 'definition6')
        definition6 = new SimpleDependencyPhysicalTableDefinition('definition6', 'definition5')

        allDependentDefinitions = [definition1, definition2, definition3, definition4, definition5, definition6]

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
        group.physicalTables.stream().map({it.getTableName()}).collect(Collectors.toSet()) as Set == tableNames
    }

    def "loading physical tables with dependency loads all satisfied dependency physical tables"() {
        given:
        Set<TableName> currentTableNames = [definition1.getName(), definition2.getName(), definition3.getName()]

        when:
        TableGroup group = loader.buildDimensionSpanningTableGroup(
                currentTableNames,
                allDependentDefinitions,
                dicts,
                apiNames
        )

        then:
        dicts.physicalDictionary.size() == 3
        group.physicalTables.stream().map({ it.getTableName() }).collect(Collectors.toSet()) == currentTableNames
    }

    def "loading a physical table with dependency outside of the current table group will be loaded successfully"() {
        given:
        Set<TableName> currentTableNames = [definition2.getName(), definition3.getName()]

        when:
        TableGroup group = loader.buildDimensionSpanningTableGroup(
                currentTableNames,
                allDependentDefinitions,
                dicts,
                apiNames
        )

        then:
        dicts.physicalDictionary.size() == 3
        group.physicalTables.stream().map({ it.getTableName() }).collect(Collectors.toSet()) == currentTableNames
    }

    def "unsatisfied dependency physical table definition loading will throw an exception"() {
        given:
        Set<TableName> currentTableNames = [definition2.getName(), definition3.getName()]

        when:
        loader.buildDimensionSpanningTableGroup(
                currentTableNames,
                [definition2, definition3] as Set,
                dicts,
                apiNames
        )

        then:
        RuntimeException e = thrown()
        e.message == 'Unable to resolve physical table dependency for physical table: definition1, might be missing or circular dependency'
    }

    def "circular dependency physical table definition loading will throw an exception"() {
        given:
        Set<TableName> currentTableNames = [definition1.getName(), definition2.getName(), definition3.getName(), definition5.getName(), definition6.getName()]

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
        Set<TableName> currentTableNames = [definition1.getName(), definition2.getName(), definition3.getName(), definition4.getName()]

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
        group1.physicalTables.stream().map({it.getTableName()}).collect(Collectors.toSet()) as Set == tableNames
        group2.physicalTables.stream().map({it.getTableName()}).collect(Collectors.toSet()) as Set == tableNames
    }
}
