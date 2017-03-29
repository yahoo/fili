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
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService
import com.yahoo.bard.webservice.table.ConcretePhysicalTable
import com.yahoo.bard.webservice.table.PhysicalTable
import com.yahoo.bard.webservice.table.TableGroup

import org.joda.time.DateTimeZone

import spock.lang.Specification

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

    PhysicalTableDefinition definition1
    PhysicalTableDefinition definition2
    PhysicalTableDefinition definition3
    PhysicalTableDefinition definition4
    PhysicalTableDefinition definition5
    PhysicalTableDefinition definition6

    PhysicalTable physicalTable

    def setup() {
        loader = new SimpleBaseTableLoader(Mock(DataSourceMetadataService))
        dicts = new ResourceDictionaries()
        apiNames = TestApiMetricName.getByLogicalTable(SHAPES)
        metricNames = TestDruidMetricName.getByLogicalTable(SHAPES)
        physDefs = TestPhysicalTableDefinitionUtils.buildShapeTableDefinitions(new TestDimensions(), metricNames)
        dimNames = TestApiDimensionName.getByLogicalTable(SHAPES)

        dims = dimNames.collect {
            name -> new KeyValueStoreDimension(TestDimensions.buildStandardDimensionConfig(name))
        }
        dicts.getDimensionDictionary().addAll(dims)

        physicalTable = Mock(PhysicalTable)
        physicalTable.getName() >> 'definition2'

        definition1 = new SimpleDependencyPhysicalTableDefinition('definition1', 'definition2')
        definition2 = new SimpleDependencyPhysicalTableDefinition('definition2', physicalTable)
        definition3 = new SimpleDependencyPhysicalTableDefinition('definition3', 'definition1')
        definition4 = new SimpleDependencyPhysicalTableDefinition('definition4', 'definition4')
        definition5 = new SimpleDependencyPhysicalTableDefinition('definition5', 'definition6')
        definition6 = new SimpleDependencyPhysicalTableDefinition('definition6', 'definition5')
    }

    def "table group has correct contents after being build"() {
        when:
        TableGroup group = loader.buildDimensionSpanningTableGroup(
                apiNames,
                physDefs,
                dicts
        )

        then:
        group.physicalTables.size() == physDefs.size()
        group.apiMetricNames == apiNames
    }


    def "loading distinct physical tables without dependency results in correct tables in dictionary"() {
        given:
        List<PhysicalTableDefinition> allDefs = physDefs as List

        when:
        Set<PhysicalTable> tables = loader.loadPhysicalTablesWithDependency(allDefs as Set, dicts)

        then:
        dicts.physicalDictionary.size() == 6
        dicts.physicalDictionary.values() as Set == tables
    }

    def "loading physical tables with dependency loads all satisfied dependency physical tables"() {
        given:
        Set<PhysicalTableDefinition> tableDefinitions = [definition1, definition2, definition3]
        LinkedHashSet<PhysicalTable> tables = loader.loadPhysicalTablesWithDependency(tableDefinitions, dicts)

        expect:
        dicts.physicalDictionary.size() == 3
        dicts.physicalDictionary.values() as Set == tables as Set
    }

    def "unsatisfied dependency physical table definition loading will throw an exception"() {
        given:
        Set<PhysicalTableDefinition> tableDefinitions = [definition2, definition3]

        when:
        loader.loadPhysicalTablesWithDependency(tableDefinitions, dicts)

        then:
        RuntimeException e = thrown()
        e.message == 'Unable to resolve physical table dependency for physical table: definition1'
    }

    def "circular dependency physical table definition loading will throw an exception"() {
        given:
        Set<PhysicalTableDefinition> tableDefinitions = [definition1, definition2, definition3, definition5, definition6]

        when:
        loader.loadPhysicalTablesWithDependency(tableDefinitions, dicts)

        then:
        RuntimeException e = thrown()
        e.message == 'Unable to resolve physical table dependency for physical table: definition5'
    }

    def "self dependency physical table definition loading will throw an exception"() {
        given:
        Set<PhysicalTableDefinition> tableDefinitions = [definition1, definition2, definition3, definition4]

        when:
        loader.loadPhysicalTablesWithDependency(tableDefinitions, dicts)

        then:
        RuntimeException e = thrown()
        e.message == 'Unable to resolve physical table dependency for physical table: definition4'
    }

    def "load duplicate physical tables results in sharing definitions"() {
        given:
        List<PhysicalTableDefinition> allDefs = physDefs as List

        when:
        loader.loadPhysicalTablesWithDependency([allDefs[0], allDefs[4]] as Set, dicts)
        LinkedHashSet<PhysicalTable> tables = loader.loadPhysicalTablesWithDependency(
                allDefs as Set,
                dicts
        )

        then:
        dicts.physicalDictionary.size() == 6
        dicts.physicalDictionary.values() as Set == tables
    }
}
