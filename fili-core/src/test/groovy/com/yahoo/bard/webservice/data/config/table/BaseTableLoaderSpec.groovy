// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.table

import com.yahoo.bard.webservice.data.config.ResourceDictionaries
import com.yahoo.bard.webservice.data.config.dimension.TestDimensions
import com.yahoo.bard.webservice.data.config.names.ApiMetricName
import com.yahoo.bard.webservice.data.config.names.FieldName
import com.yahoo.bard.webservice.data.config.names.TestApiDimensionName
import com.yahoo.bard.webservice.data.config.names.TestApiMetricName
import com.yahoo.bard.webservice.data.config.names.TestDruidMetricName
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.table.TableGroup
import com.yahoo.bard.webservice.table.PhysicalTable
import spock.lang.Specification

import static com.yahoo.bard.webservice.data.config.names.TestLogicalTableName.SHAPES;

/**
 * Testing basic table loader functionality
 */
class BaseTableLoaderSpec extends Specification {
    static class SimpleBaseTableLoader extends BaseTableLoader {
        @Override
        public void loadTableDictionary(ResourceDictionaries dictionaries) {
            // stub to allow testing of abstract class in unit tests
        }
    }

    BaseTableLoader loader
    ResourceDictionaries dicts
    Set<ApiMetricName> apiNames
    Set<FieldName> metricNames
    Set<PhysicalTableDefinition> physDefs
    Set<TestApiDimensionName> dimNames
    Collection<Dimension> dims

    def setup() {
        loader = new SimpleBaseTableLoader()
        dicts = new ResourceDictionaries()
        apiNames = TestApiMetricName.getByLogicalTable(SHAPES)
        metricNames = TestDruidMetricName.getByLogicalTable(SHAPES)
        physDefs = TestPhysicalTableDefinitionUtils.buildShapeTableDefinitions(new TestDimensions())
        dimNames = TestApiDimensionName.getByLogicalTable(SHAPES)

        dims = dimNames.collect {
            name -> new KeyValueStoreDimension(TestDimensions.buildStandardDimensionConfig(name))
        }
        dicts.getDimensionDictionary().addAll(dims)
    }

    def "table group has correct contents after being build"() {
        when:
        TableGroup group = loader.buildTableGroup(
                SHAPES.asName(),
                apiNames,
                metricNames,
                physDefs,
                dicts
        )

        then:
        group.physicalTables.size() == physDefs.size()
        group.apiMetricNames == apiNames
    }

    def "loading distinct physical tables results in correct tables in dictionary"() {
        setup:
        List<PhysicalTableDefinition> allDefs = physDefs as List

        when:
        PhysicalTable t1 = loader.loadPhysicalTable(allDefs[0], metricNames, dicts)
        PhysicalTable t2 = loader.loadPhysicalTable(allDefs[1], metricNames, dicts)

        then:
        dicts.physicalDictionary.size() == 2
        dicts.physicalDictionary.containsValue(t1)
        dicts.physicalDictionary.containsValue(t2)
    }

    def "load duplicate physical tables results in sharing definitions"() {
        setup:
        List<PhysicalTableDefinition> allDefs = physDefs as List

        when:
        PhysicalTable t1 = loader.loadPhysicalTable(allDefs[0], metricNames, dicts)
        PhysicalTable t2 = loader.loadPhysicalTable(allDefs[0], metricNames, dicts)

        then:
        dicts.physicalDictionary.size() == 1
        dicts.physicalDictionary.containsValue(t1)
        t1.is(t2)
    }
}
