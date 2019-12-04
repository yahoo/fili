// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.luthier


import com.yahoo.bard.webservice.data.config.luthier.dimension.LuthierDimensionField
import com.yahoo.bard.webservice.data.config.luthier.factories.dimension.KeyValueStoreDimensionFactory
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.KeyValueStore
import com.yahoo.bard.webservice.data.dimension.SearchProvider
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.dimension.impl.LuceneSearchProvider
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.time.AllGranularity
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.table.LogicalTableDictionary
import com.yahoo.bard.webservice.table.TableIdentifier

import spock.lang.Specification

class LuthierIndustrialParkSpec extends Specification {
    LuthierIndustrialPark defaultIndustrialPark
    LuthierResourceDictionaries resourceDictionaries
    void setup() {
        resourceDictionaries = new LuthierResourceDictionaries()
        defaultIndustrialPark = new LuthierIndustrialPark.Builder(resourceDictionaries).build()
        defaultIndustrialPark.load()
    }

    def "An industrialPark instance built with a custom dimensionFactories map contains a particular testDimension."() {
        given:
            Map<String, Factory<Dimension>> dimensionFactoriesMap = new HashMap<>()
            dimensionFactoriesMap.put("KeyValueStoreDimension", new KeyValueStoreDimensionFactory())
            LuthierIndustrialPark industrialPark = new LuthierIndustrialPark.Builder(resourceDictionaries)
                .withFactories(ConceptType.DIMENSION, dimensionFactoriesMap)
                .build()
            assert industrialPark
        when:
            Dimension testDimension = defaultIndustrialPark.getDimension("testDimension")
        then:
            testDimension != null
            testDimension.getApiName() == "testDimension"
            println("Dimension fields for testDimension: " + testDimension.getDimensionFields())
            testDimension.getFieldByName("id") == new LuthierDimensionField(
                    "id",
                    "id",
                    ["primaryKey"]
            )
    }

    def "IllegalArgumentException is thrown correctly when try to get a non-existent field from an existing dimension"() {
        when:
            Dimension testDimension = defaultIndustrialPark.getDimension("testDimension")
            testDimension.getFieldByName("nonExistentField")
        then:
            thrown(IllegalArgumentException)
    }

    def "A Lucene SearchProvider is correctly constructed through a test Dimension from the default Industrial Park"() {
        when:
            Dimension testDimension = defaultIndustrialPark.getDimension("testDimension")
            LuceneSearchProvider luceneSearchProvider = testDimension.getSearchProvider()
        then:
            luceneSearchProvider.getDimension() == testDimension
            luceneSearchProvider.luceneIndexPath == "./target/tmp/lucene/"
            luceneSearchProvider.maxResults == 100000
            luceneSearchProvider.searchTimeout == 600000
    }

    def "When a dimension name gets fetched the second time, it refers to the same object as the first one"() {
        when:
            Dimension testDimension = defaultIndustrialPark.getDimension("testDimension")
            Dimension secondTestDimension = defaultIndustrialPark.getDimension("testDimension")
            Dimension differentTestDimension = defaultIndustrialPark.getDimension("comment")
        then:
            secondTestDimension.is(testDimension)
            ! differentTestDimension.is(testDimension)
    }

    def "When a metric name is fetched while one is already in cache, it returns the cached value"() {
        when:
        LogicalMetric logicalMetric = Mock(LogicalMetric)
        resourceDictionaries.getMetricDictionary().put("test", logicalMetric)

        then:
        defaultIndustrialPark.getMetric("test").is(logicalMetric)
    }

    def "Verify that a sample table builds"() {
        given:
        TableIdentifier tableIdentifier = new TableIdentifier("air_quality", DefaultTimeGrain.DAY)
        expect:
        LogicalTable logicalTable = defaultIndustrialPark.getLogicalTable(tableIdentifier)
        logicalTable != null
        logicalTable.getDescription() == "air_quality"
        ! logicalTable.getLogicalMetrics().isEmpty()
        ! logicalTable.getDimensions().isEmpty()
    }

    def "Verify that a table builds the same way twice"() {
        when: "A table is built"
        TableIdentifier tableIdentifier = new TableIdentifier("air_quality", DefaultTimeGrain.DAY)
        LogicalTable logicalTable1 = defaultIndustrialPark.getLogicalTable(tableIdentifier)

        and: "we remove all of tje tables from the dictionary"
        LogicalTableDictionary logicalTableDictionary = defaultIndustrialPark.getDictionaries().getLogicalDictionary();
        Set<TableIdentifier> tableIdentifiers = new HashSet<>(logicalTableDictionary.keySet())
        tableIdentifiers.stream().forEach({logicalTableDictionary.remove(it)})

        and: "we build it again"
        LogicalTable logicalTable2 = defaultIndustrialPark.getLogicalTable(tableIdentifier)


        then: "the new table is equal to the old one, but is not the same one"
        logicalTable1 == logicalTable2
        ! logicalTable1.is(logicalTable2)

        when:  "and when the park fetches it again"
        LogicalTable logicalTable3 = defaultIndustrialPark.getLogicalTable(tableIdentifier)

        then: "it's the same object"
        logicalTable3.is(logicalTable2)
    }

    def "Verify that all tables in a table group build at once"() {
        when:
        TableIdentifier tableIdentifier = new TableIdentifier("air_quality", DefaultTimeGrain.DAY)
        LogicalTable logicalTable1 = defaultIndustrialPark.getLogicalTable(tableIdentifier)
        TableIdentifier tableIdentifier2 = new TableIdentifier("air_quality", AllGranularity.INSTANCE)

        then:
        defaultIndustrialPark.getLogicalTableDictionary().get(tableIdentifier2) != null
    }

    def "Verify that a search provider and domain are cached after dimension build"() {
        when:
        String name = "PT08.S5(O3)"
        KeyValueStoreDimension dimension = (KeyValueStoreDimension) defaultIndustrialPark.getDimension(name)
        SearchProvider searchProvider = defaultIndustrialPark.getSearchProvider(name)
        KeyValueStore keyValueStore = defaultIndustrialPark.getKeyValueStore(name)

        then:
        dimension.getKeyValueStore().is(keyValueStore)
        dimension.getSearchProvider().is(searchProvider)
    }
}
