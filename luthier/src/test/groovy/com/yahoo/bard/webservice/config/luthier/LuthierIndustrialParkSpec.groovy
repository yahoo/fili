// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config.luthier

import com.yahoo.bard.webservice.config.luthier.factories.KeyValueStoreDimensionFactory
import com.yahoo.bard.webservice.data.config.ConceptType
import com.yahoo.bard.webservice.data.config.Factory
import com.yahoo.bard.webservice.data.config.LuthierIndustrialPark
import com.yahoo.bard.webservice.data.config.dimension.LuthierDimensionField
import com.yahoo.bard.webservice.data.config.LuthierResourceDictionaries
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.impl.LuceneSearchProvider
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
        when:
            Dimension testDimension = defaultIndustrialPark.getDimension("testDimension")
        then:
            testDimension != null
            testDimension.getApiName() == "testDimension"
            testDimension.getFieldByName("testPk") == new LuthierDimensionField(
                    "testPk",
                    "TEST_PK",
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
}
