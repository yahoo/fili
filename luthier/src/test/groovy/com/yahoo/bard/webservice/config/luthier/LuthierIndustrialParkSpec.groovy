package com.yahoo.bard.webservice.config.luthier


import com.yahoo.bard.webservice.config.luthier.factories.KeyValueStoreDimensionFactory
import com.yahoo.bard.webservice.data.config.LuthierDimensionField
import com.yahoo.bard.webservice.data.config.LuthierResourceDictionaries
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.impl.LuceneSearchProvider
import spock.lang.Specification

class LuthierIndustrialParkSpec extends Specification {
    LuthierIndustrialPark industrialPark
    LuthierIndustrialPark defaultIndustrialPark
    LuthierResourceDictionaries resourceDictionaries
    void setup() {
        resourceDictionaries = new LuthierResourceDictionaries()
    }

    def "An industrialPark instance built with a custom dimensionFactories map contains a particular testDimension."() {
        given:
            Map<String, Factory<Dimension>> dimensionFactoriesMap = new HashMap<>()
            dimensionFactoriesMap.put("KeyValueStoreDimension", new KeyValueStoreDimensionFactory())
            industrialPark = new LuthierIndustrialPark.Builder(resourceDictionaries)
                .withDimensionFactories(dimensionFactoriesMap)
                .build()
        when:
            industrialPark.load()
            Dimension testDimension = industrialPark.getDimension("testDimension")
            testDimension.getFieldByName("nonExistentField")
        then:
            testDimension != null
            testDimension.getApiName() == "testDimension"
            testDimension.getFieldByName("testPk") == new LuthierDimensionField(
                    "testPk",
                    "TEST_PK",
                    ["primaryKey"]
            )
            thrown(IllegalArgumentException)
    }

    def "A Lucene SearchProvider is correctly constructed through a test Dimension from the default Industrial Park"() {
        given:
            defaultIndustrialPark = new LuthierIndustrialPark.Builder(resourceDictionaries).build()
        when:
            defaultIndustrialPark.load()
            Dimension testDimension = defaultIndustrialPark.getDimension("testDimension")
            LuceneSearchProvider luceneSearchProvider = testDimension.getSearchProvider()
        then:
            luceneSearchProvider.getDimension() == testDimension
            luceneSearchProvider.luceneIndexPath == "./target/tmp/lucene/"
            luceneSearchProvider.maxResults == 100000
            luceneSearchProvider.searchTimeout == 600000
    }

    def "When a dimension name gets fetched the second time, it refers to the same object as the first one"() {
        given:
            defaultIndustrialPark = new LuthierIndustrialPark.Builder(resourceDictionaries).build()
        when:
            defaultIndustrialPark.load()
            Dimension testDimension = defaultIndustrialPark.getDimension("testDimension")
            Dimension secondTestDimension = defaultIndustrialPark.getDimension("testDimension")
            Dimension differentTestDimension = defaultIndustrialPark.getDimension("comment")
        then:
            secondTestDimension.is(testDimension)
            ! differentTestDimension.is(testDimension)
    }
}
