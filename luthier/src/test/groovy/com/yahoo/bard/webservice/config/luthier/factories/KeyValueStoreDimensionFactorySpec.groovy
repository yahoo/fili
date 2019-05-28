package com.yahoo.bard.webservice.config.luthier.factories

import com.yahoo.bard.webservice.config.luthier.Factory
import com.yahoo.bard.webservice.config.luthier.LuthierIndustrialPark
import com.yahoo.bard.webservice.data.config.LuthierDimensionField
import com.yahoo.bard.webservice.data.config.LuthierResourceDictionaries
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionField
import com.yahoo.bard.webservice.data.dimension.SearchProvider
import com.yahoo.bard.webservice.data.dimension.impl.LuceneSearchProvider
import spock.lang.Specification

class KeyValueStoreDimensionFactorySpec extends Specification {
    LuthierIndustrialPark park
    Dimension testDimension
    void setup() {
        Map<String, Factory<Dimension>> dimensionFactoriesMap = new HashMap<>()
        dimensionFactoriesMap.put("KeyValueStoreDimension", new KeyValueStoreDimensionFactory())

        LuthierResourceDictionaries resourceDictionaries = new LuthierResourceDictionaries()
        park = new LuthierIndustrialPark.Builder(resourceDictionaries)
                .withDimensionFactories(dimensionFactoriesMap)
                .build()
        park.load()
        testDimension = park.getDimension("testDimension")
    }

    def "The spec content of a specific test dimension matches with the JSON config file and defaults"() {
        when:
            // set up for: fields content correctness
            LinkedHashSet<DimensionField> dimensionFields = testDimension.getDimensionFields()
            LuthierDimensionField key = testDimension.getKey()

            SearchProvider searchProvider = testDimension.getSearchProvider()
        then:
            // basic String correctness
            testDimension.getApiName() == "testDimension"
            testDimension.getLongName() == "a longName for testing"
            testDimension.getCategory() == "a category for testing"
            testDimension.getDescription() == "a description for testing"
            testDimension.getStorageStrategy().getApiName() == "loaded"
            testDimension.isAggregatable() == false

            // Fields content correctness
            List expectedCamelNames = ["testPk", "testField1", "testField2", "testField3"]
            List expectedDescriptions = ["TEST_PK", "TEST_FIELD_1", "TEST_FIELD_2", "TEST_FIELD_3"]
            List expectedFieldTags = [ ["primaryKey"], [], [], [] ]
            // next line is to be uncommented after we implemented the description correctly
            // List expectedFieldDescriptions = [ "", "", "", "" ]
            for (int i = 0; i < dimensionFields.size(); i++) {
                assert dimensionFields[i].getName() == expectedCamelNames[i]
                assert dimensionFields[i].getTags() == expectedFieldTags[i]
                assert dimensionFields[i].getDescription() == expectedDescriptions[i]
            }
            key == dimensionFields[0]

            // type correctness of searchProvider
            // we cannnot check keyValueStore directly
            searchProvider.getClass() == LuceneSearchProvider
    }

    def "When new dimension's name match with the previous dimension, use a reference to the previous one"() {
        when:
            Dimension duplicatedDimension = park.getDimension("testDimension")
        then:
            duplicatedDimension.is(testDimension)
    }
}
