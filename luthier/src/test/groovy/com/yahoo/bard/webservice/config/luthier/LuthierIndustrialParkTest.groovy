package com.yahoo.bard.webservice.config.luthier

import com.yahoo.bard.webservice.config.luthier.factories.DefaultDimensionFactory

import com.yahoo.bard.webservice.data.config.ResourceDictionaries
import com.yahoo.bard.webservice.data.dimension.Dimension
import spock.lang.Specification

class LuthierIndustrialParkTest extends Specification {
    LuthierIndustrialPark industrialPark
    void setup() {
        Map<String, Factory<Dimension>> dimensionFactoriesMap = new HashMap<>()
        dimensionFactoriesMap.put("DefaultDimensionFactory", new DefaultDimensionFactory())

        ResourceDictionaries resourceDictionaries = new ResourceDictionaries()
        industrialPark = new LuthierIndustrialPark.Builder(resourceDictionaries)
                .withDimensionFactories(dimensionFactoriesMap)
                .build()

    }

    def "An industrialPark instance is loaded up without error."() {
        when:
            industrialPark.load()
        then:
            true
    }
}
