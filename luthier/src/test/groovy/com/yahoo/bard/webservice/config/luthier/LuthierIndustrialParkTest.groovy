package com.yahoo.bard.webservice.config.luthier

import com.yahoo.bard.webservice.data.config.ResourceDictionaries
import com.yahoo.bard.webservice.data.dimension.Dimension
import spock.lang.Specification

class LuthierIndustrialParkTest extends Specification {
    LuthierIndustrialPark industrialPark
    void setup() {
        ResourceDictionaries resourceDictionaries = new ResourceDictionaries()
        Map<String, Factory<Dimension>> dimensionFactoriesMap = new HashMap<>()
        industrialPark = new LuthierIndustrialPark(resourceDictionaries, dimensionFactoriesMap)

    }

    void cleanup() {
    }

    def "An industrialPark instance is loaded up without error."() {
        when:
            industrialPark.load()
        then:
            true
    }
}
