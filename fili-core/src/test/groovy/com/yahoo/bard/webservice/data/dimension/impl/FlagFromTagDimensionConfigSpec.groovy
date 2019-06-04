// Copyright 2019 Verizon Media Group.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.impl

import com.yahoo.bard.webservice.data.config.dimension.FlagFromTagDimensionConfig
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.CascadeExtractionFunction
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.ExtractionFunction
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.LookupExtractionFunction
import com.yahoo.bard.webservice.util.DimensionStoreKeyUtils

import spock.lang.Specification
import spock.lang.Unroll

class FlagFromTagDimensionConfigSpec extends Specification {

    DimensionDictionary dimensionDictionary

    String trueValue, falseValue

    def setup() {
        trueValue = "TRUE_VALUE"
        falseValue = "FALSE_VALUE"

        dimensionDictionary = new DimensionDictionary()
        // filtering dimension
        dimensionDictionary.add(Mock(Dimension) {getApiName() >> "filteringDimensionApiName"})

    }

    FlagFromTagDimensionConfig getConfig(List<ExtractionFunction> baseExtractionFunctions) {
        FlagFromTagDimensionConfig.Builder builder = new FlagFromTagDimensionConfig.Builder(
                {"flagFromTag"},
                "physicalName",
                "description",
                "longName",
                "category",
                "filteringDimensionApiName",
                "TAG_VALUE",
        )
        return builder
                .extractionFunctions(baseExtractionFunctions)
                .trueValue(trueValue)
                .falseValue(falseValue)
                .build()
    }

    @Unroll
    def "grouping dimension successfully constructed from RegisteredLookupDimension"() {
        when:
        Dimension fft = new FlagFromTagDimension(getConfig(extractionFunctions), dimensionDictionary)

        then:
        fft.getRegisteredLookupExtractionFns().size() == extractionFunctions.size() + 2
        fft.getSearchProvider() instanceof MapSearchProvider

        where:
        extractionFunctions << [
                [] as List<ExtractionFunction>,
                [Mock(CascadeExtractionFunction), Mock(LookupExtractionFunction)] as List<ExtractionFunction>,
        ]
    }

    def "Map store is created properly"() {
        when:
        Dimension fft = new FlagFromTagDimension(getConfig([]), dimensionDictionary)

        then:
        fft.getKeyValueStore().get(DimensionStoreKeyUtils.getRowKey(fft.getKey().getName(), trueValue)) == /{"id":"${trueValue}"}/
        fft.getKeyValueStore().get(DimensionStoreKeyUtils.getRowKey(fft.getKey().getName(), falseValue)) == /{"id":"${falseValue}"}/
    }

    def "Search provider is created properly"() {
        when:
        Dimension fft = new FlagFromTagDimension(getConfig([]), dimensionDictionary)
        Set<String> expectedValues = [trueValue, falseValue] as Set

        then:
        fft.getSearchProvider().findAllDimensionRows().stream().allMatch({ row -> expectedValues.remove(row.get(fft.getKey())) })
    }
}
