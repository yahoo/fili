// Copyright 2019 Verizon Media Group.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.impl

import com.yahoo.bard.webservice.data.config.dimension.DefaultDimensionField
import com.yahoo.bard.webservice.data.config.dimension.DefaultKeyValueStoreDimensionConfig
import com.yahoo.bard.webservice.data.config.dimension.DefaultRegisteredLookupDimensionConfig
import com.yahoo.bard.webservice.data.config.dimension.FlagFromTagDimensionConfig
import com.yahoo.bard.webservice.data.config.dimension.RegisteredLookupDimensionConfig
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.dimension.KeyValueStore
import com.yahoo.bard.webservice.data.dimension.SearchProvider
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.CascadeExtractionFunction
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.ExtractionFunction
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.LookupExtractionFunction

import spock.lang.Specification
import spock.lang.Unroll

//TODO refactor this to directly test construction in the config
class FlagFromTagDimensionConfigSpec extends Specification {

    DimensionDictionary dimensionDictionary

    String apiName
    String filteringApiName
    String groupingApiName

    def setup() {
        dimensionDictionary = new DimensionDictionary()
        // filtering dimension
        dimensionDictionary.add(Mock(Dimension) {getApiName() >> filteringApiName})

        apiName = "flagFromTag"
        filteringApiName = "filteringDimensionApiName"
        groupingApiName = "groupingDimensionApiName"
    }

    FlagFromTagDimensionConfig getConfig(String physicalName) {
        FlagFromTagDimensionConfig.build(
                {apiName},
                physicalName,
                "description",
                "longName",
                "category",
                [DefaultDimensionField.ID] as LinkedHashSet,
                [DefaultDimensionField.ID] as LinkedHashSet,
                filteringApiName,
                groupingApiName,
                "TAG_VALUE",
                "TRUE_VALUE",
                "FALSE_VALUE",
                dimensionDictionary
        )
    }

    @Unroll
    def "grouping dimension successfully constructed from RegisteredLookupDimension"() {
        setup:
        RegisteredLookupDimensionConfig rldConfig = new DefaultRegisteredLookupDimensionConfig(
                {groupingApiName},
                "rldPhysicalName",
                "rldDescription",
                "rldLongName",
                "rldCategory",
                [DefaultDimensionField.ID] as LinkedHashSet,
                [DefaultDimensionField.ID] as LinkedHashSet,
                Mock(KeyValueStore),
                Mock(SearchProvider),
                extractionFunctions
        )
        dimensionDictionary.add(new RegisteredLookupDimension(rldConfig))

        when:
        Dimension fft = new FlagFromTagDimension(getConfig("rldPhysicalName"))

        then:
        fft.groupingDimension instanceof RegisteredLookupDimension
        fft.groupingDimension.registeredLookupExtractionFns.size() == extractionFunctions.size() + 2
        fft.getSearchProvider() instanceof MapSearchProvider

        where:
        extractionFunctions << [
                [] as List<ExtractionFunction>,
                [Mock(CascadeExtractionFunction), Mock(LookupExtractionFunction)] as List<ExtractionFunction>,
        ]
    }

    def "grouping dimension successfully constructed from KeyValueStore dimension"() {
        setup:
        KeyValueStore kvs = Mock()
        DefaultKeyValueStoreDimensionConfig kvsConfig = new DefaultKeyValueStoreDimensionConfig(
                {groupingApiName},
                "kvsPhysicalName",
                "kvsDescription",
                "kvsLongName",
                "kvsCategory",
                [DefaultDimensionField.ID] as LinkedHashSet,
                [DefaultDimensionField.ID] as LinkedHashSet,
                kvs,
                Mock(SearchProvider)
        )
        dimensionDictionary.add(new KeyValueStoreDimension((kvsConfig)))

        when:
        Dimension fft = new FlagFromTagDimension(getConfig("kvsPhysicalName"))

        then:
        fft.groupingDimension instanceof RegisteredLookupDimension
        fft.groupingDimension.registeredLookupExtractionFns.size() == 2
        fft.groupingDimension.getKeyValueStore() == kvs
        fft.getSearchProvider() instanceof MapSearchProvider
    }

    def "grouping dimension successfully constructed from arbitrary dimension implementation"() {
        setup:
        Dimension testDim = Mock()
        testDim.getApiName() >> groupingApiName
        testDim.getDescription() >> "testDimDescription"
        testDim.getLongName() >> "testDimLongName"
        testDim.getCategory() >> "testDimCategory"
        testDim.getDimensionFields() >> { [DefaultDimensionField.ID] as LinkedHashSet }
        testDim.getDefaultDimensionFields() >> { [DefaultDimensionField.ID] as LinkedHashSet }
        testDim.getSearchProvider() >> Mock(SearchProvider)

        dimensionDictionary.add(testDim)

        when:
        Dimension fft = new FlagFromTagDimension(getConfig("unused_physical_name"))

        then:
        fft.groupingDimension instanceof RegisteredLookupDimension
        fft.groupingDimension.registeredLookupExtractionFns.size() == 2
        fft.groupingDimension.getKeyValueStore() != null
        fft.getSearchProvider() instanceof MapSearchProvider
    }
}
