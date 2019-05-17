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

class FlagFromTagDimensionSpec extends Specification {

    DimensionDictionary dimensionDictionary
    FlagFromTagDimensionConfig config

    String apiName
    String filteringApiName
    String groupingApiName

    def setup() {
        apiName = "flagFromTag"
        filteringApiName = "filteringDimensionApiName"
        groupingApiName = "groupingDimensionApiName"
        String tagValue = "TAG_VALUE"
        String trueValue = "TRUE_VALUE"
        String falseValue = "FALSE_VALUE"

        config = new FlagFromTagDimensionConfig(
                {apiName},
                "description",
                "longName",
                "category",
                filteringApiName,
                groupingApiName,
                tagValue,
                trueValue,
                falseValue
        )

        dimensionDictionary = new DimensionDictionary()
        // filtering dimension
        dimensionDictionary.add(Mock(Dimension) {getApiName() >> filteringApiName})
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
        Dimension fft = new FlagFromTagDimension(config, dimensionDictionary)

        then:
        fft.groupingDimension instanceof RegisteredLookupDimension
        fft.groupingDimension.registeredLookupExtractionFns.size() == extractionFunctions.size() + 1
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
        Dimension fft = new FlagFromTagDimension(config, dimensionDictionary)

        then:
        fft.groupingDimension instanceof RegisteredLookupDimension
        fft.groupingDimension.registeredLookupExtractionFns.size() == 1
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
        Dimension fft = new FlagFromTagDimension(config, dimensionDictionary)

        then:
        fft.groupingDimension instanceof RegisteredLookupDimension
        fft.groupingDimension.registeredLookupExtractionFns.size() == 1
        fft.groupingDimension.getKeyValueStore() == null
        fft.getSearchProvider() instanceof MapSearchProvider
    }
}
