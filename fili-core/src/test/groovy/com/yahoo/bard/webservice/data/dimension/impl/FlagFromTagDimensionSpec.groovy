// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.impl

import com.yahoo.bard.webservice.data.config.ResourceDictionaries
import com.yahoo.bard.webservice.data.config.dimension.DefaultDimensionField
import com.yahoo.bard.webservice.data.config.dimension.FlagFromTagDimensionConfig
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.SearchProvider
import com.yahoo.bard.webservice.web.ApiFilter
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException
import com.yahoo.bard.webservice.web.DefaultFilterOperation

import spock.lang.Specification

class FlagFromTagDimensionSpec extends Specification {
    FlagFromTagDimension fft
    Dimension fftBaseDim, otherDim
    ResourceDictionaries dictionaries

    def setup() {
        fftBaseDim = Mock(Dimension)
        fftBaseDim.getApiName() >> "baseDimension"
        fftBaseDim.getDescription() >> "unused"
        fftBaseDim.getLongName() >> "unused"
        fftBaseDim.getCategory() >> "unused"
        fftBaseDim.getKey() >> DefaultDimensionField.ID
        fftBaseDim.getDimensionFields() >> []
        fftBaseDim.getDefaultDimensionFields() >> []
        fftBaseDim.getSearchProvider() >> Mock(SearchProvider)

        otherDim = Mock() { getApiName() >> "otherDim" }

        dictionaries = new ResourceDictionaries()
        dictionaries.dimensionDictionary.add(fftBaseDim)
        dictionaries.dimensionDictionary.add(otherDim)

        FlagFromTagDimensionConfig.Builder builder = new FlagFromTagDimensionConfig.Builder(
                {"flagFromTag"},
                "baseDimension",
                "fftDescription",
                "fftLongName",
                "fftCategory",
                "baseDimension", // filtering
                "TAG_VALUE"
        )
        FlagFromTagDimensionConfig fftConfig = builder.trueValue("TRUE_VALUE").falseValue("FALSE_VALUE").build()

        fft = new FlagFromTagDimension(fftConfig, dictionaries.dimensionDictionary)
        dictionaries.dimensionDictionary.add(fft)
    }

    ApiFilter getFftFilter1() {
        new ApiFilter(
                fft,
                DefaultDimensionField.ID,
                DefaultFilterOperation.in,
                [fft.getTrueValue()]
        )
    }

    ApiFilter getExpectedFftFilter1Transformation() {
        new ApiFilter(
                fft.getFilteringDimension(),
                DefaultDimensionField.ID,
                DefaultFilterOperation.in,
                [fft.getTagValue()]
        )
    }

    ApiFilter getFftFilter2() {
        new ApiFilter(
                fft,
                DefaultDimensionField.ID,
                DefaultFilterOperation.notin,
                [fft.getFalseValue()]
        )
    }

    ApiFilter getExpectedFftFilter2Transformation() {
        new ApiFilter(
                fft.getFilteringDimension(),
                DefaultDimensionField.ID,
                DefaultFilterOperation.eq,
                [fft.getTagValue()]
        )
    }

    def "trying to optimize non-fft dimension fails and throws an error"() {
        setup:
        Dimension dim = Mock()
        ApiFilter filter = Mock() {getDimension() >> dim}

        when:
        fft.optimizeFilters([filter])

        then:
        thrown(IllegalArgumentException)
    }

    def "filter operation is passed through when true value is filtered on and negated when false value is filtered on"() {
        expect:
        [
                DefaultFilterOperation.in,
                DefaultFilterOperation.startswith,
                DefaultFilterOperation.contains,
                DefaultFilterOperation.eq,
                DefaultFilterOperation.notin
        ].each {
            assert fft.transformFilterOperation(fft, it, "TRUE_VALUE") == it
        }

        and:
        [
                DefaultFilterOperation.in,
                DefaultFilterOperation.startswith,
                DefaultFilterOperation.contains,
                DefaultFilterOperation.eq,
        ].each {
            assert fft.transformFilterOperation(fft, it, "FALSE_VALUE") == fft.negativeInvertedFilterOperation
        }

        and:
        fft.transformFilterOperation(fft, DefaultFilterOperation.notin, "FALSE_VALUE") == fft.positiveInvertedFilterOperation
    }


    def "Validating initial api filter fails on incorrectly formed filter"() {
        setup:
        ApiFilter filter = new ApiFilter(fft, fft.getKey(), DefaultFilterOperation.notin, ["TRUE_VALUE", "FALSE_VALUE"])

        when: "both tag values are in the same filter. this creates a nonsensical filter that can't be transformed"
        fft.validateFlagFromTagFilter(filter)

        then:
        thrown(BadApiRequestException)

        when: "try to invert a filter that is not invertible and thus not supported"
        filter = new ApiFilter(fft, fft.getKey(), DefaultFilterOperation.between, ["FALSE_VALUE"])
        fft.validateFlagFromTagFilter(filter)

        then:
        thrown(BadApiRequestException)
    }

    def "Set of single filter is properly transformed"() {
        setup:
        Set<ApiFilter> input = [fftFilter1] as Set

        when:
        Set<ApiFilter> result = fft.transformApiFilterSet(input)

        then:
        result.size() == 1
        !result.contains(fftFilter1)

        and:
        ApiFilter transformedFftFilter = result.stream()
                .findFirst()
                .orElseThrow({return new IllegalStateException("transformed flag for tag filter not found")})

        transformedFftFilter == expectedFftFilter1Transformation
    }

    def "All filters in filter set are properly transformed"() {
        setup:
        Set<ApiFilter> input = [fftFilter1, fftFilter2] as Set

        when:
        Set<ApiFilter> result = fft.transformApiFilterSet(input)
        Set<ApiFilter> expected = [expectedFftFilter1Transformation, expectedFftFilter2Transformation]

        then:
        result.size() == 2
        !result.contains(fftFilter1)
        !result.contains(fftFilter2)

        and:
        result.stream().allMatch({
            it -> expected.remove(it)
        })
    }
}
