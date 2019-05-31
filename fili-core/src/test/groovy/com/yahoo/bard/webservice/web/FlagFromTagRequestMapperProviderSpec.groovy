// Copyright 2019 Verizon Media Group.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import com.yahoo.bard.webservice.data.config.ResourceDictionaries
import com.yahoo.bard.webservice.data.config.dimension.DefaultDimensionField
import com.yahoo.bard.webservice.data.config.dimension.FlagFromTagDimensionConfig
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.SearchProvider
import com.yahoo.bard.webservice.data.dimension.impl.FlagFromTagDimension
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest
import com.yahoo.bard.webservice.web.apirequest.DimensionsApiRequest
import com.yahoo.bard.webservice.web.apirequest.TablesApiRequest
import com.yahoo.bard.webservice.web.filters.ApiFilters

import spock.lang.Specification

import javax.ws.rs.container.ContainerRequestContext

class FlagFromTagRequestMapperProviderSpec extends Specification {

    FlagFromTagRequestMapperProvider provider

    FlagFromTagDimension fft
    Dimension fftBaseDim, otherDim
    ResourceDictionaries dictionaries
    ApiFilter fftFilter

//    def setup() {
//        provider = FlagFromTagRequestMapperProvider.Builder.simpleProvider()
//        fftBaseDim = Mock(Dimension)
//        fftBaseDim.getApiName() >> "baseDimension"
//        fftBaseDim.getDescription() >> "unused"
//        fftBaseDim.getLongName() >> "unused"
//        fftBaseDim.getCategory() >> "unused"
//        fftBaseDim.getKey() >> DefaultDimensionField.ID
//        fftBaseDim.getDimensionFields() >> []
//        fftBaseDim.getDefaultDimensionFields() >> []
//        fftBaseDim.getSearchProvider() >> Mock(SearchProvider)
//
//        otherDim = Mock() { getApiName() >> "otherDim" }
//
//        dictionaries = new ResourceDictionaries()
//        dictionaries.dimensionDictionary.add(fftBaseDim)
//        dictionaries.dimensionDictionary.add(otherDim)
//
//        FlagFromTagDimensionConfig fftConfig = FlagFromTagDimensionConfig.build(
//                {"flagFromTag"},
//                "baseDimension",
//                "fftDescription",
//                "fftLongName",
//                "fftCategory",
//                [DefaultDimensionField.ID] as LinkedHashSet,
//                [DefaultDimensionField.ID] as LinkedHashSet,
//                "baseDimension", // filtering
//                "baseDimension", // grouping
//                "TAG_VALUE",
//                "TRUE_VALUE",
//                "FALSE_VALUE",
//                dictionaries.dimensionDictionary
//        )
//
//        fft = new FlagFromTagDimension(fftConfig)
//        dictionaries.dimensionDictionary.add(fft)
//
//        fftFilter = new ApiFilter(
//                fft,
//                DefaultDimensionField.ID,
//                DefaultFilterOperation.in,
//                [fft.getTrueValue()]
//        )
//    }
//
//    boolean transformedFftFilterTest(ApiFilter filter) {
//        return filter.dimension == fft.getFilteringDimension() &&
//                filter.dimensionField == fft.getFilteringDimension().getKey() &&
//                filter.operation == DefaultFilterOperation.in &&
//                filter.values == [fft.getTagValue()] as Set
//    }

//    def "filter operation is passed through when true value is filtered on and negated when false value is filtered on"() {
//        expect:
//        [
//                DefaultFilterOperation.in,
//                DefaultFilterOperation.startswith,
//                DefaultFilterOperation.contains,
//                DefaultFilterOperation.eq,
//                DefaultFilterOperation.notin
//        ].each {
//            assert provider.transformFilterOperation(fft, it, "TRUE_VALUE") == it
//        }
//
//        and:
//        [
//                DefaultFilterOperation.in,
//                DefaultFilterOperation.startswith,
//                DefaultFilterOperation.contains,
//                DefaultFilterOperation.eq,
//        ].each {
//            assert provider.transformFilterOperation(fft, it, "FALSE_VALUE") == provider.negativeInvertedFilterOperation
//        }
//
//        and:
//        provider.transformFilterOperation(fft, DefaultFilterOperation.notin, "FALSE_VALUE") == provider.positiveInvertedFilterOperation
//    }
//
//    def "Validating initial api filter fails on incorrectly formed filter"() {
//        setup:
//        ApiFilter filter = new ApiFilter(fft, fft.getKey(), DefaultFilterOperation.notin, ["TRUE_VALUE", "FALSE_VALUE"])
//
//        when: "both tag values are in the same filter. this creates a nonsensical filter that can't be transformed"
//        provider.validateFlagFromTagFilter(filter)
//
//        then:
//        thrown(BadApiRequestException)
//
//        when: "try to invert a filter that is not invertible and thus not supported"
//        filter = new ApiFilter(fft, fft.getKey(), DefaultFilterOperation.between, ["FALSE_VALUE"])
//        provider.validateFlagFromTagFilter(filter)
//
//        then:
//        thrown(BadApiRequestException)
//    }
//
//    def "Transforming set of filters containing filters on both FFT and non-FFT properly ignores non-FFT dimensions while transforming FFT dimensions"() {
//        setup:
//        ApiFilter nonFftFilter = Mock() {getDimension() >> otherDim}
//
//        Set<ApiFilter> input = [fftFilter, nonFftFilter] as Set
//
//        when:
//        Set<ApiFilter> result = provider.transformApiFilterSet(input)
//
//        then:
//        result.size() == 2
//        result.contains(nonFftFilter)
//        !result.contains(fftFilter)
//
//        and:
//        ApiFilter transformedFftFilter = result.stream()
//                .filter({filter -> filter != nonFftFilter})
//                .findFirst()
//                .orElseThrow({return new IllegalStateException("transformed flag for tag filter not found")})
//        transformedFftFilterTest(transformedFftFilter)
//    }
//
//    def "Transforming ApiFilters containing filters on both FFT and non-FFT dimensions properly ignores non-FFT dimensions while transforming FFT dimensions"() {
//        setup:
//        Set<ApiFilter> otherFilters = [Mock(ApiFilter), Mock(ApiFilter)]
//        ApiFilters testFilters = new ApiFilters([
//                (fft): [fftFilter] as Set,
//                (otherDim): (otherFilters)
//        ] as Map)
//
//        when:
//        ApiFilters resultFilters = provider.transformApiFilters(testFilters)
//
//        then: "non-FFT filters untouched"
//        resultFilters.containsKey(otherDim)
//        resultFilters.get(otherDim) == otherFilters
//
//        and: "fft dimension is not filtered on"
//        ! resultFilters.containsKey(fft)
//
//        and: "instead, fft filtering dimension is filtered on, with the correctly transformed filter"
//        resultFilters.containsKey(fft.getFilteringDimension())
//        resultFilters.get(fft.getFilteringDimension()).size() == 1
//        transformedFftFilterTest(resultFilters.get(fft.getFilteringDimension()).iterator().next())
//    }
//
//    def "Filters on DataApiRequest that use a FFT dimension have those filters properly converted"() {
//        setup:
//        RequestMapper<DataApiRequest> mapper = provider.dataMapper(dictionaries)
//
//        DataApiRequest request = Mock(DataApiRequest)
//        request.getApiFilters() >> new ApiFilters([
//                (fft): [fftFilter] as Set
//        ] as Map )
//
//        when:
//        mapper.apply(request, Mock(ContainerRequestContext))
//
//        then:
//        1 * request.withFilters({
//            it instanceof ApiFilters &&
//                    it.size() == 1 &&
//                    it.containsKey(fft.getFilteringDimension()) &&
//                    it.get(fft.getFilteringDimension()).size() == 1 &&
//                    transformedFftFilterTest(it.get(fft.getFilteringDimension()).iterator().next())
//        })
//    }
//
//    def "Filters on non-fft dimensions remain untouched"() {
//        setup:
//        RequestMapper<DataApiRequest> mapper = provider.dataMapper(dictionaries)
//
//        DataApiRequest request = Mock(DataApiRequest)
//        Set<ApiFilter> otherFilters = [Mock(ApiFilter), Mock(ApiFilter)]
//        request.getApiFilters() >> new ApiFilters([
//                (fft): [fftFilter] as Set,
//                (otherDim): (otherFilters)
//        ] as Map )
//
//        when:
//        mapper.apply(request, Mock(ContainerRequestContext))
//
//        then:
//        1 * request.withFilters({
//            it instanceof ApiFilters &&
//                    it.size() == 2 &&
//                    it.containsKey(otherDim) &&
//                    it.get(otherDim) == otherFilters
//        })
//    }
//
//    def "Dimensions properly filter. Non fft dimensions are not transformed"() {
//        setup:
//        RequestMapper<DimensionsApiRequest> mapper = provider.dimensionsMapper(dictionaries)
//
//        DimensionsApiRequest request = Mock()
//        ApiFilter otherFilter = Mock(ApiFilter) { getDimension() >> Mock(Dimension) }
//        request.getFilters() >> [fftFilter, otherFilter]
//
//        when:
//        mapper.apply(request, Mock(ContainerRequestContext))
//
//        then:
//        1 * request.withFilters({
//            boolean doesntRemoveNonFftFilter = it instanceof Set<ApiFilters> &&
//                    it.size() == 2 &&
//                    it.contains(otherFilter)
//            it.remove(otherFilter)
//            ApiFilter transformedFilter = it.iterator().next()
//            boolean fftFilterIsTransformed = (transformedFilter.dimension == fft.getFilteringDimension()) &&
//                    (transformedFilter.values == [fft.getTagValue()] as Set)
//            doesntRemoveNonFftFilter && fftFilterIsTransformed
//        })
//    }
//
//    def "Tables api requests properly filter. Non fft dimensions are not transformed"() {
//        setup:
//        RequestMapper<TablesApiRequest> mapper = provider.tablesMapper(dictionaries)
//
//        TablesApiRequest request = Mock()
//        Set<ApiFilter> otherFilters = [Mock(ApiFilter), Mock(ApiFilter)]
//        request.getApiFilters() >> new ApiFilters([
//                (fft): [fftFilter] as Set,
//                (otherDim): (otherFilters)
//        ] as Map )
//
//        when:
//        mapper.apply(request, Mock(ContainerRequestContext))
//
//        then:
//        1 * request.withFilters({
//            it instanceof ApiFilters &&
//                    it.size() == 2 &&
//                    it.containsKey(otherDim) &&
//                    it.get(otherDim) == otherFilters &&
//                    it.containsKey(fft.getFilteringDimension()) &&
//                    it.get(fft.getFilteringDimension()).size() == 1 &&
//                    transformedFftFilterTest(it.get(fft.getFilteringDimension()).iterator().next())
//        })
//    }
}
