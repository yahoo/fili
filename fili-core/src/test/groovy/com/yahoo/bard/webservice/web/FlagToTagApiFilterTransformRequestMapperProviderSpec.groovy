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

class FlagToTagApiFilterTransformRequestMapperProviderSpec extends Specification {

    FlagToTagApiFilterTransformRequestMapperProvider provider

    FlagFromTagDimension fft
    Dimension testDim
    ResourceDictionaries dictionaries
    ApiFilter fftFilter

    def setup() {
        provider = new FlagToTagApiFilterTransformRequestMapperProvider()

        FlagFromTagDimensionConfig fftConfig = new FlagFromTagDimensionConfig(
                {"flagFromTag"},
                "fftDescription",
                "fftLongName",
                "fftCategory",
                "unused", // filtering
                "unused", // grouping
                "TAG_VALUE",
                "TRUE_VALUE",
                "FALSE_VALUE",
        )
        testDim = Mock(Dimension)
        testDim.getApiName() >> "unused"
        testDim.getDescription() >> "unused"
        testDim.getLongName() >> "unused"
        testDim.getCategory() >> "unused"
        testDim.getDimensionFields() >> []
        testDim.getDefaultDimensionFields() >> []
        testDim.getSearchProvider() >> Mock(SearchProvider)

        dictionaries = new ResourceDictionaries()
        dictionaries.dimensionDictionary.add(testDim)

        fft = new FlagFromTagDimension(
                fftConfig,
                dictionaries.dimensionDictionary
        )
        dictionaries.dimensionDictionary.add(fft)

        fftFilter = new ApiFilter(
                fft,
                DefaultDimensionField.ID,
                DefaultFilterOperation.in,
                [fft.getTrueValue()]
        )
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
            assert provider.transformFilterOperation(fft, it, ["TRUE_VALUE"]) == it
        }

        and:
        [
                DefaultFilterOperation.in,
                DefaultFilterOperation.startswith,
                DefaultFilterOperation.contains,
                DefaultFilterOperation.eq,
        ].each {
            assert provider.transformFilterOperation(fft, it, ["FALSE_VALUE"]) == DefaultFilterOperation.notin
        }

        and:
        provider.transformFilterOperation(fft, DefaultFilterOperation.notin, ["FALSE_VALUE"]) == DefaultFilterOperation.in
    }

    def "filter transformation fails gracefully when processing a filter that is malformed or not configured to handle"() {
        when: "both tag values are in the same filter. this creates a nonsensical filter that can't be transformed"
        provider.transformFilterOperation(fft, DefaultFilterOperation.notin, ["TRUE_VALUE", "FALSE_VALUE"])

        then:
        thrown(BadApiRequestException)

        when: "try to invert a filter that is not invertable and thus not supported"
        provider.transformFilterOperation(fft, DefaultFilterOperation.between, ["FALSE_VALUE"])

        then:
        thrown(BadApiRequestException)
    }

    def "Filters on DataApiRequest that use a FFT dimension have those filters properly converted"() {
        setup:
        RequestMapper<DataApiRequest> mapper = provider.dataApiRequestMapper(dictionaries)

        DataApiRequest request = Mock(DataApiRequest)
        request.getApiFilters() >> new ApiFilters([
                (fft): [fftFilter] as Set
        ] as Map )

        when:
        mapper.apply(request, Mock(ContainerRequestContext))

        then:
        1 * request.withFilters({
            it instanceof ApiFilters &&
                    it.size() == 1 &&
                    it.containsKey(fft.getFilteringDimension()) &&
                    it.get(fft.getFilteringDimension()).size() == 1 &&
                    it.get(fft.getFilteringDimension()).iterator().next().dimension == fft.getFilteringDimension()
                    it.get(fft.getFilteringDimension()).iterator().next().dimensionField == DefaultDimensionField.ID &&
                    it.get(fft.getFilteringDimension()).iterator().next().operation == DefaultFilterOperation.in &&
                    it.get(fft.getFilteringDimension()).iterator().next().values == [fft.getTagValue()] as Set
        })
    }

    def "Filters on non-fft dimensions remain untouched"() {
        setup:
        RequestMapper<DataApiRequest> mapper = provider.dataApiRequestMapper(dictionaries)

        DataApiRequest request = Mock(DataApiRequest)
        Dimension otherDim = Mock(Dimension)
        Set<ApiFilter> otherFilters = [Mock(ApiFilter), Mock(ApiFilter)]
        request.getApiFilters() >> new ApiFilters([
                (fft): [fftFilter] as Set,
                (otherDim): (otherFilters)
        ] as Map )

        when:
        mapper.apply(request, Mock(ContainerRequestContext))

        then:
        1 * request.withFilters({
            it instanceof ApiFilters &&
                    it.size() == 2 &&
                    it.containsKey(otherDim) &&
                    it.get(otherDim) == otherFilters
        })
    }

    def "Dimensions properly filter. Non fft dimensions are not transformed"() {
        setup:
        RequestMapper<DimensionsApiRequest> mapper = provider.dimensionsApiRequestMapper(dictionaries)

        DimensionsApiRequest request = Mock()
        ApiFilter otherFilter = Mock(ApiFilter) { getDimension() >> Mock(Dimension) }
        request.getFilters() >> [fftFilter, otherFilter]

        when:
        mapper.apply(request, Mock(ContainerRequestContext))

        then:
        1 * request.withFilters({
            boolean doesntRemoveNonFftFilter = it instanceof Set<ApiFilters> &&
                    it.size() == 2 &&
                    it.contains(otherFilter)
            it.remove(otherFilter)
            ApiFilter transformedFilter = it.iterator().next()
            boolean fftFilterIsTransformed = (transformedFilter.dimension == fft.getFilteringDimension()) &&
                    (transformedFilter.values == [fft.getTagValue()] as Set)
            doesntRemoveNonFftFilter && fftFilterIsTransformed
        })
    }

    def "Tables api requests properly filter. Non fft dimensions are not transformed"() {
        setup:
        RequestMapper<TablesApiRequest> mapper = provider.tablesApiRequestMapper(dictionaries)

        TablesApiRequest request = Mock()
        Dimension otherDim = Mock(Dimension)
        Set<ApiFilter> otherFilters = [Mock(ApiFilter), Mock(ApiFilter)]
        request.getApiFilters() >> new ApiFilters([
                (fft): [fftFilter] as Set,
                (otherDim): (otherFilters)
        ] as Map )

        when:
        mapper.apply(request, Mock(ContainerRequestContext))

        then:
        1 * request.withFilters({
            it instanceof ApiFilters &&
                    it.size() == 2 &&
                    it.containsKey(otherDim) &&
                    it.get(otherDim) == otherFilters &&
                    it.containsKey(fft.getFilteringDimension()) &&
                    it.get(fft.getFilteringDimension()).size() == 1 &&
                    it.get(fft.getFilteringDimension()).iterator().next().dimension == fft.getFilteringDimension()
                    it.get(fft.getFilteringDimension()).iterator().next().values == [fft.getTagValue()] as Set
        })
    }
}
