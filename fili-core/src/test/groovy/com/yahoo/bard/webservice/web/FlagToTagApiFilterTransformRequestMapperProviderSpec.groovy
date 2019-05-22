package com.yahoo.bard.webservice.web

import com.yahoo.bard.webservice.data.config.ResourceDictionaries
import com.yahoo.bard.webservice.data.config.dimension.DefaultDimensionField
import com.yahoo.bard.webservice.data.config.dimension.FlagFromTagDimensionConfig
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.SearchProvider
import com.yahoo.bard.webservice.data.dimension.impl.FlagFromTagDimension
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest
import com.yahoo.bard.webservice.web.filters.ApiFilters

import spock.lang.Specification

import javax.ws.rs.container.ContainerRequestContext

class FlagToTagApiFilterTransformRequestMapperProviderSpec extends Specification {

    FlagFromTagDimension fft
    Dimension testDim, groupDim, filterDim
    ResourceDictionaries dictionaries

    def setup() {
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
            assert FlagToTagApiFilterTransformRequestMapperProvider.transformFilterOperation(fft, it, ["TRUE_VALUE"]) == it
        }

        and:
        [
                DefaultFilterOperation.in,
                DefaultFilterOperation.startswith,
                DefaultFilterOperation.contains,
                DefaultFilterOperation.eq,
        ].each {
            assert FlagToTagApiFilterTransformRequestMapperProvider.transformFilterOperation(fft, it, ["FALSE_VALUE"]) == DefaultFilterOperation.notin
        }

        and:
        FlagToTagApiFilterTransformRequestMapperProvider.transformFilterOperation(fft, DefaultFilterOperation.notin, ["FALSE_VALUE"]) == DefaultFilterOperation.in
    }

    def "filter transformation fails gracefully when processing a filter that is malformed or not configured to handle"() {
        when: "both tag values are in the same filter. this creates a nonsensical filter that can't be transformed"
        FlagToTagApiFilterTransformRequestMapperProvider.transformFilterOperation(fft, DefaultFilterOperation.notin, ["TRUE_VALUE", "FALSE_VALUE"])

        then:
        thrown(BadApiRequestException)

        when: "try to invert a filter that is not invertable and thus not supported"
        FlagToTagApiFilterTransformRequestMapperProvider.transformFilterOperation(fft, DefaultFilterOperation.between, ["FALSE_VALUE"])

        then:
        thrown(BadApiRequestException)
    }

    def "Filters on DataApiRequest that use a FFT dimension have those filters properly converted"() {
        setup:
        RequestMapper<DataApiRequest> mapper = FlagToTagApiFilterTransformRequestMapperProvider.getDataRequestMapper(dictionaries)

        DataApiRequest dari = Mock(DataApiRequest)
        dari.getApiFilters() >> new ApiFilters([
                (fft): [new ApiFilter(
                        fft,
                        DefaultDimensionField.ID,
                        DefaultFilterOperation.in,
                        [fft.getTrueValue()]
                )] as Set
        ] as Map )

        when:
        mapper.apply(dari, Mock(ContainerRequestContext))

        then:
        1 * dari.withFilters({
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
        RequestMapper<DataApiRequest> mapper = FlagToTagApiFilterTransformRequestMapperProvider.getDataRequestMapper(dictionaries)

        DataApiRequest dari = Mock(DataApiRequest)
        Dimension otherDim = Mock(Dimension)
        Set<ApiFilter> otherFilters = [Mock(ApiFilter), Mock(ApiFilter)]
        dari.getApiFilters() >> new ApiFilters([
                (fft): [new ApiFilter(
                        fft,
                        DefaultDimensionField.ID,
                        DefaultFilterOperation.in,
                        [fft.getTrueValue()])] as Set,
                (otherDim): (otherFilters)
        ] as Map )

        when:
        mapper.apply(dari, Mock(ContainerRequestContext))

        then:
        1 * dari.withFilters({
            it instanceof ApiFilters &&
                    it.size() == 2 &&
                    it.containsKey(otherDim) &&
                    it.get(otherDim) == otherFilters
        })
    }
}
