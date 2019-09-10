package com.yahoo.bard.webservice.web.apirequest.rfc

import com.yahoo.bard.webservice.config.BardFeatureFlag
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.data.time.Granularity
import com.yahoo.bard.webservice.web.filters.ApiFilters
import com.yahoo.bard.webservice.web.util.BardConfigResources

import spock.lang.Shared
import spock.lang.Specification

import javax.ws.rs.core.PathSegment

class DataApiRequestBuilderSpec extends Specification {

    @Shared RequestParameters simpleRequestParameters = new RequestParameters(
            "logicalTable",
            "day", // granularity
            [] as List<PathSegment>, // dimensions
            "logicalMetrics",
            "intervals",
            "apiFilters",
            "havings",
            "sorts",
            "count",
            "topN",
            "format",
            "downloadFilename",
            "timeZone",
            "asyncAfter",
            "perPage",
            "page"
    )

    @Shared GranularityGenerator granularityGenerator = new GranularityGenerator()

    DataApiRequestBuilder builder

    def setup() {
        builder = new DataApiRequestBuilder(Mock(BardConfigResources))
    }

    static class GranularityGenerator implements Generator<Granularity> {

        @Override
        Granularity bind(DataApiRequestBuilder builder, RequestParameters params, BardConfigResources resource) {
            return DefaultTimeGrain.DAY
        }

        @Override
        void validate(Granularity entity, DataApiRequestBuilder builder, RequestParameters params, BardConfigResources resources) {
            // do nothing
        }
    }

    def "Calling any of the setters will set the generated value in the builder, and mark that stage as done"() {
        expect: "built map initializes granularity to not yet built"
        !builder.built.get(DataApiRequestBuilder.BuildPhase.GRANULARITY)

        when: "builder builds granularity"
        builder.granularity(simpleRequestParameters, granularityGenerator)

        then: "granularity is properly set from generator"
        builder.granularity == DefaultTimeGrain.DAY

        and: "granularity is marked as having been built"
        builder.built.get(DataApiRequestBuilder.BuildPhase.GRANULARITY)
    }

    def "Building DataApiRequest throws an error unless all build phases have been completed"() {
        setup:
        Boolean requireAllBuiltFlag = BardFeatureFlag.POJO_DARI_REQUIRE_ALL_STAGES_CALLED.isOn()
        BardFeatureFlag.POJO_DARI_REQUIRE_ALL_STAGES_CALLED.setOn(true)
        builder.@dimensions = [] as LinkedHashSet
        builder.@perDimensionFields = [:] as LinkedHashMap
        builder.@metrics = [] as LinkedHashSet
        builder.@intervals = []
        builder.@apiFilters = new ApiFilters()
        builder.@havings = [:] as LinkedHashMap
        builder.@sorts = [] as LinkedHashSet

        when: "attempt to build without all data pieces being built"
        builder.build()

        then:
        thrown(IllegalStateException)

        when:
        DataApiRequestBuilder.BuildPhase.values().each {
            builder.built.put(it, Boolean.TRUE)
        }
        builder.build()

        then:
        noExceptionThrown()

        cleanup:
        BardFeatureFlag.POJO_DARI_REQUIRE_ALL_STAGES_CALLED.setOn(requireAllBuiltFlag)
    }
}