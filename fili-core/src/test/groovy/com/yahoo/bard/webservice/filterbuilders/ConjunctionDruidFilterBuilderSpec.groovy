// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.filterbuilders

import static com.yahoo.bard.webservice.druid.model.filter.Filter.DefaultFilterType.AND
import static com.yahoo.bard.webservice.druid.model.filter.Filter.DefaultFilterType.SELECTOR

import com.yahoo.bard.webservice.data.QueryBuildingTestingResources
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionRow
import com.yahoo.bard.webservice.data.dimension.DimensionRowNotFoundException
import com.yahoo.bard.webservice.druid.model.builders.ConjunctionDruidFilterBuilder
import com.yahoo.bard.webservice.druid.model.filter.Filter
import com.yahoo.bard.webservice.druid.model.filter.SearchFilter
import com.yahoo.bard.webservice.druid.model.filter.SelectorFilter
import com.yahoo.bard.webservice.web.ApiFilter
import com.yahoo.bard.webservice.web.apirequest.generator.filter.FilterBinders

import org.apache.commons.lang3.tuple.Pair

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class ConjunctionDruidFilterBuilderSpec extends Specification {

    @Shared QueryBuildingTestingResources resources

    ConjunctionDruidFilterBuilder filterBuilder
    Map<String, ApiFilter> apiFilters

    FilterBinders filterBinders = FilterBinders.instance

    def setupSpec() {
        resources = new QueryBuildingTestingResources()
    }

    def setup() {
        filterBuilder = new ConjunctionDruidFilterBuilder() {
            @Override
            protected Filter buildDimensionFilter(
                    Dimension dimension,
                    Set<ApiFilter> filters
            ) throws DimensionRowNotFoundException {
                return new SelectorFilter(resources.d3, "1")
            }
        }

        Map<String, String> filterSpecs = [
                ageIdEq1234:      "ageBracket|id-eq[1,2,3,4]",
                ageDescEq1129:    "ageBracket|desc-eq[11-14,14-29]",
                ageIdNotin56:     "ageBracket|id-notin[5,6]",
                ageIdIn56:        "ageBracket|id-in[5,6]", // used to represent the negation of "ageIdNotin56"
                ageDescNotin1429: "ageBracket|desc-notin[14-29]",
                ageDescIn1429:    "ageBracket|desc-in[14-29]" // used to represent the negation of "ageDescNotin1429"

        ]
        apiFilters = filterSpecs.collectEntries {
            [(it.key): filterBinders.generateApiFilter(it.value, resources.dimensionDictionary)]
        }
    }

    def "If there are no filters to build, then the the filter builder returns null"(){
        expect:
        filterBuilder.buildFilters([:]) == null
    }

    @Unroll
    def "buildFilters takes a conjunction of clauses, one for each dimension in #dimensions"() {
        when:
        Filter filter = filterBuilder.buildFilters(dimensions.collectEntries {
            it ->
                Dimension dim = (Dimension) it
                [
                        (dim): [Mock(ApiFilter) {getDimension() >> dim}] as Set
                ]
        })

        then:
        filter.type == (dimensions.size() == 1 ? SELECTOR : AND)
        // For this test, a single dimension translates to a single selector filter, which is checked above
        dimensions.size() == 1 || filter.fields.size() == dimensions.size()

        where:
        dimensions << [[resources.d3], [resources.d2, resources.d3], [resources.d1, resources.d2, resources.d3]]
    }

    @Unroll
    def "#filters get split into positive (#left) and negative (#right) filters in the case of #description"() {
        when: "filters are splitted"
        Pair<Set<ApiFilter>, Set<ApiFilter>> splitFilters = filterBuilder.splitApiFilters(
                filters.collect { apiFilters[it] } as Set
        )

        then: "positive filters come out in left of pair; negative filters, which are negated as well, come out in right of pair"
        splitFilters.getLeft() == left.collect { apiFilters[it] } as Set
        splitFilters.getRight() == right.collect { apiFilters[it] } as Set

        where:
        filters                              | description                               || left                            | right
        ["ageIdEq1234"]                      | "single positive filter"                  || ["ageIdEq1234"]                 | []
        ["ageIdNotin56"]                     | "single negative filter"                  || []                              | ["ageIdNotin56"]
        ["ageIdEq1234","ageIdNotin56"]       | "combination of single pos & neg filters" || ["ageIdEq1234"]                 | ["ageIdNotin56"]
        ["ageDescNotin1429","ageDescEq1129"] | "combination of single pos & neg filters" || ["ageDescEq1129"]               | ["ageDescNotin1429"]
        ["ageIdEq1234","ageDescEq1129"]      | "all positive filters"                    || ["ageIdEq1234","ageDescEq1129"] | []
        ["ageDescNotin1429","ageIdNotin56"]  | "all negative filters"                    || []                              | ["ageDescNotin1429","ageIdNotin56"]
    }

    @Unroll
    def "Negative filter #negativeFilter is negated to #negated"() {
        expect:
        filterBuilder.negateNegativeFilters([ apiFilters[negativeFilter] ]) == [ apiFilters[negated] ] as Set

        where:
        negativeFilter     || negated
        "ageIdNotin56"     || "ageIdIn56"
        "ageDescNotin1429" || "ageDescIn1429"
    }

    def "Negating positive filter throws error"() {
        when:
        filterBuilder.negateNegativeFilters([ apiFilters["ageIdIn56"] ])

        then:
        Exception exception = thrown()
        exception instanceof IllegalArgumentException
        exception.message.concat(ConjunctionDruidFilterBuilder.NON_NEGATIVE_FILTER_ERROR_FORMAT)
    }

    @Unroll
    def "getFilteredDimensionRows resolves #filters on dimension 'ageBracket' into #dimensionrows"() {
        expect:
        Set<ApiFilter> filtersUnderTest = filters.collect { apiFilters[it] }
        filterBuilder.getFilteredDimensionRows(resources.d3, filtersUnderTest) == dimensionrows

        where:
        filters                              || dimensionrows
        ["ageIdEq1234"]                      || getDimensionRows(["1", "2", "3", "4"])
        ["ageIdNotin56"]                     || getDimensionRows(["1", "2", "3", "4"])
        ["ageIdEq1234","ageDescEq1129"]      || getDimensionRows(["2", "3"])
        ["ageIdEq1234","ageIdNotin56"]       || getDimensionRows(["1", "2", "3", "4"])
        ["ageDescNotin1429","ageDescEq1129"] || getDimensionRows(["2"])
        ["ageDescNotin1429","ageIdNotin56"]  || getDimensionRows(["1", "2", "4"])
    }

    @Unroll
    def "getFilteredDimensionRowValues resolves #filters on dimension 'ageBracket' into #dimensionrowValues"() {
        expect:
        Set<ApiFilter> filtersUnderTest = filters.collect { apiFilters[it] }
        filterBuilder.getFilteredDimensionRowValues(resources.d3, filtersUnderTest) == dimensionrowValues

        where:
        filters                              || dimensionrowValues
        ["ageIdEq1234"]                      || ["1", "2", "3", "4"]
        ["ageIdNotin56"]                     || ["1", "2", "3", "4"]
        ["ageIdEq1234","ageDescEq1129"]      || ["2", "3"]
        ["ageIdEq1234","ageIdNotin56"]       || ["1", "2", "3", "4"]
        ["ageDescNotin1429","ageDescEq1129"] || ["2"]
        ["ageDescNotin1429","ageIdNotin56"]  || ["1", "2", "4"]
    }

    @Unroll
    def "buildSelectorFilters constructs one selector filter for each id #ids"() {
        expect:
        filterBuilder.buildSelectorFilters(resources.d3, getDimensionRows(ids)) == getSelectorFilters(ids)

        where:
        ids                  | _
        []                   | _
        ["1"]                | _
        ["2", "3"]           | _
        ["1", "2", "3", "4"] | _
        ["1", "3", "4"]      | _


    }

    @Unroll
    def "buildContainsSearchFilters constructs a list of SearchFilters for #values"() {
        expect:
        filterBuilder.buildContainsSearchFilters(resources.d16, values) == getSearchFilters(values)

        where:
        values              | _
        []                  | _
        ['v1', 'v2', 'v3']  | _
    }

    List<Filter> getSearchFilters(List<String> values) {
        return values.collect {new SearchFilter(resources.d16, SearchFilter.QueryType.Contains, it)}
    }

    TreeSet<DimensionRow> getDimensionRows(List<String> ids) {
        return ids.collect {resources.d3.findDimensionRowByKeyValue(it)} as TreeSet<DimensionRow>
    }

    List<Filter> getSelectorFilters(List<String> ids) {
        ids.collect { new SelectorFilter(resources.d3, it)}
    }
}
