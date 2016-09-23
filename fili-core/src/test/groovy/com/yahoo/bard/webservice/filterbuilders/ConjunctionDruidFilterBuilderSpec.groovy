// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.filterbuilders

import static com.yahoo.bard.webservice.druid.model.filter.Filter.DefaultFilterType.AND
import static com.yahoo.bard.webservice.druid.model.filter.Filter.DefaultFilterType.SELECTOR

import com.yahoo.bard.webservice.data.QueryBuildingTestingResources
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionRow
import com.yahoo.bard.webservice.data.dimension.DimensionRowNotFoundException
import com.yahoo.bard.webservice.data.filterbuilders.ConjunctionDruidFilterBuilder
import com.yahoo.bard.webservice.druid.model.filter.Filter
import com.yahoo.bard.webservice.druid.model.filter.SelectorFilter
import com.yahoo.bard.webservice.web.ApiFilter

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class ConjunctionDruidFilterBuilderSpec extends Specification {

    @Shared QueryBuildingTestingResources resources

    ConjunctionDruidFilterBuilder filterBuilder
    Map<String, ApiFilter> apiFilters

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
                ageIdEq1234 : "ageBracket|id-eq[1,2,3,4]",
                ageDescEq1129 : "ageBracket|desc-eq[11-14,14-29]",
                ageIdNotin56   : "ageBracket|id-notin[5,6]",
                ageDescNotin1429: "ageBracket|desc-notin[14-29]"
        ]
        apiFilters = filterSpecs.collectEntries {[(it.key): new ApiFilter(it.value, resources.dimensionDictionary)]}
    }

    def "If there are no filters to build, then the the filter builder returns null"(){
        expect:
        filterBuilder.buildFilters([:]) == null
    }

    @Unroll
    def "buildFilters takes a conjunction of clauses, one for each dimension in #dimensions"() {

        when:
        Filter filter = filterBuilder.buildFilters(dimensions.collectEntries { [(it): [Mock(ApiFilter)] as Set] })

        then:
        filter.type == (dimensions.size() == 1 ? SELECTOR : AND)
        // For this test, a single dimension translates to a single selector filter, which is checked above
        dimensions.size() == 1 || filter.fields.size() == dimensions.size()

        where:
        dimensions << [[resources.d3], [resources.d2, resources.d3], [resources.d1, resources.d2, resources.d3]]
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

    TreeSet<DimensionRow> getDimensionRows(List<String> ids) {
        return ids.collect {resources.d3.findDimensionRowByKeyValue(it)} as TreeSet<DimensionRow>
    }

    List<Filter> getSelectorFilters(List<String> ids) {
        ids.collect { new SelectorFilter(resources.d3, it)}
    }
}
