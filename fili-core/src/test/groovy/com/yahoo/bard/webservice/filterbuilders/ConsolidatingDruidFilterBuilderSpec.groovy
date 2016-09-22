// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.filterbuilders

import static com.yahoo.bard.webservice.druid.model.filter.Filter.DefaultFilterType.NOT
import static com.yahoo.bard.webservice.druid.model.filter.Filter.DefaultFilterType.AND
import static com.yahoo.bard.webservice.druid.model.filter.Filter.DefaultFilterType.OR
import static com.yahoo.bard.webservice.druid.model.filter.Filter.DefaultFilterType.SELECTOR

import com.yahoo.bard.webservice.data.QueryBuildingTestingResources
import com.yahoo.bard.webservice.data.filterbuilders.DruidFilterBuilder
import com.yahoo.bard.webservice.data.filterbuilders.ConsolidatingDruidFilterBuilder
import com.yahoo.bard.webservice.druid.model.filter.AndFilter
import com.yahoo.bard.webservice.druid.model.filter.Filter
import com.yahoo.bard.webservice.druid.model.filter.NotFilter
import com.yahoo.bard.webservice.druid.model.filter.OrFilter
import com.yahoo.bard.webservice.druid.model.filter.SelectorFilter
import com.yahoo.bard.webservice.web.ApiFilter

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class ConsolidatingDruidFilterBuilderSpec extends Specification {

    @Shared QueryBuildingTestingResources resources

    Map<String, ApiFilter> apiFilters
    DruidFilterBuilder filterBuilder

    def setupSpec() {
        resources = new QueryBuildingTestingResources()
    }

    def setup() {
        filterBuilder = new ConsolidatingDruidFilterBuilder()

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
    def "#filterString is a #filterType-filter on #selectorDimension, with #orFilterSize or-clauses of selectors"() {
        setup:
        ApiFilter filter = new ApiFilter(filterString, resources.dimensionDictionary)

        when: "We build a single filter term"
        // resources.d3 is the ageBracket dimension.
        Filter outerFilter = filterBuilder.buildFilters([(resources.d3): [filter] as Set])

        and: "Remove the NOT if there is one"
        Filter positiveFilter = outerFilter.type == NOT ? outerFilter.field : outerFilter

        and: "Extract the clauses from the constructed filter, if the filter has more than one clause"
        List<Filter> clauses = positiveFilter.getType() == SELECTOR ? [positiveFilter] : positiveFilter.fields

        then: "The extracted Filter has been correctly hydrated"
        outerFilter.type == filterType
        clauses.size() == orFilterSize
        clauses.every { it.getType() == SELECTOR}
        SelectorFilter selectorFilter = clauses.get(selectIndex)
        selectorFilter.getDimension() == resources.dimensionDictionary.findByApiName(selectorDimension)
        selectorFilter.getValue() == value

        where:
        filterString                      | filterType | orFilterSize   | selectIndex | selectorDimension | value
        "ageBracket|id-eq[1]"             | SELECTOR   | 1              | 0           | "ageBracket"      | "1"
        "ageBracket|id-in[1]"             | SELECTOR   | 1              | 0           | "ageBracket"      | "1"
        "ageBracket|id-startswith[1]"     | SELECTOR   | 1              | 0           | "ageBracket"      | "1"
        "ageBracket|id-contains[1]"       | SELECTOR   | 1              | 0           | "ageBracket"      | "1"
        "ageBracket|id-notin[1]"          | NOT        | 1              | 0           | "ageBracket"      | "1"
        "ageBracket|id-eq[1,2,4]"         | OR         | 3              | 2           | "ageBracket"      | "4"
        "ageBracket|desc-eq[11-14,14-29]" | OR         | 2              | 1           | "ageBracket"      | "3"
    }

    @Unroll
    def "Filtering for #apiList on #apiName gives us a Druid filter of type #filterType on #druidList"() {
        setup:
        Set<ApiFilter> apiSet = apiList.collect { apiFilters.get(it) } as LinkedHashSet
        Filter druidExpected = druidFilterMaker()

        when: "We construct the Druid filters"
        Filter outerFilter = filterBuilder.buildFilters([(resources.d3): apiSet])

        then: "The constructed filter combines the api filters appropriately"
        outerFilter.type == filterType
        outerFilter == druidExpected

        where:
        apiName = resources.d3.apiName
        apiList                               | filterType | druidList                             | druidFilterMaker
        ["ageIdEq1234"]                       | OR         | ["ageIdEq1234"]                       | {makeIdEq()}
        ["ageIdEq1234", "ageDescEq1129"]      | OR         | ["ageIdEq1234", "ageDescEq1129"]      | {makeIdDescEq()}
        ["ageIdNotin56"]                      | NOT        | ["ageIdNotEq56"]                      | {makeIdNotin()}
        ["ageDescEq1129", "ageDescNotin1429"] | SELECTOR   | ["ageDescEq1129", "ageDescNotEq1429"] | {makeDescEqNoteq()}
        ["ageIdNotin56", "ageDescNotin1429"]  | AND        | ["ageIdNotin56", "ageDescNotin1429"]  | {makeIdDescNotin()}
    }

    @Unroll
    def "Filtering for #apiList on #dimensions gives a #filterType-filter with subfilter type #subFilterType"() {

        setup:
        Set apiSet = (apiList.collect { apiFilters.get(it) }) as Set

        Map dimFilterMap = dimensions.collectEntries { [(it): apiSet] }

        Filter outerFilter = filterBuilder.buildFilters(dimFilterMap)
        Filter innerFilter = outerFilter.type == NOT ? outerFilter.field : outerFilter.fields[0]

        expect:
        outerFilter.type == filterType
        innerFilter.type == subFilterType

        where:
        dimensions                   |  apiList                          | filterType | subFilterType
        [resources.d3]               |  ["ageIdEq1234"]                  | OR         | SELECTOR
        [resources.d3]               |  ["ageIdEq1234", "ageDescEq1129"] | OR         | SELECTOR
        [resources.d2, resources.d3] |  ["ageIdEq1234"]                  | AND        | OR
        [resources.d2, resources.d3] |  ["ageIdEq1234", "ageDescEq1129"] | AND        | OR
        [resources.d3]               |  ["ageIdNotin56"]                 | NOT        | OR
        [resources.d2, resources.d3] |  ["ageIdNotin56"]                 | AND        | NOT
    }


    // The following methods manually translate specific Api filter queries into Druid filters. The selector filters
    // are derived based on the values of the 'ages' map in QueryBuildingTestingResources
    Filter makeIdEq() {
        // ageBracket|id-eq[1,2,3,4]
        new OrFilter([
                new SelectorFilter(resources.d3, "1"),
                new SelectorFilter(resources.d3, "2"),
                new SelectorFilter(resources.d3, "3"),
                new SelectorFilter(resources.d3, "4")
        ])
    }

    Filter makeIdDescEq() {
        // ageBracket|id-eq[1,2,3,4],ageBracket|desc-eq[11-14,14-29]
        new OrFilter([
                new SelectorFilter(resources.d3, "2"),
                new SelectorFilter(resources.d3, "3"),
        ])
    }

    Filter makeIdNotin() {
        // ageBracket|id-notin[5,6]
        new NotFilter(
                new OrFilter([
                        new SelectorFilter(resources.d3, "5"),
                        new SelectorFilter(resources.d3, "6")
                ])
        )
    }

    Filter makeDescEqNoteq() {
        // ageBracket|desc-eq[11-14,14-29],ageBracket|desc-notin[14-29]
        new SelectorFilter(resources.d3, "2")
    }

    Filter makeIdDescNotin() {
        // ageBracket|id-notin[5,6],ageBracket|desc-notin[14-29]
        new AndFilter([
                new NotFilter(
                        new OrFilter([
                                new SelectorFilter(resources.d3, "5"),
                                new SelectorFilter(resources.d3, "6")
                        ])
                ),
                new NotFilter(
                        new SelectorFilter(resources.d3, "3")
                )
        ])
    }
}
