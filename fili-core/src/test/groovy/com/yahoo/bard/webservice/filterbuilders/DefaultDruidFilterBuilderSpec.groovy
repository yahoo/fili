// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.filterbuilders

import static com.yahoo.bard.webservice.druid.model.filter.Filter.DefaultFilterType.AND
import static com.yahoo.bard.webservice.druid.model.filter.Filter.DefaultFilterType.NOT
import static com.yahoo.bard.webservice.druid.model.filter.Filter.DefaultFilterType.OR
import static com.yahoo.bard.webservice.druid.model.filter.Filter.DefaultFilterType.SELECTOR

import com.yahoo.bard.webservice.data.QueryBuildingTestingResources
import com.yahoo.bard.webservice.data.filterbuilders.DefaultDruidFilterBuilder
import com.yahoo.bard.webservice.data.filterbuilders.DruidFilterBuilder
import com.yahoo.bard.webservice.druid.model.filter.Filter
import com.yahoo.bard.webservice.druid.model.filter.NotFilter
import com.yahoo.bard.webservice.druid.model.filter.OrFilter
import com.yahoo.bard.webservice.druid.model.filter.SelectorFilter
import com.yahoo.bard.webservice.web.ApiFilter

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

public class DefaultDruidFilterBuilderSpec extends Specification {

    @Shared QueryBuildingTestingResources resources

    Map filterSpecs
    Map apiFilters
    Map druidFilters
    DruidFilterBuilder filterBuilder

    def setupSpec() {
        resources = new QueryBuildingTestingResources()
    }

    def setup() {
        filterSpecs = [ageIdEq1234 : "ageBracket|id-eq[1,2,3,4]",
                       ageDescEq1129 : "ageBracket|desc-eq[11-14,14-29]",
                       ageIdNotin56   : "ageBracket|id-notin[5,6]",
                       ageDescNotin1429: "ageBracket|desc-notin[14-29]"]

        apiFilters = [:]
        filterSpecs.each {
            apiFilters.put(it.key, new ApiFilter(it.value, resources.dimensionDictionary))
        }

        Filter ageIdEq1234 = new OrFilter([
                            new SelectorFilter(resources.d3, "1"),
                            new SelectorFilter(resources.d3, "2"),
                            new SelectorFilter(resources.d3, "3"),
                            new SelectorFilter(resources.d3, "4")
        ])
        Filter ageDescEq1129 = new OrFilter([
                new SelectorFilter(resources.d3, "2"),
                new SelectorFilter(resources.d3, "3")
        ])
        Filter ageIdNotin56 = new NotFilter(new OrFilter([
                new SelectorFilter(resources.d3, "5"),
                new SelectorFilter(resources.d3, "6")
        ]))
        Filter ageDescNotin1429 = new NotFilter(new OrFilter([
               new SelectorFilter(resources.d3, "3")
        ]))
        druidFilters =  [
                ageIdEq1234: ageIdEq1234,
                ageDescEq1129: ageDescEq1129,
                ageIdNotin56: ageIdNotin56,
                ageDescNotin1429: ageDescNotin1429
        ]
        filterBuilder = new DefaultDruidFilterBuilder()
    }

    def "If there are no filters to build, then the the filter builder returns null"(){
        expect:
        filterBuilder.buildFilters([:]) == null
    }

    @Unroll
    def "#filterString is a #outerFilterType-filter on #selectorDimension, with #orFilterSize or-clauses"() {
        setup:
        ApiFilter filter = new ApiFilter(filterString, resources.dimensionDictionary)

        when: "We build a single selector filter"
        //resources.d3 is the ageBracket dimension.
        Filter outerFilter = filterBuilder.buildFilters([(resources.d3): [filter] as Set])

        and: "Extract the constructed or-filter"
        OrFilter orFilter = outerFilter.type == NOT ? outerFilter.getField() : outerFilter


        then: "The extracted OrFilter has been correctly hydrated"
        outerFilter.type == outerFilterType
        orFilter.fields.size() == orFilterSize
        SelectorFilter sF = orFilter.fields.get(selectIndex)
        sF.getDimension() == resources.dimensionDictionary.findByApiName(selectorDimension)
        sF.getValue() == value

        where:
        filterString                      | outerFilterType | orFilterSize   | selectIndex | selectorDimension | value
        "ageBracket|id-eq[1]"             | OR              | 1              | 0           | "ageBracket"      | "1"
        "ageBracket|id-in[1]"             | OR              | 1              | 0           | "ageBracket"      | "1"
        "ageBracket|id-startswith[1]"     | OR              | 1              | 0           | "ageBracket"      | "1"
        "ageBracket|id-contains[1]"       | OR              | 1              | 0           | "ageBracket"      | "1"
        "ageBracket|id-notin[1]"          | NOT             | 1              | 0           | "ageBracket"      | "1"
        "ageBracket|id-eq[1,2,4]"         | OR              | 3              | 2           | "ageBracket"      | "4"
        "ageBracket|desc-eq[11-14,14-29]" | OR              | 2              | 1           | "ageBracket"      | "3"
    }

    @Unroll
    def "Filtering for #apiList on #apiName gives us a Druid filter of type #filterType on #druidList"() {

        setup:

        Set apiSet = apiList.collect { apiFilters.get(it) } as Set

        List druidExpected = druidList.collect() { druidFilters.get(it) }

        when:
        Filter outerFilter = filterBuilder.buildFilters([(resources.d3): apiSet])
        List<Filter> filters = outerFilter.type == AND ? outerFilter.fields : [outerFilter]

        then:
        outerFilter.type == filterType
        if (outerFilter.type == OR ) {
            assert druidExpected == [outerFilter]
        } else {
            assert filters.containsAll(druidExpected) && druidExpected.containsAll(filters)
        }

        where:
        apiName = resources.d3.apiName
        apiList                               | filterType | druidList
        ["ageIdEq1234"]                       | OR         | ["ageIdEq1234"]
        ["ageIdEq1234", "ageDescEq1129"]      | AND        | ["ageIdEq1234", "ageDescEq1129"]
        ["ageIdNotin56"]                      | NOT        | ["ageIdNotin56"]
        ["ageDescEq1129", "ageDescNotin1429"] | AND        | ["ageDescEq1129", "ageDescNotin1429"]}


    @Unroll
    def "Filtering for #apiList on #dimensions gives a #filterType-filter with subfilter type #subfilterType"() {

        setup:
        Set apiSet = (apiList.collect { apiFilters.get(it) }) as Set

        Map dimFilterMap = dimensions.collectEntries { [(it): apiSet] }

        Filter outerFilter = filterBuilder.buildFilters(dimFilterMap)
        Filter innerFilter
        if (outerFilter.type == OR ||  outerFilter.type == AND) {
            innerFilter = outerFilter.fields[0]
        } else  if (outerFilter.type == NOT) {
            innerFilter = outerFilter.field
        } else {
            innerFilter = null
        }

        expect:
        outerFilter.type == filterType
        innerFilter.type == subfilterType

        where:
        dimensions                  |  apiList                           | filterType | subfilterType
        [resources.d3]              |  [ "ageIdEq1234"]                  | OR         | SELECTOR
        [resources.d3]              |  [ "ageIdNotin56"]                 | NOT        | OR
        [resources.d3]              |  [ "ageIdEq1234", "ageDescEq1129"] | AND        | OR
        [resources.d2, resources.d3]|  [ "ageIdEq1234"]                  | AND        | OR
        [resources.d2, resources.d3]|  [ "ageIdNotin56"]                 | AND        | NOT
        [resources.d2, resources.d3]|  [ "ageIdEq1234", "ageDescEq1129"] | AND        | AND
    }
}
