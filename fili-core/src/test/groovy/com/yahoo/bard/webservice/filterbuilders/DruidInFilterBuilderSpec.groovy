// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.filterbuilders

import static com.yahoo.bard.webservice.druid.model.filter.Filter.DefaultFilterType.AND
import static com.yahoo.bard.webservice.druid.model.filter.Filter.DefaultFilterType.IN
import static com.yahoo.bard.webservice.druid.model.filter.Filter.DefaultFilterType.NOT

import com.yahoo.bard.webservice.data.QueryBuildingTestingResources
import com.yahoo.bard.webservice.druid.model.builders.DruidFilterBuilder
import com.yahoo.bard.webservice.druid.model.builders.DruidInFilterBuilder
import com.yahoo.bard.webservice.druid.model.filter.Filter
import com.yahoo.bard.webservice.druid.model.filter.InFilter
import com.yahoo.bard.webservice.druid.model.filter.NotFilter
import com.yahoo.bard.webservice.web.ApiFilter
import com.yahoo.bard.webservice.web.apirequest.generator.filter.FilterBinders

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class DruidInFilterBuilderSpec extends Specification {

    @Shared QueryBuildingTestingResources resources

    Map filterSpecs
    Map apiFilters
    Map druidFilters
    DruidFilterBuilder filterBuilder
    FilterBinders filterBinders = FilterBinders.instance

    def setupSpec() {
        resources = new QueryBuildingTestingResources()
    }

    def setup() {
        filterSpecs = [ageIdEq1234 : "ageBracket|id-in[1,2,3,4]",
                       ageDescEq1129 : "ageBracket|desc-eq[11-14,14-29]",
                       ageIdNotin56   : "ageBracket|id-notin[5,6]",
                       ageDescNotin1429: "ageBracket|desc-notin[14-29]"]

        apiFilters = [:]
        filterSpecs.each {
            apiFilters.put(it.key, filterBinders.generateApiFilter(it.value, resources.dimensionDictionary))
        }

        Filter ageIdEq1234 = new InFilter(resources.d3, ["1", "2", "3", "4"])
        Filter ageDescEq1129 = new InFilter(resources.d3, ["2", "3"])
        Filter ageIdNotin56 = new NotFilter(new InFilter(resources.d3, ["5", "6"]))
        Filter ageDescNotin1429 = new NotFilter(new InFilter(resources.d3, ["3"]))

        druidFilters =  [
                ageIdEq1234: ageIdEq1234,
                ageDescEq1129: ageDescEq1129,
                ageIdNotin56: ageIdNotin56,
                ageDescNotin1429: ageDescNotin1429
        ]
        filterBuilder = new DruidInFilterBuilder()
    }

    def "If there are no filters to build, then the the filter builder returns null"(){
        expect:
        filterBuilder.buildFilters([:]) == null
    }

    def "Combinations of positive and negative API filters are consolidated into 2 filters"() {
        when:
        Filter filter = filterBuilder.buildFilters([(resources.d3): apiFilters.collect { it.value } as Set])

        then:
        new HashSet<>(filter.fields.get(0).values) == ["2", "3"] as HashSet
        new HashSet<>(filter.fields.get(1).field.values) == ["3", "5", "6"] as HashSet
    }

    @Unroll
    def "#filterString is a #outerFilterType-filter on #dimension, with values = #values"() {
        setup: "Init. API filters"
        ApiFilter filter = filterBinders.generateApiFilter(filterString, resources.dimensionDictionary)

        when: "We build a single in-filter"
        //resources.d3 is the ageBracket dimension.
        Filter outerFilter = filterBuilder.buildFilters([(resources.d3): [filter] as Set])

        and: "Extract the constructed in-filter"
        InFilter inFilter = outerFilter.type == NOT ? outerFilter.getField() : outerFilter

        then: "The extracted OrFilter has been correctly hydrated"
        outerFilter.type == outerFilterType
        if (outerFilterType == NOT) {
            assert outerFilter.getField().getDimension() == resources.dimensionDictionary.findByApiName(dimension)
            assert outerFilter.getField().getValues() == values as TreeSet
        } else {
            assert outerFilter.getDimension() == resources.dimensionDictionary.findByApiName(dimension)
            assert outerFilter.getValues() == values as TreeSet
        }

        where:
        filterString                      | outerFilterType | dimension    | values
        "ageBracket|id-eq[1]"             | IN              | "ageBracket" | ["1"]
        "ageBracket|id-in[1]"             | IN              | "ageBracket" | ["1"]
        "ageBracket|id-startswith[1]"     | IN              | "ageBracket" | ["1"]
        "ageBracket|id-contains[1]"       | IN              | "ageBracket" | ["1"]
        "ageBracket|id-notin[1]"          | NOT             | "ageBracket" | ["1"]
        "ageBracket|id-eq[1,2,4]"         | IN              | "ageBracket" | ["1", "2", "4"]
        "ageBracket|desc-eq[11-14,14-29]" | IN              | "ageBracket" | ["2", "3"]
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
        if (outerFilter.type == IN ) {
            assert druidExpected == [outerFilter]
        } else {
            assert filters.containsAll(druidExpected) && druidExpected.containsAll(filters)
        }

        where:
        apiName = resources.d3.apiName
        apiList                               | filterType | druidList
        ["ageIdEq1234"]                       | IN         | ["ageIdEq1234"]
        ["ageIdNotin56"]                      | NOT        | ["ageIdNotin56"]
        ["ageDescEq1129", "ageDescNotin1429"] | AND        | ["ageDescEq1129", "ageDescNotin1429"]
    }

    @Unroll
    def "Filtering for #apiList on #dimensions gives a #filterType-filter with subfilter type #subfilterType"() {
        setup:
        Set apiSet = (apiList.collect { apiFilters.get(it) }) as Set

        Map dimFilterMap = dimensions.collectEntries { [(it): apiSet] }

        Filter outerFilter = filterBuilder.buildFilters(dimFilterMap)
        Filter innerFilter
        if (outerFilter.type == IN) {
            innerFilter = outerFilter
        } else if (outerFilter.type == AND) {
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
        [resources.d3]              |  [ "ageIdEq1234"]                  | IN         | IN
        [resources.d3]              |  [ "ageIdNotin56"]                 | NOT        | IN
        [resources.d3]              |  [ "ageIdEq1234", "ageDescEq1129"] | IN         | IN
        [resources.d2, resources.d3]|  [ "ageIdEq1234"]                  | AND        | IN
        [resources.d2, resources.d3]|  [ "ageIdNotin56"]                 | AND        | NOT
        [resources.d2, resources.d3]|  [ "ageIdEq1234", "ageDescEq1129"] | AND        | IN
    }
}
