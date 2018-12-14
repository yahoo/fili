// Copyright 2018 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.filterbuilders

import static com.yahoo.bard.webservice.druid.model.filter.Filter.DefaultFilterType.AND
import static com.yahoo.bard.webservice.druid.model.filter.Filter.DefaultFilterType.BOUND

import com.yahoo.bard.webservice.data.QueryBuildingTestingResources
import com.yahoo.bard.webservice.data.dimension.FilterBuilderException
import com.yahoo.bard.webservice.data.filterbuilders.DruidBoundFilterBuilder
import com.yahoo.bard.webservice.druid.model.builders.DruidFilterBuilder
import com.yahoo.bard.webservice.druid.model.filter.BoundFilter
import com.yahoo.bard.webservice.druid.model.filter.Filter
import com.yahoo.bard.webservice.web.ApiFilter
import com.yahoo.bard.webservice.web.FilterOperation
import com.yahoo.bard.webservice.web.apirequest.binders.FilterBinders

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class DruidBoundFilterBuilderSpec extends Specification {
    @Shared QueryBuildingTestingResources resources
    Map filterSpecs
    Map apiFilters
    Map druidFilters
    DruidFilterBuilder filterBuilder
    FilterBinders filterBinders = FilterBinders.INSTANCE

    def setupSpec() {
        resources = new QueryBuildingTestingResources()
    }

    def setup() {
        filterSpecs = [
                startDateIdLt     : "dim7|id-lt[2018-08-10]",
                startDateIdLte    : "dim7|id-lte[2018-08-10]",
                startDateIdGt     : "dim7|id-gt[2016-08-10]",
                startDateIdGte    : "dim7|id-gte[2016-08-10]",
                startDateIdBetween: "dim7|id-between[2016-08-10,2018-08-10]"
        ]
        apiFilters = [:]
        filterSpecs.each {
            apiFilters.put(it.key, filterBinders.generateApiFilter(it.value, resources.dimensionDictionary))
        }

        Filter startDateLtBoundFilter = new BoundFilter(resources.d7, null, "2018-08-10").withUpperBoundStrict(true)
        Filter startDateLteBoundFilter = new BoundFilter(resources.d7, null, "2018-08-10").withUpperBoundStrict(false)
        Filter startDateGteBoundFilter = new BoundFilter(resources.d7, "2016-08-10", null).withLowerBoundStrict(false)
        Filter startDateGtBoundFilter = new BoundFilter(resources.d7, "2016-08-10", null).withLowerBoundStrict(true)
        Filter startDateBetweenBoundFilter = new BoundFilter(resources.d7, "2016-08-10", "2018-08-10").
                withUpperBoundStrict(true).withLowerBoundStrict(false)

        druidFilters = [
                startDateIdLt     : startDateLtBoundFilter,
                startDateIdLte    : startDateLteBoundFilter,
                startDateIdGt     : startDateGtBoundFilter,
                startDateIdGte    : startDateGteBoundFilter,
                startDateIdBetween: startDateBetweenBoundFilter
        ]

        filterBuilder = new DruidBoundFilterBuilder()
    }

    @Unroll
    def "Filtering for #apiList on #apiName gives us a Druid filter of type #filterType on #druidList"() {
        setup:
        LinkedHashSet apiSet = apiList.collect { apiFilters.get(it) } as LinkedHashSet

        List druidExpected = druidList.collect() { druidFilters.get(it) }

        when:
        Filter outerFilter = filterBuilder.buildFilters([(resources.d7): apiSet])
        List<Filter> filters = outerFilter.type == AND ? outerFilter.fields : [outerFilter]

        then:
        outerFilter.type == filterType
        if (outerFilter.type == BOUND) {
            assert druidExpected == [outerFilter]
        } else {
            assert filters.containsAll(druidExpected) && druidExpected.containsAll(filters)
        }

        where:
        apiName = resources.d7.apiName

        apiList | filterType | druidList
        ["startDateIdLt"] | BOUND | ["startDateIdLt"]
        ["startDateIdLt", "startDateIdGte"] | AND | ["startDateIdLt", "startDateIdGte"]
        ["startDateIdBetween"] | BOUND | ["startDateIdBetween"]
    }

    @Unroll
    def "buildDruidBoundFilters method build appropriate Bound Druid Filter #druidList based on the API Filters"() {
        setup:
        ApiFilter apiFilter = filterBinders.generateApiFilter(apiFilterString, resources.dimensionDictionary)
        List druidExpected = druidList.collect() { druidFilters.get(it) }

        when:
        DruidBoundFilterBuilder boundFilterBuilder = new DruidBoundFilterBuilder();
        Filter outerFilter = boundFilterBuilder.buildDruidBoundFilters(apiFilter)

        then:
        assert druidExpected == [outerFilter]

        where:
        apiFilterString                          | druidList
        "dim7|id-lt[2018-08-10]"                 | ["startDateIdLt"]
        "dim7|id-lte[2018-08-10]"                | ["startDateIdLte"]
        "dim7|id-gte[2016-08-10]"                | ["startDateIdGte"]
        "dim7|id-gt[2016-08-10]"                 | ["startDateIdGt"]
        "dim7|id-between[2016-08-10,2018-08-10]" | ["startDateIdBetween"]
    }

    @Unroll
    def "ApiFilter is validated for correct number of arguments"() {
        setup:
        ApiFilter apiFilter = filterBinders.generateApiFilter(apiFilterString, resources.dimensionDictionary)
        DruidBoundFilterBuilder boundFilterBuilder = new DruidBoundFilterBuilder();

        when:
        boundFilterBuilder.validateFilter(apiFilter)

        then:
        IllegalArgumentException exception = thrown(IllegalArgumentException)
        exception.cause instanceof FilterBuilderException
        exception.cause.message.contains(druidList)

        where:
        apiFilterString                 | druidList
        "dim7|id-bet[1]"                | "exactly"
        "dim7|id-lte[2018-08-10, 2018]" | "exactly"
    }

    @Unroll
    def "ApiFilter validates for DefaultOperations"() {
        setup:
        DruidBoundFilterBuilder boundFilterBuilder = new DruidBoundFilterBuilder();
        FilterOperation filterOperation = Mock FilterOperation
        ApiFilter apiFilter = filterBinders.generateApiFilter(apiFilterString, resources.dimensionDictionary).
                withOperation(filterOperation)

        when:
        boundFilterBuilder.validateFilter(apiFilter)

        then:
        IllegalArgumentException exception = thrown(IllegalArgumentException)
        exception.cause instanceof FilterBuilderException
        exception.cause.message.contains(druidList)

        where:
        apiFilterString                 | druidList
        "dim7|id-bet[1]"                | "invalid"
    }
}
