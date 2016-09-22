// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.resolver

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.MONTH
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.WEEK
import static org.joda.time.DateTimeZone.UTC

import com.yahoo.bard.webservice.data.QueryBuildingTestingResources
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery
import com.yahoo.bard.webservice.druid.model.query.Granularity
import com.yahoo.bard.webservice.web.DataApiRequest

import org.joda.time.Interval

import spock.lang.Specification
import spock.lang.Unroll

/**
 * Test the Grain resolution in RequestQueryGranularityResolver
 */
class RequestQueryGranularityResolverSpec extends Specification {

    Set metricNamesSet
    QueryBuildingTestingResources resources = new QueryBuildingTestingResources()
    List<Interval> intervalList = [resources.interval1]

    Map apiRequestPrototype;
    Map queryPrototype;
    RequestQueryGranularityResolver resolver = new RequestQueryGranularityResolver()

    def setup() {
        metricNamesSet = [resources.m1.name]

        apiRequestPrototype = [
                table: resources.lt12,
                dimensions: [],
                intervals: intervalList,
                granularity: DAY,
                filterDimensions: [],
                logicalMetrics: [] as Set
        ]

        queryPrototype = [
                filteredMetricDimensions: [] as Set,
                dependantFieldNames: metricNamesSet,
                timeGrain: null
        ]
    }

    TemplateDruidQuery buildQuery(Map<String, Object> prototype) {
        TemplateDruidQuery query = Mock(TemplateDruidQuery)
        query.innermostQuery >> query
        query.dependentFieldNames >> prototype['dependantFieldNames']
        query.metricDimensions >> prototype['filteredMetricDimensions']
        query.timeGrain >> prototype['timeGrain']
        return query
    }

    @Unroll
    def "getCoarsestValidTimeGrain is '#minGrain' when API grain = '#apiGrain' and query grain = '#queryGrain'"() {
        DataApiRequest apiRequest = Mock(DataApiRequest)
        apiRequest.granularity >> apiGrain.buildZonedTimeGrain(UTC)
        apiRequest.getTimeZone() >> UTC
        Granularity expected = minGrain.buildZonedTimeGrain(UTC)
        queryPrototype['timeGrain'] = queryGrain
        TemplateDruidQuery query = buildQuery(queryPrototype);

        expect:
        resolver.apply(apiRequest, query) == expected

        where:
        apiGrain    | queryGrain        | minGrain
        DAY         | DAY               | DAY
        DAY         | null              | DAY
        WEEK        | DAY               | DAY
        MONTH       | DAY               | DAY
    }

    @Unroll
    def "getCoarsestValidTimeGrain: error thrown when API grain = '#apiGrain' and query grain = '#queryGrain'"() {
        setup:
        DataApiRequest apiRequest = Mock(DataApiRequest)
        apiRequest.granularity >> apiGrain
        apiRequest.timeZone >> UTC

        queryPrototype['timeGrain'] = queryGrain
        TemplateDruidQuery query = buildQuery(queryPrototype);

        when:
        resolver.apply(apiRequest, query)

        then:
        thrown(IllegalArgumentException)

        where:
        apiGrain        | queryGrain
        DAY             | MONTH
        DAY             | WEEK
        MONTH           | WEEK
        WEEK            | MONTH
    }
}
