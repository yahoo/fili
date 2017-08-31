// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.resolver

import com.yahoo.bard.webservice.data.PartialDataHandler
import com.yahoo.bard.webservice.data.QueryBuildingTestingResources
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.druid.model.query.Granularity
import com.yahoo.bard.webservice.web.DataApiRequest

import org.joda.time.DateTime
import org.joda.time.Interval

import spock.lang.Specification
import spock.lang.Unroll

class VolatileTimeComparatorSpec extends Specification {

    static final int HOUR_IS_BETTER = -1
    static final int DAY_IS_BETTER = 1
    static final int BOTH_ARE_EQUALLY_GOOD = 0

    //5.184 x 10^8
    static final long SIX_DAYS_IN_MILLISECONDS = 518400000L

    QueryBuildingTestingResources resources = new QueryBuildingTestingResources()

    @Unroll
    def "#hourAvailable and #hourVolatile is #betterworse than #dayAvailable and #dayVolatile"() {
        given: "Availability for the hour and day physical tables"
        resources.setupVolatileTables([
                [resources.volatileHourTable, new Interval(hourAvailable), new Interval(hourVolatile)],
                [resources.volatileDayTable, new Interval(dayAvailable), new Interval(dayVolatile)]
        ])

        and: "The api request of interest"
        DataApiRequest request = initializeApiRequest(
                [new Interval(new DateTime("2014-01-01"), new DateTime("2017-01-01"))],
                DefaultTimeGrain.YEAR
        )
        and: "The query of interest"
        TemplateDruidQuery query = Stub(TemplateDruidQuery)
        query.getInnermostQuery() >> query
        query.getTimeGrain() >> null
        query.getIntervals() >> (request.getIntervals() as List)

        and: "The volatile time comparator under test"
        VolatileTimeComparator comparator = new VolatileTimeComparator(
                new QueryPlanningConstraint(request, query),
                new PartialDataHandler(),
                resources.volatileIntervalsService
        )

        when: "We perform the comparison"
        int compareResult = comparator.compare(resources.volatileHourTable, resources.volatileDayTable)

        then: "The result is zero if we expect zero, otherwise it has the expected sign"
        compareResult == expectedCompare || (compareResult < 0) == (expectedCompare < 0)

        where:
        hourAvailable           | hourVolatile            | dayAvailable            | dayVolatile             | expectedCompare
        // Both have equal non-volatile data at the request grain
        "2014-01-01/2016-08-15" | "2016-08-15/2016-08-16" | "2014-01-01/2016-08-01" | "2016-08-01/2016-09-01" | HOUR_IS_BETTER //hour has more volatile data at the request grain
        "2014-01-01/2016-08-01" | "2016-08-01/2016-08-02" | "2014-01-01/2016-08-01" | "2016-08-01/2016-09-01" | BOTH_ARE_EQUALLY_GOOD //both have equal volatile data at the request grain
        "2014-01-01/2016-08-15" | "2016-08-15/2016-08-16" | "2014-01-01/2016-09-01" | "2016-09-01/2016-10-01" | DAY_IS_BETTER //day has more volatile data at the request grain
        // This check is based solely on how full the volatile bucket (2015-2016) is at the reqest level. It does not
        // consider whether or not non-volatile request buckets are partial
        "2014-02-01/2016-08-15" | "2016-08-15/2016-08-16" | "2014-01-01/2016-08-01" | "2016-08-01/2016-09-01" | HOUR_IS_BETTER //hour has more volatile data
        "2014-02-01/2016-08-15" | "2016-08-15/2016-08-16" | "2014-01-01/2016-09-01" | "2016-09-01/2016-10-01" | DAY_IS_BETTER //day has more volatile data
        // When an available request interval is also volatile, it is not factored into the comparison.
        "2014-01-01/2016-08-15" | "2016-08-01/2016-08-02" | "2014-01-01/2016-08-01" | "2014-01-01/2017-01-01" | HOUR_IS_BETTER // hour has more data available
        // It doesn't matter whether the volatile interval in the partial request interval is available or not.
        // All that matters is that the request interval is volatile and partial.
        // It also doesn't matter how long the volatile interval is at the table level.
        "2014-01-01/2016-08-15" | "2016-08-14/2016-08-15" | "2014-01-01/2016-08-01" | "2016-07-01/2016-08-01" | HOUR_IS_BETTER //hour has more data available at the request grain

        betterworse = expectedCompare == HOUR_IS_BETTER ?
                "better" :
                expectedCompare == DAY_IS_BETTER ? "worse" : "the same as"
    }


    @Unroll
    def "getAvailableVolatileDataDuration gives #duration with #available available, #volatility volatility, at #granularity"() {
        given: "Availability and volatility"
        resources.setupVolatileTables([
                [resources.volatileHourTable, new Interval(available), new Interval(volatility)]
        ])

        and: "The api request of interest"
        DataApiRequest request = initializeApiRequest(
                [new Interval(new DateTime("2007-01-01"), new DateTime("2007-01-08"))],
                requestGranularity
        )
        and: "The query of interest"
        TemplateDruidQuery query = Stub(TemplateDruidQuery)
        query.getInnermostQuery() >> query
        query.getTimeGrain() >> null
        query.getIntervals() >> (request.getIntervals() as List)

        and: "The volatile time comparator under test"
        VolatileTimeComparator comparator = new VolatileTimeComparator(
                new QueryPlanningConstraint(request, query),
                new PartialDataHandler(),
                resources.volatileIntervalsService
        )

        expect:
        comparator.getAvailableVolatileDataDuration(resources.volatileHourTable) == duration

        where:
        available               | volatility              | requestGranularity    || duration
        //The partial bucket (the 6th) is missing completely
        "2007-01-01/2007-01-07" | "2007-01-07/2007-01-08" | DefaultTimeGrain.DAY  || 0
        //The table has some but not all the data in the volatile bucket
        "2007-01-01/2007-01-07" | "2007-01-07/2007-01-08" | DefaultTimeGrain.WEEK || SIX_DAYS_IN_MILLISECONDS
        //The table doesn't have any volatile intervals in the request range. So there is no data
        //that is volatile and partial
        "2007-01-01/2014-01-07" | "2016/2017"             | DefaultTimeGrain.DAY  || 0
        "2007-01-01/2014-01-07" | "2016/2017"             | DefaultTimeGrain.WEEK || 0
        //All the data is available. In this case, there is no partial data, so
        //there is no data that is volatile and partial.
        "2007-01-01/2014-01-08" | "2007-01-07/2007-01-08" | DefaultTimeGrain.DAY  || 0
        "2007-01-01/2014-01-08" | "2007-01-07/2007-01-08" | DefaultTimeGrain.WEEK || 0
        //None of the volatile data is available.
        "2013/2014"             | "2007-01-07/2007-01-08" | DefaultTimeGrain.DAY  || 0
        "2013/2014"             | "2007-01-07/2007-01-08" | DefaultTimeGrain.WEEK || 0
    }

    private DataApiRequest initializeApiRequest(List<Interval> intervals, Granularity granularity) {
        Stub(DataApiRequest) {
            getIntervals() >> intervals
            getDimensions() >> [resources.d1]
            getFilterDimensions() >> []
            getGranularity() >> granularity
        }
    }
}
