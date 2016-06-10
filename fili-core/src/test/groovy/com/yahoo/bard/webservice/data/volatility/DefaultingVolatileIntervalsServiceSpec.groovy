// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.data.volatility

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY

import com.yahoo.bard.webservice.druid.model.query.AllGranularity
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery
import com.yahoo.bard.webservice.table.PhysicalTable
import com.yahoo.bard.webservice.util.DefaultingDictionary
import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import org.joda.time.DateTime
import org.joda.time.Interval

import spock.lang.Specification

/**
 * Test the default intervals service
 */
class DefaultingVolatileIntervalsServiceSpec extends Specification {

    DruidAggregationQuery query = Mock(DruidAggregationQuery)
    DruidAggregationQuery allGrainQuery = Mock(DruidAggregationQuery)

    PhysicalTable table = Mock(PhysicalTable)
    PhysicalTable table2 = Mock(PhysicalTable)
    PhysicalTable table3 = Mock(PhysicalTable)

    VolatileIntervalsFunction defaultFunction = Mock(VolatileIntervalsFunction)
    VolatileIntervalsFunction mockFunction2 = Mock(VolatileIntervalsFunction)
    VolatileIntervalsFunction mockFunction3 = Mock(VolatileIntervalsFunction)

    List<Interval> volatileIntervals1, volatileIntervals2, volatileIntervals3

    final DateTime origin = new DateTime(0)
    final DateTime end = origin.plusDays(100)

    SimplifiedIntervalList fullRange = new SimplifiedIntervalList([new Interval(origin, end)])

    DefaultingDictionary<PhysicalTable, VolatileIntervalsFunction> intervalsFunctions

    def setup() {
        // Create intervals which are distinct subintervals from a common base interval
        (volatileIntervals1, volatileIntervals2, volatileIntervals3) = (1..3).collect {
            DateTime volatileStart = origin.plusDays(10*it)
            DateTime volatileEnd = origin.plusDays(10*it + 20)
            Interval volatileInterval = new Interval(volatileStart, volatileEnd)
            [volatileInterval]
        }

        defaultFunction.getVolatileIntervals() >> volatileIntervals1
        mockFunction2.getVolatileIntervals() >> volatileIntervals2
        mockFunction3.getVolatileIntervals() >> volatileIntervals3

        query.getIntervals() >> fullRange
        query.getGranularity() >> DAY

        allGrainQuery.getIntervals() >> fullRange
        allGrainQuery.getGranularity() >> AllGranularity.INSTANCE

        intervalsFunctions = new DefaultingDictionary<>(defaultFunction)
        intervalsFunctions.put(table2, mockFunction2)
        intervalsFunctions.put(table3, mockFunction3)
    }


    def "getVolatileIntervals returns the same values for every table with a single default service"() {
        given:
        DefaultingVolatileIntervalsService service = new DefaultingVolatileIntervalsService(defaultFunction)

        expect:
        service.getVolatileIntervals(query, table) == volatileIntervals1
        service.getVolatileIntervals(query, table2) == volatileIntervals1
        service.getVolatileIntervals(query, table3) == volatileIntervals1
    }

    def "All timegrain is the same for every table as well"() {
        given:
        DefaultingVolatileIntervalsService service = new DefaultingVolatileIntervalsService(defaultFunction)

        expect:
        service.getVolatileIntervals(allGrainQuery, table) == fullRange
        service.getVolatileIntervals(allGrainQuery, table2) == fullRange
        service.getVolatileIntervals(allGrainQuery, table3) == fullRange
    }

    def "getVolatileIntervals returns values from correct mapped or default service by table."() {
        given:
        DefaultingVolatileIntervalsService service = new DefaultingVolatileIntervalsService(
                defaultFunction,
                [ (table2): mockFunction2, (table3): mockFunction3 ]
        )

        expect:
        service.getVolatileIntervals(query, table) == volatileIntervals1
        service.getVolatileIntervals(query, table2) == volatileIntervals2
        service.getVolatileIntervals(query, table3) == volatileIntervals3
    }

    def "getVolatileIntervals for all granularity buckets correctly"() {
        given:
        DefaultingVolatileIntervalsService service = new DefaultingVolatileIntervalsService(intervalsFunctions)

        expect:
        service.getVolatileIntervals(allGrainQuery, table) == fullRange
        service.getVolatileIntervals(allGrainQuery, table2) == fullRange
        service.getVolatileIntervals(allGrainQuery, table3) == fullRange
    }
}
