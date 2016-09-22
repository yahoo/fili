// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.YEAR

import com.yahoo.bard.webservice.data.PartialDataHandler
import com.yahoo.bard.webservice.data.Result
import com.yahoo.bard.webservice.data.metric.mappers.PartialDataResultSetMapper
import com.yahoo.bard.webservice.druid.model.query.AllGranularity
import com.yahoo.bard.webservice.table.PhysicalTableDictionary
import com.yahoo.bard.webservice.table.Schema
import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import org.joda.time.DateTime
import org.joda.time.Interval
import org.joda.time.Period

import spock.lang.Specification
import spock.lang.Unroll

import java.util.function.Supplier

public class PartialDataResultSetMapperSpec extends Specification {

    PhysicalTableDictionary physicalTableDictionary = Mock(PhysicalTableDictionary)
    SimplifiedIntervalList intervals = SimplifiedIntervalList.NO_INTERVALS
    Schema schema = Mock(Schema)
    Result result = Mock(Result)
    PartialDataHandler handler = new PartialDataHandler()

    def setup() {
        result.getTimeStamp() >> new DateTime()
        schema.getGranularity() >> DAY
    }

    def "Test constructor initializes missing intervals"() {
        when: "Create a PDRS Mapper"
        PartialDataResultSetMapper mapper = buildMapper(intervals)

        then: "Check initialized"
        mapper.missingIntervals == intervals
    }

    def "Test mapping a schema results the schema"() {
        when: "Create a PDRS Mapper"
        PartialDataResultSetMapper mapper = buildMapper(intervals)

        then: "It returns the schema unchanged"
        mapper.map(schema) == schema
    }

    def "Test result returned with no missing intervals"() {
        setup:
        DateTime start = new DateTime("2013-01-01")

        when: "Missing intervals is empty, for a time grain of a year"
        PartialDataResultSetMapper mapper = buildMapper(intervals)

        then: "Mapper returns the unmodified result"
        mapper.map(result, schema) == result
    }

    @Unroll
    def "If #gapIntervals gap overlaps a fixed request, data is filtered: #filtered"() {
        setup: "Given a request for a fixed duration and a year"
        Schema schema = Mock(Schema)
        schema.getGranularity() >> YEAR
        DateTime start = new DateTime("2014-01-01")

        and: "A mapper with a gap interval"
        PartialDataResultSetMapper mapper = buildMapper(buildIntervalList(gapIntervals[0], gapIntervals[1]))
        Result result = Mock(Result)
        1 * result.getTimeStamp() >> start

        expect: "Results are filtered if the request duration overlaps the gap"
        mapper.map(result, schema) == (filtered ? null : result)

        where:
        gapIntervals            | filtered
        ["2014-01-03", "P1Y"]   | true
        ["2014-04-02", "P1W"]   | true
        ["2014-01-01", "P1Y"]   | true
        ["2013-01-01", "P1Y"]   | false
        ["2015-01-01", "P1Y"]   | false
    }

    @Unroll
    def "Interval starting on #request #doesText filter when is volatile: #isVolatile and is missing #isMissing"() {
        setup: "Given an overlapping missing interval and volatile interval"
        SimplifiedIntervalList missingData = buildIntervalList("2015", "P1Y")
        Supplier<SimplifiedIntervalList> volatileData = { ->  buildIntervalList("2014-06", "P1Y")}
        PartialDataResultSetMapper mapper = buildMapper(missingData, volatileData)
        Result result = new Result([:], [:], new DateTime(request))

        expect:
        mapper.map(result, schema) == (filterAllows ? result : null)

        where:
        request        | isVolatile    | isMissing
        "2014-10"      | true          | false
        "2014-12"      | true          | true
        "2015-06"      | false         | true
        "2013"         | false         | false
        "2016"         | false         | false

        filterAllows = (isVolatile || ! isMissing)
        doesText = (filterAllows) ? "does" : "does not"
    }

    @Unroll
    def "With #volatileIntervals, #expected is missing and not volatile"() {
        setup:
        SimplifiedIntervalList missingList = buildIntervalList("2015", "P1Y")
        SimplifiedIntervalList volatileList = buildIntervalList(volatileIntervals)
        SimplifiedIntervalList expectedList = buildIntervalList(expected)
        PartialDataResultSetMapper mapper = buildMapper(missingList, { -> volatileList })

        expect:
        mapper.missingNotVolatile == expectedList

        where:
        volatileIntervals       | expected
        [["2015-02", "P1M"]]    | [["2015-01", "P1M"], ["2015-03", "P10M"]]
        [["2015-02", "P1Y"]]    | [["2015-01", "P1M"]]
        [["2015", "P1Y"]]       | []
    }

    @Unroll
    def "Under all time grain, missing data #missingIntervals and volatileData #missingIntervals is filtered: #filtered"() {
        setup: "Given an all time grain request"
        Schema schema1 = Mock(Schema)
        schema1.getGranularity() >> AllGranularity.INSTANCE

        and: "some possibly missing or volatile intervals"
        SimplifiedIntervalList missingData = buildIntervalList(missingIntervals)
        Supplier<SimplifiedIntervalList> volatileData = { -> buildIntervalList(volatileIntervals) }
        PartialDataResultSetMapper mapper = buildMapper(missingData, volatileData)
        Result result = new Result([:], [:], new DateTime("2014"))

        expect:
        mapper.map(result, schema1) ==  filtered ? null : result

        where:
        missingIntervals        | volatileIntervals     | filtered
        [["2014", "P1Y"]]       | [["2014", "P1Y"]]     | false
        [["2014", "P1Y"]]       | [["2017", "P1M"]]     | false
        [["2015", "P1Y"]]       | []                    | true
        []                      | []                    | false
        []                      | [["2014", "P1Y"]]     | false
    }

    SimplifiedIntervalList buildIntervalList(List<List<String>> intervals) {
        new SimplifiedIntervalList(intervals.collect({ it -> buildInterval(it[0], it[1]) }))
    }

    SimplifiedIntervalList buildIntervalList(String start, String end) {
        new SimplifiedIntervalList([buildInterval(start, end)])
    }

    Interval buildInterval(String start, String end) {
        new Interval(new DateTime(start), new Period(end))
    }

    PartialDataResultSetMapper buildMapper(
            SimplifiedIntervalList intervals,
            Supplier<SimplifiedIntervalList> provider = { -> SimplifiedIntervalList.NO_INTERVALS }
    ) {
        return new PartialDataResultSetMapper(intervals, provider)
    }
}
