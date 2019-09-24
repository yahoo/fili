// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.MONTH
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.WEEK
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.YEAR
import static com.yahoo.bard.webservice.util.IntervalUtilsSpec.buildIntervalList
import static com.yahoo.bard.webservice.util.IntervalUtilsSpec.complementExpectedSets
import static org.joda.time.DateTimeZone.UTC

import com.yahoo.bard.webservice.data.config.names.TableName
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.time.AllGranularity
import com.yahoo.bard.webservice.data.time.Granularity
import com.yahoo.bard.webservice.druid.model.query.GroupByQuery
import com.yahoo.bard.webservice.metadata.TestDataSourceMetadataService
import com.yahoo.bard.webservice.table.Column
import com.yahoo.bard.webservice.table.physicaltables.StrictPhysicalTable
import com.yahoo.bard.webservice.table.resolver.QueryPlanningConstraint
import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import org.joda.time.DateTimeZone
import org.joda.time.Interval

import spock.lang.Specification
import spock.lang.Unroll

/**
 * Tests for the Partial Data Handler
 */
class PartialDataHandlerSpec extends Specification {

    PartialDataHandler partialDataHandler = new PartialDataHandler()

    Dimension dim1, dim2, dim3
    StrictPhysicalTable table

    Set<String> columnNames
    GroupByQuery groupByQuery = Mock(GroupByQuery.class)
    DimensionDictionary dimensionDictionary
    QueryPlanningConstraint dataSourceConstraint

    static DateTimeZone dateTimeZone

    def setupSpec() {
        dateTimeZone = DateTimeZone.getDefault()
        DateTimeZone.setDefault(UTC)
    }

    def cleanupSpec() {
        DateTimeZone.setDefault(dateTimeZone)
    }

    def setup() {
        columnNames = ["userDeviceType", "property", "os",  "page_views"] as Set

        // Setup mock dimensions
        dim1 = Mock(KeyValueStoreDimension.class)
        dim1.apiName >> "userDeviceType"
        dim2 = Mock(KeyValueStoreDimension.class)
        dim2.apiName >> "property"
        dim3 = Mock(KeyValueStoreDimension.class)
        dim3.apiName >> "os"

        dimensionDictionary = new DimensionDictionary([dim1, dim2, dim3] as Set)

        // Setup mock Request
        dataSourceConstraint = Stub(QueryPlanningConstraint)
        dataSourceConstraint.getAllColumnNames() >> columnNames

        /*
         * dim1 is missing four days of data internally, dim2 and dim3 are complete over the period and page_views is
         * starts inside the dim1 hole and goes to the end of the period.
         */
        Map<String, Set<Interval>> segmentIntervals = [
                'user_device_type': buildIntervals(["2014-07-01/2014-07-09","2014-07-11/2014-07-29"]) as Set,
                'property': buildIntervals(["2014-07-01/2014-07-29"]) as Set,
                'os': buildIntervals(["2014-07-01/2014-07-29"]) as Set,
                'page_views': buildIntervals(["2014-07-04/2014-07-29"]) as Set
        ]

        table = new StrictPhysicalTable(
                TableName.of("basefact_network"),
                DAY.buildZonedTimeGrain(UTC),
                [new Column("userDeviceType"), new Column("property"), new Column("os"), new Column("page_views")] as Set,
                ["userDeviceType": "user_device_type"],
                new TestDataSourceMetadataService(segmentIntervals)
        )
    }

    def "Given a weekly request make sure partiality rounds to the request weeks"() {
        setup:
        SimplifiedIntervalList expectedIntervals = buildIntervals(["2014-06-30/2014-07-14"])
        dataSourceConstraint.getRequestGranularity() >> WEEK
        dataSourceConstraint.getIntervals() >> [new Interval("2014-06-30/2014-07-28")]

        expect:
        expectedIntervals == partialDataHandler.findMissingTimeGrainIntervals(
                table.getAvailableIntervals(dataSourceConstraint),
                new SimplifiedIntervalList(dataSourceConstraint.getIntervals()),
                dataSourceConstraint.getRequestGranularity()
        )
    }

    def "Check that partial data on granularity 'all' with some partial intervals reports ALL intervals missing"() {
        setup:
        SimplifiedIntervalList requestedIntervals = buildIntervals(["2014-06-30/2014-07-14"])
        dataSourceConstraint.getRequestGranularity() >> AllGranularity.INSTANCE
        dataSourceConstraint.getIntervals() >> requestedIntervals

        expect:
        requestedIntervals == partialDataHandler.findMissingTimeGrainIntervals(
                table.getAvailableIntervals(dataSourceConstraint),
                new SimplifiedIntervalList(dataSourceConstraint.getIntervals()),
                dataSourceConstraint.getRequestGranularity()
        )
    }

    SimplifiedIntervalList buildIntervals(List<String> intervals) {
        intervals.collect({ (new Interval(it)) }) as SimplifiedIntervalList
    }


    @Unroll
    def "Complement of #supply yields #expected with fixed request and grain"() {
        setup:
        SimplifiedIntervalList supplyIntervals = buildIntervalList(supply)
        SimplifiedIntervalList requestedIntervals = buildIntervalList(['2014/2015'])
        Granularity granularity = MONTH
        SimplifiedIntervalList expectedIntervals = buildIntervalList(expected)

        expect:
        PartialDataHandler.collectBucketedIntervalsNotInIntervalList(
                supplyIntervals,
                requestedIntervals,
                granularity
        ) == expectedIntervals

        where:
        supply               | expected
        ["2013-04/2014-04"]  | ["2014-04/2015"]
        ["2014-04/2014-05"]  | ["2014/2014-04", "2014-05/2015"]
        ["2012-09/2016-05"]  | []
        ["2012/2013"]        | ["2014/2015"]
        ["2020/2023"]        | ["2014/2015"]
    }


    @Unroll
    def "Collect of #requestedIntervals by #grain yields #expected when fixed supply is removed"() {
        setup:
        SimplifiedIntervalList supply = buildIntervalList(['2012-05-04/2017-02-03'])
        SimplifiedIntervalList expectedIntervals = buildIntervalList(expected)
        SimplifiedIntervalList requestedIntervals = buildIntervalList(requested)
        Granularity granularity = grain

        expect:
        PartialDataHandler.collectBucketedIntervalsNotInIntervalList(
                supply,
                requestedIntervals,
                granularity
        ) == expectedIntervals

        where:
        grain | requested                 | expected
        YEAR  | ["2012/2017"]             | ["2012/2013"]
        MONTH | ["2012-02/2017"]          | ["2012-02/2012-06"]
        DAY   | ["2012-02-02/2016-05"]    | ["2012-02-02/2012-05-04"]
        DAY   | ["2012-06/2016-05"]       | []
        YEAR  | ["2013/2018"]             | ["2017/2018"]
        MONTH | ["2013/2017-04"]          | ["2017-02/2017-04"]
        DAY   | ["2012-02-03/2017-04-04"] | ["2012-02-03/2012-05-04", "2017-02-03/2017-04-04"]
    }

    @Unroll
    def "Complement cuts out the correct hole(s) when #comment"(
            Granularity granularity,
            String comment,
            List<String> fromAsStrings,
            List<String> removeAsStrings,
            List<String> expectedAsStrings
    ) {
        given:
        SimplifiedIntervalList from = buildIntervalList(fromAsStrings)
        SimplifiedIntervalList remove = buildIntervalList(removeAsStrings)
        SimplifiedIntervalList expected = buildIntervalList(expectedAsStrings)

        expect:
        PartialDataHandler.collectBucketedIntervalsNotInIntervalList(remove, from, granularity) == expected

        where:
        [granularity, comment, fromAsStrings, removeAsStrings, expectedAsStrings] << complementExpectedSets()
    }
}
