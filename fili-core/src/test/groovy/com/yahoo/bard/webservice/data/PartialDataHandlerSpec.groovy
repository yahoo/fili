// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.WEEK
import static org.joda.time.DateTimeZone.UTC

import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.druid.model.query.AllGranularity
import com.yahoo.bard.webservice.druid.model.query.GroupByQuery
import com.yahoo.bard.webservice.metadata.TestDataSourceMetadataService
import com.yahoo.bard.webservice.table.Column
import com.yahoo.bard.webservice.table.ConcretePhysicalTable
import com.yahoo.bard.webservice.table.resolver.QueryPlanningConstraint
import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import org.joda.time.Interval

import spock.lang.Specification

/**
 * Tests for the Partial Data Handler
 */
class PartialDataHandlerSpec extends Specification {

    PartialDataHandler partialDataHandler = new PartialDataHandler()

    Dimension dim1, dim2, dim3
    ConcretePhysicalTable table

    Set<String> columnNames
    GroupByQuery groupByQuery = Mock(GroupByQuery.class)
    DimensionDictionary dimensionDictionary
    QueryPlanningConstraint dataSourceConstraint

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

        table = new ConcretePhysicalTable(
                "basefact_network",
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
                dataSourceConstraint,
                [table] as Set,
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
                dataSourceConstraint,
                [table] as Set,
                new SimplifiedIntervalList(dataSourceConstraint.getIntervals()),
                dataSourceConstraint.getRequestGranularity()
        )
    }

    SimplifiedIntervalList buildIntervals(List<String> intervals) {
        intervals.collect({ new Interval(it) }) as SimplifiedIntervalList
    }
}
