// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data

import static com.yahoo.bard.webservice.config.BardFeatureFlag.PERMISSIVE_COLUMN_AVAILABILITY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.WEEK
import static org.joda.time.DateTimeZone.UTC

import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.druid.model.query.AllGranularity
import com.yahoo.bard.webservice.druid.model.query.GroupByQuery
import com.yahoo.bard.webservice.metadata.SegmentMetadata
import com.yahoo.bard.webservice.table.ConcretePhysicalTable
import com.yahoo.bard.webservice.table.PhysicalTable
import com.yahoo.bard.webservice.table.resolver.QueryPlanningConstraint
import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import org.joda.time.Interval

import spock.lang.Specification
import spock.lang.Unroll

/**
 * Tests for the Partial Data Handler
 */
class PartialDataHandlerSpec extends Specification {

    PartialDataHandler partialDataHandler = new PartialDataHandler()
    static boolean originalConfig = PERMISSIVE_COLUMN_AVAILABILITY.isOn()

    Dimension dim1, dim2, dim3
    Set<PhysicalTable> tables = [new ConcretePhysicalTable(
            "basefact_network",
            DAY.buildZonedTimeGrain(UTC),
            [] as Set,
            ["userDeviceType": "user_device_type"]
    )] as Set

    Set<String> columnNames
    GroupByQuery groupByQuery = Mock(GroupByQuery.class)
    DimensionDictionary dimensionDictionary
    QueryPlanningConstraint dataSourceConstraint

    def cleanup() {
        PERMISSIVE_COLUMN_AVAILABILITY.setOn(originalConfig)
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
        SegmentMetadata segments = new SegmentMetadata(
                [("userDeviceType"): buildIntervals(["2014-07-01/2014-07-09","2014-07-11/2014-07-29"]) as LinkedHashSet,
                 ("property"): buildIntervals(["2014-07-01/2014-07-29"]) as LinkedHashSet,
                 ("os"): buildIntervals(["2014-07-01/2014-07-29"]) as LinkedHashSet],
                [("page_views"): buildIntervals(["2014-07-04/2014-07-29"]) as Set]
        )

        tables*.resetColumns(segments, dimensionDictionary)
    }

    @Unroll
    def "Merging all columns produces #expected with #comment"() {
        setup:
        PERMISSIVE_COLUMN_AVAILABILITY.setOn(on)

        expect:
        partialDataHandler.getAvailability(tables[0], columnNames) == expected

        where:
        on    |  comment       |  expected
        true  | "Union"        |  buildIntervals(["2014-07/2014-07-29"])
        false | "Intersection" |  buildIntervals(["2014-07-04/2014-07-09","2014-07-11/2014-07-29"])
    }

    @Unroll
    def "Get availability returns correct result when an unavailable column is requested using #comment"() {
        setup:
        PERMISSIVE_COLUMN_AVAILABILITY.setOn(on)
        columnNames.add("missing_column")

        expect:
        partialDataHandler.getAvailability(tables[0], columnNames) == expected

        where:
        on    |  comment       |  expected
        true  | "Union"        |  buildIntervals(["2014-07/2014-07-29"])
        false | "Intersection" |  buildIntervals([])
    }


    def "Given a weekly request make sure partiality rounds to the request weeks"() {
        setup:
        PERMISSIVE_COLUMN_AVAILABILITY.setOn(false)
        SimplifiedIntervalList expectedIntervals = buildIntervals(["2014-06-30/2014-07-14"])
        dataSourceConstraint.getRequestGranularity() >> WEEK
        dataSourceConstraint.getIntervals() >> [new Interval("2014-06-30/2014-07-28")]

        expect:
        expectedIntervals == partialDataHandler.findMissingTimeGrainIntervals(
                dataSourceConstraint.getAllColumnNames(),
                tables,
                new SimplifiedIntervalList(dataSourceConstraint.getIntervals()),
                dataSourceConstraint.getRequestGranularity()
        )
    }

    def "Check that partial data on granularity 'all' with some partial intervals reports ALL intervals missing"() {
        setup:
        PERMISSIVE_COLUMN_AVAILABILITY.setOn(false)
        SimplifiedIntervalList requestedIntervals = buildIntervals(["2014-06-30/2014-07-14"])
        dataSourceConstraint.getRequestGranularity() >> AllGranularity.INSTANCE
        dataSourceConstraint.getIntervals() >> requestedIntervals

        expect:
        requestedIntervals == partialDataHandler.findMissingTimeGrainIntervals(
                dataSourceConstraint.getAllColumnNames(),
                tables,
                new SimplifiedIntervalList(dataSourceConstraint.getIntervals()),
                dataSourceConstraint.getRequestGranularity()
        )
    }

    SimplifiedIntervalList buildIntervals(List<String> intervals) {
        intervals.collect({ new Interval(it) }) as SimplifiedIntervalList
    }
}
