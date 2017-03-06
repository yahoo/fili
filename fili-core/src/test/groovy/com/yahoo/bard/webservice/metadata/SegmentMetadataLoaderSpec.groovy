// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.metadata

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.WEEK
import static org.joda.time.DateTimeZone.UTC

import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.metric.MetricColumn
import com.yahoo.bard.webservice.druid.client.DruidWebService
import com.yahoo.bard.webservice.druid.client.SuccessCallback
import com.yahoo.bard.webservice.models.druid.client.impl.TestDruidWebService
import com.yahoo.bard.webservice.table.ConcretePhysicalTable
import com.yahoo.bard.webservice.table.PhysicalTable
import com.yahoo.bard.webservice.table.PhysicalTableDictionary
import com.yahoo.bard.webservice.table.availability.ImmutableAvailability

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.joda.JodaModule

import org.joda.time.DateTimeZone
import org.joda.time.Interval

import spock.lang.Specification

class SegmentMetadataLoaderSpec extends Specification {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JodaModule())
            .registerModule(new Jdk8Module().configureAbsentsAsNulls(false))

    PhysicalTableDictionary tableDict = new PhysicalTableDictionary()
    DimensionDictionary dimensionDict = new DimensionDictionary()
    static {
        DateTimeZone.setDefault(UTC)
    }
    TestDruidWebService druidWS = new TestDruidWebService()
    MetricColumn metric1 = new MetricColumn("abe")
    MetricColumn metric2 = new MetricColumn("lincoln")

    String intervalString
    List<Interval> intervalList

    Map<MetricColumn, List<Interval>> expectedColumnCache

    String gappySegmentMetadataJson = """
{
    "2012-01-01T00:00:00.000Z/2013-12-27T00:00:00.000Z":
        {
            "dimensions":
                [
                    "product",
                    "region"
                ],
            "metrics":
                [
                    "page_views",
                    "timespent"
                ]
        },
    "2013-12-27T00:00:00.000Z/2013-12-28T00:00:00.000Z":
        {
            "dimensions":
                [
                    "product",
                    "region"
                ],
            "metrics":
                [
                    "page_views"
                ]
        },
    "2013-12-28T00:00:00.000Z/2015-03-17T00:00:00.000Z":
        {
            "dimensions":
                [
                    "product",
                    "region"
                ],
            "metrics":
                [
                    "page_views",
                    "timespent"
                ]
       }
}
"""

    def setup() {
        DateTimeZone.setDefault(UTC)

        intervalString = "2014-09-04T00:00:00.000Z/2014-09-06T00:00:00.000Z"
        intervalList = [new Interval(intervalString)]
        expectedColumnCache = [
                (metric1): intervalList,
                (metric2): intervalList
        ]

        ["tablename"].each {

            PhysicalTable table = new ConcretePhysicalTable(
                    it, WEEK.buildZonedTimeGrain(UTC),
                    [metric1, metric2] as Set
                    ,
                    [:]
            )
            tableDict.put(it, table)
        }
        String json = """
        {
            "${intervalString}":
                {
                    "dimensions":
                        [],
                    "metrics":
                        ["${metric1.name}", "${metric2.name}"]
                }
        }
        """
        druidWS.jsonResponse = {json}

    }

    def "test whether SegmentMetadataLoader loads table column cache appropriately"() {
        setup: "run the loader"
        SegmentMetadataLoader loader = new SegmentMetadataLoader(tableDict, dimensionDict, druidWS, MAPPER)
        loader.run()

        expect: "cache gets loaded as expected"
        tableDict.get("tablename").getAvailability() == new ImmutableAvailability("tablename", expectedColumnCache)
    }


    def "Test segment metadata can deserialize JSON correctly"() {
        setup:
        SegmentMetadataLoader loader = new SegmentMetadataLoader(tableDict, dimensionDict, druidWS, MAPPER)
        druidWS.jsonResponse = {gappySegmentMetadataJson}
        ConcretePhysicalTable table = Mock(ConcretePhysicalTable)
        SegmentMetadata capture

        when:
        SuccessCallback success = loader.buildSegmentMetadataSuccessCallback(table)
        success.invoke(MAPPER.readTree(gappySegmentMetadataJson))

        then:
        1 * table.resetColumns(_, dimensionDict) >> { segmentMetadata, dimensionDictionary ->
            capture = segmentMetadata
        }
        capture.getDimensionIntervals().containsKey("product")
        capture.getDimensionIntervals().get("product").size() == 1
        capture.getMetricIntervals().get("timespent").size() == 2
    }


    def "Test querySegmentMetadata builds callbacks and sends query"() {
        setup:
        DruidWebService testWs = Mock(DruidWebService)
        SegmentMetadataLoader loader = new SegmentMetadataLoader(tableDict, dimensionDict, testWs, MAPPER)
        ConcretePhysicalTable table = Mock(ConcretePhysicalTable)

        when:
        loader.querySegmentMetadata(table)

        then:
        1 * testWs.getJsonObject(_, _, _, _)

    }
}
