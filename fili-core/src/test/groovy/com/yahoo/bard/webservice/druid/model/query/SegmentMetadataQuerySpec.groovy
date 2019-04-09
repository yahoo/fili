// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.query

import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.data.config.names.DataSourceName
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.druid.model.datasource.DataSource
import com.yahoo.bard.webservice.druid.model.datasource.TableDataSource
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService
import com.yahoo.bard.webservice.table.TableTestUtils
import com.yahoo.bard.webservice.util.GroovyTestUtils

import com.fasterxml.jackson.databind.ObjectMapper

import org.joda.time.DateTimeZone
import org.joda.time.Interval

import spock.lang.Shared
import spock.lang.Specification

class SegmentMetadataQuerySpec extends Specification {
    private static final ObjectMapper MAPPER = new ObjectMappersSuite().getMapper()

    @Shared
    DateTimeZone currentTZ

    def setupSpec() {
        currentTZ = DateTimeZone.getDefault()
        // Default JodaTime zone to UTC
        DateTimeZone.setDefault(DateTimeZone.UTC);
    }

    def "SegmentMetadataQuery serializes to JSON correctly with one interval"() {
        given: "A Table data source and interval"
        String tableName = "basefact_network"
        DataSource dataSource = new TableDataSource(TableTestUtils.buildTable(
                tableName,
                DefaultTimeGrain.DAY.buildZonedTimeGrain(DateTimeZone.UTC),
                [] as Set,
                [:],
                Mock(DataSourceMetadataService) { getAvailableIntervalsByDataSource(_ as DataSourceName) >> [:]}
        ))
        Collection<Interval> intervals = [new Interval("2014-07-01/2014-07-15")]

        when: "We create and serialize a SegmentMetadataQuery"
        String segmentMetadataQueryStr = createSerializedSegmentMetadataQuery(dataSource, intervals)

        then: "The serialized JSON is what we expect"
        String expectedQuery =  """
            {
                "queryType":"segmentMetadata",
                "dataSource": {
                    "type": "table",
                    "name": "$tableName"
                },
                "intervals":["2014-07-01T00:00:00.000Z/2014-07-15T00:00:00.000Z"],
                "context": {}
            }"""
        GroovyTestUtils.compareJson(segmentMetadataQueryStr, expectedQuery)
    }

    def "SegmentMetadataQuery serializes to JSON correctly with multiple intervals"() {
        given: "A Table data source and interval"
        String tableName = "basefact_network"
        DataSource dataSource = new TableDataSource(TableTestUtils.buildTable(
                tableName,
                DefaultTimeGrain.DAY.buildZonedTimeGrain(DateTimeZone.UTC),
                [] as Set,
                [:],
                Mock(DataSourceMetadataService) { getAvailableIntervalsByDataSource(_ as DataSourceName) >> [:]}
        ))
        Collection<Interval> intervals = [new Interval("2014-07-01/2014-07-15"), new Interval("2014-08-01/2014-08-15")]

        when: "We create and serialize a SegmentMetadataQuery"
        String segmentMetadataQueryStr = createSerializedSegmentMetadataQuery(dataSource, intervals)

        then: "The serialized JSON is what we expect"
        String expectedQuery =  """
            {
                "queryType":"segmentMetadata",
                "dataSource": {
                    "type": "table",
                    "name": "$tableName"
                },
                "intervals":[
                    "2014-07-01T00:00:00.000Z/2014-07-15T00:00:00.000Z",
                    "2014-08-01T00:00:00.000Z/2014-08-15T00:00:00.000Z"
                ],
                "context": {}
            }"""
        GroovyTestUtils.compareJson(segmentMetadataQueryStr, expectedQuery)
    }

    private static String createSerializedSegmentMetadataQuery(DataSource dataSource, Collection<Interval> intervals) {
        MAPPER.writeValueAsString(new SegmentMetadataQuery(dataSource, intervals))
    }
}
