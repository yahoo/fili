// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.metadata

import com.yahoo.bard.webservice.application.ObjectMappersSuite

import com.fasterxml.jackson.databind.ObjectMapper

import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.Interval

import io.druid.timeline.DataSegment
import spock.lang.Specification

class DataSegmentJacksonSpec extends Specification {

    ObjectMappersSuite rawMappers = new ObjectMappersSuite();
    String dataSegmentJson = """
{
    "dataSource": "dataSource",
    "interval": "2016-03-22T14:00:00.000Z/2016-03-22T15:00:00.000Z",
    "version": "v0",
    "loadSpec": {},
    "dimensions": "",
    "metrics": "",
    "shardSpec":
        {
            "type":"numbered",
            "partitionNum":0,
            "partitions":1
        },
    "binaryVersion": null,
    "size": 1,
    "identifier": "dataSource_2016-03-22T14:00:00.000Z_2016-03-22T15:00:00.000Z_v0"
}
"""
    // NOTE: This code is very volatile as druid versions change.  Future changes may not be backwards compatible.
    def "Test deserialization of data segments"() {
        setup:
        ObjectMapper mapper = new ObjectMappersSuite().mapper;
        DateTime expectedStart = (new DateTime("2016-03-22T14:00:00.000Z")).withZone(DateTimeZone.forID("UTC"))
        DateTime expectedEnd = (new DateTime("2016-03-22T15:00:00.000Z")).withZone(DateTimeZone.forID("UTC"))
        Interval expectedInterval = new Interval(expectedStart, expectedEnd);

        expect:
        DataSegment dataSegment = mapper.readValue(dataSegmentJson, DataSegment.class)
        dataSegment.binaryVersion == null
        dataSegment.identifier == "dataSource_2016-03-22T14:00:00.000Z_2016-03-22T15:00:00.000Z_v0"
        dataSegment.size == 1
        dataSegment.dataSource == "dataSource"
        dataSegment.interval == expectedInterval
        dataSegment.version == "v0"
        dataSegment.loadSpec == Collections.emptyMap()
    }
}
