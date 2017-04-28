// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.metadata

import com.yahoo.bard.webservice.data.config.names.TestApiDimensionName
import com.yahoo.bard.webservice.data.config.names.TestApiMetricName
import com.yahoo.bard.webservice.data.config.names.TestDruidTableName
import com.yahoo.bard.webservice.druid.model.metadata.NumberedShardSpec

import com.google.common.collect.Range
import com.google.common.collect.RangeSet
import com.google.common.collect.TreeRangeSet

import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.Interval
import org.joda.time.format.DateTimeFormat

import io.druid.timeline.DataSegment
import spock.lang.Shared
import spock.lang.Specification

class BaseDataSourceMetadataSpec extends Specification {
    @Shared
    String tableName = TestDruidTableName.ALL_PETS.asName()

    @Shared
    String intervalString1
    @Shared
    String intervalString2
    @Shared
    String intervalString12

    @Shared
    Interval interval1
    @Shared
    Interval interval2
    @Shared
    Interval interval12

    @Shared
    RangeSet<DateTime> rangeSet12

    @Shared
    String version1
    @Shared
    String version2

    @Shared
    DataSegment segment1

    @Shared
    DataSegment segment2

    @Shared
    DataSegment segment3

    @Shared
    DataSegment segment4

    @Shared
    List<DataSegment> segments

    @Shared
    DateTimeZone currentTZ

    @Shared
    List<String> dimensions123

    @Shared
    List<String> metrics123

    def setupSpec() {
        currentTZ = DateTimeZone.getDefault()
        DateTimeZone.setDefault(DateTimeZone.UTC)

        intervalString1 = "2015-01-01T00:00:00.000Z/2015-01-02T00:00:00.000Z"
        intervalString2 = "2015-01-02T00:00:00.000Z/2015-01-03T00:00:00.000Z"
        intervalString12 = "2015-01-01T00:00:00.000Z/2015-01-03T00:00:00.000Z"
        interval1 = Interval.parse(intervalString1)
        interval2 = Interval.parse(intervalString2)
        interval12 = Interval.parse(intervalString12)
        rangeSet12 = TreeRangeSet.create()
        rangeSet12.add(Range.closedOpen(interval12.getStart(), interval12.getEnd()))
        version1 = DateTimeFormat.fullDateTime().print(DateTime.now().minusDays(1))
        version2 = DateTimeFormat.fullDateTime().print(DateTime.now())

        TestApiDimensionName dim1 = TestApiDimensionName.BREED
        TestApiDimensionName dim2 = TestApiDimensionName.SPECIES
        TestApiDimensionName dim3 = TestApiDimensionName.SEX

        dimensions123 = [dim1, dim2, dim3]*.asName()

        TestApiMetricName met1 = TestApiMetricName.A_ROW_NUM
        TestApiMetricName met2 = TestApiMetricName.A_LIMBS
        TestApiMetricName met3 = TestApiMetricName.A_DAY_AVG_LIMBS

        metrics123 = [met1, met2, met3]*.asName()

        NumberedShardSpec partition1 = Stub(NumberedShardSpec) {
            getPartitionNum() >> 0
        }

        NumberedShardSpec partition2 = Stub(NumberedShardSpec) {
            getPartitionNum() >> 1
        }

        Integer binversion1 = 9
        long size1 = 1024
        long size2 = 512

        segment1 = new DataSegment(
                tableName,
                interval1,
                version1,
                null,
                dimensions123,
                metrics123,
                partition1,
                binversion1,
                size1
        )

        segment2 = new DataSegment(
                tableName,
                interval1,
                version2,
                null,
                dimensions123,
                metrics123,
                partition2,
                binversion1,
                size2
        )

        segment3 = new DataSegment(
                tableName,
                interval2,
                version1,
                null,
                dimensions123,
                metrics123,
                partition1,
                binversion1,
                size1
        )

        segment4 = new DataSegment(
                tableName,
                interval2,
                version2,
                null,
                dimensions123,
                metrics123,
                partition2,
                binversion1,
                size2
        )

        segments = [segment1, segment2, segment3, segment4]

    }

    def shutdownSpec() {
        DateTimeZone.setDefault(currentTZ)
    }
}
