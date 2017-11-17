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

/**
 * Base specification of all metadata tests.
 * <p>
 * Any metadata Specs should extend this Spec, override childSetupSpec(), and call/override generate*() methods to
 * initiate metadata related resources.
 * <p>
 * Take "intervals" as an example
 * <ul>
 *     <li>
 *         you don't need any of the resources(including "intervals") defined in this Spec, you do nothing
 *     </li>
 *     <li>
 *         you need "intervals" in your test, you override childSetupSpec() and call its generation method in your Spec
 *         as follows
 *         <pre>
 *             {@code
 *             @Override
 *             def childSetupSpec() {
 *                 intervals = generateIntervals()
 *             }
 *             }
 *         </pre>
 *     </li>
 *     <li>
 *         you need "intervals" in your test, but you need a different set of intervals:
 *         <pre>
 *             {@code
 *             @Override
 *             def childSetupSpec() {
 *                 intervals = generateIntervals()
 *             }
 *
 *             @Override
 *             Map<String, Interval> generateIntervals() {
 *                 [
 *                     "interval1": Interval.parse("2015-01-01T00:00:00.000Z/2015-01-02T00:00:00.000Z"),
 *                     "interval2": Interval.parse("2015-01-02T00:00:00.000Z/2015-01-03T00:00:00.000Z"),
 *                     "interval3": Interval.parse("2015-01-03T00:00:00.000Z/2015-01-04T00:00:00.000Z"),
 *                     "interval123": Interval.parse("2015-01-01T00:00:00.000Z/2015-01-04T00:00:00.000Z")
 *                 ]
 *             }
 *             }
 *         </pre>
 *     </li>
 * </ul>
 */
abstract class BaseDataSourceMetadataSpec extends Specification {
    @Shared
    DateTimeZone currentTZ

    @Shared
    String tableName
    @Shared
    Map<String, Interval> intervals
    @Shared
    RangeSet<DateTime> rangeSet
    @Shared
    List<TestApiDimensionName> dimensions
    @Shared
    List<TestApiMetricName> metrics
    @Shared
    Integer binversion1
    @Shared
    long size1
    @Shared
    long size2
    @Shared
    List<String> versions
    @Shared
    List<DataSegment> segments

    def setupSpec() {
        currentTZ = DateTimeZone.getDefault()
        DateTimeZone.setDefault(DateTimeZone.UTC)
        binversion1 = 9
        size1 = 1024
        size2 = 512

        childSetupSpec()
    }

    def childSetupSpec() {}

    def shutdownSpec() {
        DateTimeZone.setDefault(currentTZ)
    }

    String generateTableName() {
        TestDruidTableName.ALL_PETS.asName()
    }

    Map<String, Interval> generateIntervals() {
        [
                "interval1": Interval.parse("2015-01-01T00:00:00.000Z/2015-01-02T00:00:00.000Z"),
                "interval2": Interval.parse("2015-01-02T00:00:00.000Z/2015-01-03T00:00:00.000Z"),
                "interval12": Interval.parse("2015-01-01T00:00:00.000Z/2015-01-03T00:00:00.000Z")
        ]
    }

    RangeSet<DateTime> generateRangeSet() {
        rangeSet = TreeRangeSet.create()
        rangeSet.add(Range.closedOpen(intervals["interval12"].getStart(), intervals["interval12"].getEnd()))
        rangeSet
    }

    List<TestApiDimensionName> generateDimensions() {
        [TestApiDimensionName.BREED, TestApiDimensionName.SPECIES, TestApiDimensionName.SEX]
    }

    List<TestApiMetricName> generateMetrics() {
        [TestApiMetricName.A_ROW_NUM, TestApiMetricName.A_LIMBS, TestApiMetricName.A_DAY_AVG_LIMBS]
    }

    List<String> generateVersions() {
        [
                DateTimeFormat.fullDateTime().print(DateTime.now().minusDays(1)),
                DateTimeFormat.fullDateTime().print(DateTime.now())
        ]
    }

    List<DataSegment> generateSegments() {
        versions = generateVersions()

        NumberedShardSpec partition1 = Stub(NumberedShardSpec) {
            getPartitionNum() >> 0
        }

        NumberedShardSpec partition2 = Stub(NumberedShardSpec) {
            getPartitionNum() >> 1
        }

        [
                new DataSegment(
                        tableName,
                        intervals["interval1"],
                        versions[0],
                        null,
                        dimensions*.asName(),
                        metrics*.asName(),
                        partition1,
                        binversion1,
                        size1
                ),
                new DataSegment(
                        tableName,
                        intervals["interval1"],
                        versions[1],
                        null,
                        dimensions*.asName(),
                        metrics*.asName(),
                        partition2,
                        binversion1,
                        size2
                ),
                new DataSegment(
                        tableName,
                        intervals["interval2"],
                        versions[0],
                        null,
                        dimensions*.asName(),
                        metrics*.asName(),
                        partition1,
                        binversion1,
                        size1
                ),
                new DataSegment(
                        tableName,
                        intervals["interval2"],
                        versions[1],
                        null,
                        dimensions*.asName(),
                        metrics*.asName(),
                        partition2,
                        binversion1,
                        size2
                )
        ]
    }
}
