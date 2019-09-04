// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.metadata

import com.yahoo.bard.webservice.data.config.names.TestApiDimensionName
import com.yahoo.bard.webservice.data.config.names.TestApiMetricName
import com.yahoo.bard.webservice.data.config.names.TestDruidTableName
import com.yahoo.bard.webservice.druid.model.metadata.NumberedShardSpec

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

    /**
     * A map of interval name(String) to an Interval object.
     * <p>
     * With a map, you can use {@code intervals.interval1} rather than {@code intervals[0]}. This allows you writing
     * specs to give your intervals meaningful names and then use them.
     */
    @Shared
    Map<String, Interval> intervals

    /**
     * A map of dimension name(String) to a TestApiDimensionName object.
     * <p>
     * With a map, you can use {@code dimensions.dim3.asName()} rather than {@code dimensions[2].asName()}. This allows
     * you writing specs to give your dimensions meaningful names and then use them.
     */
    @Shared
    Map<String, TestApiDimensionName> dimensions

    /**
     * A map of metric name(String) to a TestApiMetricName object.
     * <p>
     * With a map, you can use {@code metrics.metric3.asName()} rather than {@code metrics[2].asName()}. This allows you
     * writing specs to give your metrics meaningful names and then use them.
     */
    @Shared
    Map<String, TestApiMetricName> metrics

    /**
     * A map of segment name(String) to a DataSegment object.
     * <p>
     * With a map, you can use {@code segments.identifier3} rather than {@code segments[2]}. This allows you writing
     * specs to give your segments meaningful names and then use them.
     */
    @Shared
    Map<String, DataSegment> segments

    /**
     * A map of version name to a version object.
     * <p>
     * With a map, you can use {@code versions.version3} rather than {@code version[2]}. This allows you writing specs
     * to give your versions meaningful names and then use them.
     */
    @Shared
    Map<String, String> versions

    /**
     * A map of size identifier to the actual size.
     * <p>
     * With a map, you can use {@code sizes.identifier3} rather than {@code sizes[2]}. This allows you writing specs
     * to give your sizes meaningful names and them use them.
     */
    @Shared
    Map<String, Long> sizes

    /**
     * A map of binary version name to its actual binary version.
     * <p>
     * A binary version is used as a required argument to construct a DataSegment.
     * <p>
     * With a map, you can use {@code binaryVersions.version3} rather than {@code binaryVersions[2]}. This allows you
     * writing specs to give your sizes meaningful names and then use them.
     */
    @Shared
    Map<String, Integer> binaryVersions

    def setupSpec() {
        currentTZ = DateTimeZone.getDefault()
        DateTimeZone.setDefault(DateTimeZone.UTC)

        childSetupSpec()
    }

    def childSetupSpec() {}

    def cleanupSpec() {
        DateTimeZone.setDefault(currentTZ)
    }

    def childCleanupSpec() {}

    String generateTableName() {
        return TestDruidTableName.ALL_PETS.asName()
    }

    /**
     * Returns a map of interval name(String) to an Interval object.
     *
     * @return a map of interval name to an Interval object.
     */
    Map<String, Interval> generateIntervals() {
        return [
                interval1: Interval.parse("2015-01-01T00:00:00.000Z/2015-01-02T00:00:00.000Z"),
                interval2: Interval.parse("2015-01-02T00:00:00.000Z/2015-01-03T00:00:00.000Z"),
                interval12: Interval.parse("2015-01-01T00:00:00.000Z/2015-01-03T00:00:00.000Z")
        ]
    }

    /**
     * Returns a map of dimension name(String) to a TestApiDimensionName object.
     *
     * @return a map of dimension name to a TestApiDimensionName object
     */
    Map<String, TestApiDimensionName> generateDimensions() {
        return [
                (TestApiDimensionName.BREED.asName()): TestApiDimensionName.BREED,
                (TestApiDimensionName.SPECIES.asName()): TestApiDimensionName.SPECIES,
                (TestApiDimensionName.SEX.asName()): TestApiDimensionName.SEX
        ]
    }

    /**
     * Returns a map of metric name(String) to a TestApiMetricName object.
     *
     * @return a map of metric name to a TestApiMetricName object
     */
    Map<String, TestApiMetricName> generateMetrics() {
        return [
                (TestApiMetricName.A_ROW_NUM.asName()): TestApiMetricName.A_ROW_NUM,
                (TestApiMetricName.A_LIMBS.asName()): TestApiMetricName.A_LIMBS,
                (TestApiMetricName.A_DAY_AVG_LIMBS.asName()): TestApiMetricName.A_DAY_AVG_LIMBS
        ]
    }

    /**
     * Returns a map of version name to a version object.
     *
     * @return map of version name to a version object
     */
    Map<String, String> generateVersions() {
        return [
                version1: DateTimeFormat.fullDateTime().print(DateTime.now().minusDays(1)),
                version2: DateTimeFormat.fullDateTime().print(DateTime.now())
        ]
    }

    /**
     * Returns a map of size identifier to its actual size.
     *
     * @return a map of size identifier to its actual size
     */
    Map<String, Long> generateSizes() {
        return [
                size1: 1024,
                size2: 512
        ]
    }

    /**
     * Returns a map of binary version name to its actual version.
     *
     * @return a map of binary version name to its actual version
     */
    Map<String, Integer> generateBinaryVersions() {
        return [binaryVersion1: 9]
    }

    /**
     * Returns a map of segment name(String) to a DataSegment object.
     * <p>
     * This method also generates {@code tableName}, {@code intervals}, {@code dimensions}, {@code metrics},
     * {@code versions}, {@code sizes}, and {@code binaryVersions}
     *
     * @return a map of segment name to a DataSegment object
     */
    Map<String, DataSegment> generateSegments() {
        tableName = generateTableName()
        intervals = generateIntervals()
        dimensions = generateDimensions()
        metrics = generateMetrics()
        versions = generateVersions()
        sizes = generateSizes()
        binaryVersions = generateBinaryVersions()

        NumberedShardSpec partition1 = Stub(NumberedShardSpec) {
            getPartitionNum() >> 0
        }

        NumberedShardSpec partition2 = Stub(NumberedShardSpec) {
            getPartitionNum() >> 1
        }

        return [
                segment1: new DataSegment(
                        tableName,
                        intervals.interval1,
                        versions.version1,
                        null,
                        dimensions.keySet() as List,
                        metrics.keySet() as List,
                        partition1,
                        binaryVersions.binaryVersion1,
                        sizes.size1
                ),
                segment2: new DataSegment(
                        tableName,
                        intervals.interval1,
                        versions.version2,
                        null,
                        dimensions.keySet() as List,
                        metrics.keySet() as List,
                        partition2,
                        binaryVersions.binaryVersion1,
                        sizes.size2
                ),
                segment3: new DataSegment(
                        tableName,
                        intervals.interval2,
                        versions.version1,
                        null,
                        dimensions.keySet() as List,
                        metrics.keySet() as List,
                        partition1,
                        binaryVersions.binaryVersion1,
                        sizes.size1
                ),
                segment4: new DataSegment(
                        tableName,
                        intervals.interval2,
                        versions.version2,
                        null,
                        dimensions.keySet() as List,
                        metrics.keySet() as List,
                        partition2,
                        binaryVersions.binaryVersion1,
                        sizes.size2
                )
        ]
    }
}
