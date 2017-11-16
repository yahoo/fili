// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.metadata

import org.joda.time.Interval

import spock.lang.Unroll

class SegmentMetadataSpec extends BaseDataSourceMetadataSpec {

    private static final String intervalString1 = "2012-01-01T00:00:00.000Z/2013-12-27T00:00:00.000Z"
    private static final String intervalString2 = "2013-12-27T00:00:00.000Z/2013-12-28T00:00:00.000Z"
    private static final String intervalString3 = "2013-12-28T00:00:00.000Z/2015-03-17T00:00:00.000Z"
    private static final String intervalString13 = "2012-01-01T00:00:00.000Z/2015-03-17T00:00:00.000Z"
    private static final String intervalString23 = "2013-12-27T00:00:00.000Z/2015-03-17T00:00:00.000Z"

    @Override
    Map<String, Interval> generateIntervals() {
        [
                "interval1": Interval.parse(intervalString1),
                "interval3": Interval.parse(intervalString3),
                "interval13": Interval.parse(intervalString13),
                "interval23": Interval.parse(intervalString23)
        ]
    }

    @Override
    def childSetupSpec() {
        intervals = generateIntervals()
    }

    def dimensionSet1 = ["dim1"]
    def dimensionSet12 = ["dim1", "dim2"]
    def dimensionSet123 = ["dim1", "dim2", "dim3"]

    def metrics1 = ["metric1"]
    def metrics12 = ["metric1", "metric2"]

    def inputData1 = [
            (intervalString1): ["dimensions":dimensionSet12, "metrics":metrics1],
            (intervalString2): ["dimensions":dimensionSet1, "metrics":metrics12],
            (intervalString3): ["dimensions":dimensionSet123, "metrics": metrics12]
    ]

    def expectedDimensions1 = [
            "dim1": [intervals["interval13"]] as Set,
            "dim2": [intervals["interval1"], intervals["interval3"]] as Set,
            "dim3": [intervals["interval3"]] as Set
    ]

    def expectedMetrics = [
            "metric1": [intervals["interval13"]] as Set,
            "metric2": [intervals["interval23"]] as Set
    ]

    def "test construct healthy segment metadata"() {
        setup:
        SegmentMetadata metadata = new SegmentMetadata(inputData1)

        expect:
        metadata.dimensionIntervals == expectedDimensions1
        metadata.metricIntervals == expectedMetrics
        !metadata.empty
    }

    def "Test equality"() {
        setup:
        SegmentMetadata metadata1 = new SegmentMetadata(inputData1)
        SegmentMetadata expected = new SegmentMetadata(expectedDimensions1, expectedMetrics)

        expect:
        metadata1 == expected
    }

    @Unroll
    def "#emptyDims dimensions, #emptyTime time, and #emptyMetrics metrics means the SegmentMetadata is #isEmpty"() {
        given: "An input object with assorted emptiness aspects"
        def input = intervalSet.collectEntries {
            [(it): ["dimensions": dimensionSet, "metrics": metricSet]]
        }

        and: "A SegmentMetadata object constructed from it"
        SegmentMetadata metadata = new SegmentMetadata(input)

        expect: "The emptiness is correctly detected"
        metadata.empty == emptyState

        where:
        emptyState | dimensionSet | metricSet  | intervalSet
        false      | ["dim"]      | ["metric"] | ["2012-01-01T00:00:00.000Z/2013-12-27T00:00:00.000Z"]
        false      | []           | ["metric"] | ["2012-01-01T00:00:00.000Z/2013-12-27T00:00:00.000Z"]
        false      | ["dim"]      | []         | ["2012-01-01T00:00:00.000Z/2013-12-27T00:00:00.000Z"]
        true       | []           | []         | ["2012-01-01T00:00:00.000Z/2013-12-27T00:00:00.000Z"]
        true       | ["dim"]      | ["metric"] | []

        emptyDims = dimensionSet ? "Full" : "Empty"
        emptyMetrics = metricSet ? "full" : "empty"
        emptyTime = intervalSet ? "full" : "empty"
        isEmpty = emptyState ? "empty" : "not empty"
    }
}
