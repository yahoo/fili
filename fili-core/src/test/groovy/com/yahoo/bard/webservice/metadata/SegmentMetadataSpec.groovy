// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.metadata

import org.joda.time.Interval

import spock.lang.Specification
import spock.lang.Unroll

class SegmentMetadataSpec extends Specification {
    String intervalString1 = "2012-01-01T00:00:00.000Z/2013-12-27T00:00:00.000Z"
    String intervalString2 = "2013-12-27T00:00:00.000Z/2013-12-28T00:00:00.000Z"
    String intervalString3 = "2013-12-28T00:00:00.000Z/2015-03-17T00:00:00.000Z"
    String intervalString13 = "2012-01-01T00:00:00.000Z/2015-03-17T00:00:00.000Z"
    String intervalString23 = "2013-12-27T00:00:00.000Z/2015-03-17T00:00:00.000Z"

    Interval interval1 = Interval.parse(intervalString1)
    Interval interval3 = Interval.parse(intervalString3)
    Interval interval13 = Interval.parse(intervalString13)
    Interval interval23 = Interval.parse(intervalString23)

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
        "dim1": [interval13,] as Set,
        "dim2": [interval1, interval3] as Set,
        "dim3": [interval3] as Set
    ]

    def expectedMetrics = [
        "metric1": [interval13] as Set,
        "metric2": [interval23] as Set
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
        def input = intervals.collectEntries {
            [(it): ["dimensions": dimensions, "metrics": metrics]]
        }

        and: "A SegmentMetadata object constructed from it"
        SegmentMetadata metadata = new SegmentMetadata(input)

        expect: "The emptiness is correctly detected"
        metadata.empty == emptyState

        where:
        emptyState | dimensions | metrics    | intervals
        false      | ["dim"]    | ["metric"] | ["2012-01-01T00:00:00.000Z/2013-12-27T00:00:00.000Z"]
        false      | []         | ["metric"] | ["2012-01-01T00:00:00.000Z/2013-12-27T00:00:00.000Z"]
        false      | ["dim"]    | []         | ["2012-01-01T00:00:00.000Z/2013-12-27T00:00:00.000Z"]
        true       | []         | []         | ["2012-01-01T00:00:00.000Z/2013-12-27T00:00:00.000Z"]
        true       | ["dim"]    | ["metric"] | []

        emptyDims = dimensions ? "Full" : "Empty"
        emptyMetrics = metrics ? "full" : "empty"
        emptyTime = intervals ? "full" : "empty"
        isEmpty = emptyState ? "empty" : "not empty"
    }
}
