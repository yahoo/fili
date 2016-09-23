// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.aggregation

import org.apache.commons.lang3.tuple.Pair

import spock.lang.Specification
import spock.lang.Unroll

class SketchAggregationSpec extends Specification {

    @Unroll
    def "verify nesting with basic sketch metrics base: #originalSketch.simpleName outer: #outer.simpleName inner: #inner.simpleName"() {
        setup:
        Aggregation original = originalSketch.newInstance(["name", "fieldName", 1024].toArray())
        Aggregation outer = outerSketch.newInstance(["name", "name", 1024].toArray())
        Aggregation inner = innerSketch.newInstance(["name", "fieldName", 1024].toArray())

        expect:
        original.nest().equals(Pair.of(outer, inner))

        where:
        originalSketch         | outerSketch            | innerSketch
        SketchCountAggregation | SketchCountAggregation | SketchMergeAggregation
        SketchMergeAggregation | SketchMergeAggregation | SketchMergeAggregation
        ThetaSketchAggregation | ThetaSketchAggregation | ThetaSketchAggregation
    }

    @Unroll
    def "Verify default metric #baseAggregation.simpleName has no dimension dependencies"() {
        setup:
        Aggregation aggregation = baseAggregation.newInstance(["name", "fieldName", 1024].toArray())

        expect:
        aggregation.dependentDimensions.empty

        where:
        baseAggregation << [SketchCountAggregation, SketchMergeAggregation, ThetaSketchAggregation]
    }
}
