// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.aggregation

import org.apache.commons.lang3.tuple.Pair

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class AggregationSpec extends Specification {

    @Shared List<Class<? extends Aggregation>> baseAggregations

    def setupSpec() {
        baseAggregations = [DoubleSumAggregation, DoubleMaxAggregation, DoubleMinAggregation,
                            LongSumAggregation, LongMaxAggregation, LongMinAggregation,
                            MaxAggregation, MinAggregation]
    }

    @Unroll
    def "verify nesting with basic metric #baseAggregation.simpleName"() {
        setup:
        Class testClass = baseAggregation
        Aggregation original = testClass.newInstance(["name", "fieldName"].toArray())
        Aggregation outer = testClass.newInstance(["name", "name"].toArray())
        Aggregation inner = testClass.newInstance(["name", "fieldName"].toArray())

        expect:
        original.nest().equals(Pair.of(outer, inner))

        where:
        baseAggregation << baseAggregations
    }

    @Unroll
    def "Verify default metric #baseAggregation.simpleName has no dimension dependencies"() {
        setup:
        Class testClass = baseAggregation
        Aggregation aggregation = testClass.newInstance(["name", "fieldName"].toArray())

        expect:
        aggregation.dependentDimensions.empty

        where:
        baseAggregation << baseAggregations
    }
}
