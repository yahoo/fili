// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.util

import com.yahoo.bard.webservice.druid.model.aggregation.SketchCountAggregation
import com.yahoo.bard.webservice.druid.model.aggregation.SketchMergeAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.FieldAccessorPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.SketchEstimatePostAggregation

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Tests that the converter converts things correctly.
 */
@Unroll
class SketchFieldConverterSpec extends Specification {
    @Shared
    FieldConverters converter = FieldConverterSupplier.sketchConverter

    def setup() {
        //Initializing the Sketch field converter
        FieldConverters  sc = FieldConverterSupplier.sketchConverter
        FieldConverterSupplier.sketchConverter = new SketchFieldConverter();
    }

    def cleanupSpec() {
        FieldConverterSupplier.sketchConverter = converter
    }

    def "asSketchCount converts an #aggregationClass into a SketchCountAggregation leaving all fields the same"() {
        given: "A SketchAggregation"
        def sketchAggregation = aggregation.newInstance(name, fieldName, size)

        when: "We convert it to a SketchCountAggregation using asOuterSketch"
        def convertedSketchAggregation = FieldConverterSupplier.sketchConverter.asOuterSketch(sketchAggregation)

        then: "We get back a SketchCountAggregation"
        convertedSketchAggregation instanceof SketchCountAggregation

        and: "the name, fieldName, and size are the same as the one we converted"
        convertedSketchAggregation.name == name
        convertedSketchAggregation.fieldName == fieldName
        convertedSketchAggregation.size == size

        where:
        aggregation            | name                     | fieldName              | size
        SketchMergeAggregation | "sampleMergeAggregation" | "sampleMergeFieldName" | 123
        SketchCountAggregation | "sampleCountAggregation" | "sampleCountFieldName" | 123
    }

    def "asSketchMerge converts an #aggregationClass into a SketchMergeAggregation leaving all fields the same"() {
        given: "A SketchAggregation"
        def sketchAggregation = aggregation.newInstance(name, fieldName, size)

        when: "We convert it to a SketchMergeAggregation using asInnerSketch"
        def convertedSketchAggregation = FieldConverterSupplier.sketchConverter.asInnerSketch(sketchAggregation)

        then: "We get back a SketchMergeAggregation"
        convertedSketchAggregation instanceof SketchMergeAggregation

        and: "the name, fieldName, and size are the same as the one we converted"
        convertedSketchAggregation.name == name
        convertedSketchAggregation.fieldName == fieldName
        convertedSketchAggregation.size == size

        where:
        aggregation            | name                     | fieldName              | size
        SketchMergeAggregation | "sampleMergeAggregation" | "sampleMergeFieldName" | 123
        SketchCountAggregation | "sampleCountAggregation" | "sampleCountFieldName" | 123
    }

    def "asSketchEstimate converts an #aggregation into a SketchEstimatePostAggregation with '_estimate' on the name"() {
        given: "A SketchAggregation"
        def sketchAggregation = aggregation.newInstance(name, fieldName, size)

        when: "We convert it to a SketchEstimatePostAggregation using asSketchEstimate"
        def convertedSketchAggregation = FieldConverterSupplier.sketchConverter.asSketchEstimate(sketchAggregation)

        then: "We get back a SketchEstimatePostAggregation"
        convertedSketchAggregation instanceof SketchEstimatePostAggregation

        and: "the name is the same as the one we converted, but with '_estimate' on the end"
        convertedSketchAggregation.name == name + "_estimate"

        and: "the field is a FieldAccessorPostAggregation accessing the aggregation we converted"
        convertedSketchAggregation.field instanceof FieldAccessorPostAggregation
        ((FieldAccessorPostAggregation) convertedSketchAggregation.field).aggregation == sketchAggregation

        where:
        aggregation            | name                     | fieldName              | size
        SketchMergeAggregation | "sampleMergeAggregation" | "sampleMergeFieldName" | 123
        SketchCountAggregation | "sampleCountAggregation" | "sampleCountFieldName" | 123
    }
}
