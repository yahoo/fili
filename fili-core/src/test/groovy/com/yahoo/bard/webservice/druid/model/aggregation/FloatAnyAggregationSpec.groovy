// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.aggregation

import spock.lang.Specification

class FloatAnyAggregationSpec extends Specification {

    String name
    String fieldName
    String type

    def setup() {
        name = "foo"
        fieldName = "bar"
        type = "floatAny"
    }

    def "Constructor creation"() {
        when:
        FloatAnyAggregation floatAnyAggregation = new FloatAnyAggregation(name, fieldName)

        then:
        floatAnyAggregation.name == name
        floatAnyAggregation.fieldName == fieldName
        floatAnyAggregation.getType() == type
    }

    def "Float Any aggregation creation with name"() {
        setup:
        FloatAnyAggregation floatAnyAggregation = new FloatAnyAggregation(name, fieldName)
        String newName = "fooPage"

        when:
        FloatAnyAggregation newFloatAnyAggregation = floatAnyAggregation.withName(newName)

        then:
        newFloatAnyAggregation.name == newName
        newFloatAnyAggregation.fieldName == fieldName
        newFloatAnyAggregation.getType() == type
    }

    def "Float Any aggregation creation with field name"() {
        setup:
        FloatAnyAggregation floatAnyAggregation = new FloatAnyAggregation(name, fieldName)
        String newFieldName = "barPage"

        when:
        FloatAnyAggregation newFloatAnyAggregation = floatAnyAggregation.withFieldName(newFieldName)

        then:
        newFloatAnyAggregation.name == name
        newFloatAnyAggregation.fieldName == newFieldName
        newFloatAnyAggregation.getType() == type
    }
}
