// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.aggregation

import spock.lang.Specification

class LongFirstAggregationSpec extends Specification {

    String name
    String fieldName
    String type

    def setup() {
        name = "foo"
        fieldName = "bar"
        type = "longFirst"
    }

    def "Constructor creation"() {
        when:
        LongFirstAggregation longFirstAggregation = new LongFirstAggregation(name, fieldName)

        then:
        longFirstAggregation.name == name
        longFirstAggregation.fieldName == fieldName
        longFirstAggregation.getType() == type
    }

    def "Long first aggregation creation with name"() {
        setup:
        LongFirstAggregation longFirstAggregation = new LongFirstAggregation(name, fieldName)
        String newName = "fooPage"

        when:
        LongFirstAggregation newLongFirstAggregation = longFirstAggregation.withName(newName)

        then:
        newLongFirstAggregation.name == newName
        newLongFirstAggregation.fieldName == fieldName
        newLongFirstAggregation.getType() == type
    }

    def "Long first aggregation creation with field name"() {
        setup:
        LongFirstAggregation longFirstAggregation = new LongFirstAggregation(name, fieldName)
        String newFieldName = "barPage"

        when:
        LongFirstAggregation newLongFirstAggregation = longFirstAggregation.withFieldName(newFieldName)

        then:
        newLongFirstAggregation.name == name
        newLongFirstAggregation.fieldName == newFieldName
        newLongFirstAggregation.getType() == type
    }
}
