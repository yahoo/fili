// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.aggregation

import spock.lang.Specification

class LongLastAggregationSpec extends Specification {

    String name
    String fieldName
    String type

    def setup() {
        name = "foo"
        fieldName = "bar"
        type = "longLast"
    }

    def "Constructor creation"() {
        when:
        LongLastAggregation longLastAggregation = new LongLastAggregation(name, fieldName)

        then:
        longLastAggregation.name == name
        longLastAggregation.fieldName == fieldName
        longLastAggregation.getType() == type
    }

    def "Long Last aggregation creation with name"() {
        setup:
        LongLastAggregation longLastAggregation = new LongLastAggregation(name, fieldName)
        String newName = "fooPage"

        when:
        LongLastAggregation newLongLastAggregation = longLastAggregation.withName(newName)

        then:
        newLongLastAggregation.name == newName
        newLongLastAggregation.fieldName == fieldName
        newLongLastAggregation.getType() == type
    }

    def "Long Last aggregation creation with field name"() {
        setup:
        LongLastAggregation longLastAggregation = new LongLastAggregation(name, fieldName)
        String newFieldName = "barPage"

        when:
        LongLastAggregation newLongLastAggregation = longLastAggregation.withFieldName(newFieldName)

        then:
        newLongLastAggregation.name == name
        newLongLastAggregation.fieldName == newFieldName
        newLongLastAggregation.getType() == type
    }
}
