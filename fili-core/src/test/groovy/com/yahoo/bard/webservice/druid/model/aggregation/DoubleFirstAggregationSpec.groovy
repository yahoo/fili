// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.aggregation

import spock.lang.Specification

class DoubleFirstAggregationSpec extends Specification {

    String name
    String fieldName

    def setup() {
        name = "foo"
        fieldName = "bar"
    }

    def "Constructor creation"() {
        when:
        DoubleFirstAggregation doubleFirstAggregation = new DoubleFirstAggregation(name, fieldName)

        then:
        doubleFirstAggregation.name == name
        doubleFirstAggregation.fieldName == fieldName
    }

    def "Double first aggregation creation with name"() {
        setup:
        DoubleFirstAggregation doubleFirstAggregation = new DoubleFirstAggregation(name, fieldName)
        String newName = "fooPage"

        when:
        DoubleFirstAggregation newDoubleFirstAggregation = doubleFirstAggregation.withName(newName)

        then:
        newDoubleFirstAggregation.name == newName
        newDoubleFirstAggregation.name == newName
    }
}
