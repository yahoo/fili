// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension

import com.fasterxml.jackson.databind.ObjectMapper

import spock.lang.Specification

class DimensionRowSpec extends Specification {

    String keyValue = "key"
    String nonKeyValue = "nonKey"
    String keyFieldFieldName = "keyFieldName"
    String nonKeyFieldName = "nonKeyFieldName"
    DimensionField keyField = Mock(DimensionField)
    DimensionField nonKeyField = Mock(DimensionField)
    DimensionRow testRow

    def setup() {
        keyField.name >> keyFieldFieldName
        nonKeyField.name >> nonKeyFieldName
        testRow = new DimensionRow(keyField, [(keyField): keyValue, (nonKeyField): nonKeyValue])
    }

    def "healthy initialization returns correct state"() {
        expect:
        testRow.getKeyValue() == keyValue
        testRow.getRowMap() == [(nonKeyFieldName): nonKeyValue, (keyFieldFieldName): keyValue]
    }

    def "json serialization into the correct format we expect"() {
        setup:
        ObjectMapper objectMapper = new ObjectMapper()
        String result = objectMapper.writeValueAsString(testRow)

        expect:
        result.contains('"nonKeyFieldName":"nonKey"')
        result.contains('"keyFieldName":"key"')
    }
}
