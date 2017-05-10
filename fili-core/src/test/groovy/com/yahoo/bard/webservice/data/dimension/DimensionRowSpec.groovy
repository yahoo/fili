// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension

import spock.lang.Specification

class DimensionRowSpec extends Specification {

    String keyValue = "key"
    String nonKeyValue = "nonKey"
    String keyFieldFieldName = "keyFieldName"
    String nonKeyFieldName = "nonKeyFieldName"
    DimensionField keyField = Mock(DimensionField)
    DimensionField nonKeyField = Mock(DimensionField)

    def setup() {
        keyField.name >> keyFieldFieldName
        nonKeyField.name >> nonKeyFieldName
    }

    def "healthy initialization returns correct state"() {
        setup:
        DimensionRow testRow = new DimensionRow(keyField, [(keyField): keyValue, (nonKeyField): nonKeyValue])

        expect:
        testRow.getKeyValue() == keyValue
        testRow.getRowMap() == [(nonKeyFieldName): nonKeyValue, (keyFieldFieldName): keyValue]
    }
}
