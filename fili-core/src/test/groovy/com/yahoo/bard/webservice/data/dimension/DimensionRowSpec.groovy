// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension

import spock.lang.Specification

import java.util.function.BiFunction

class DimensionRowSpec extends Specification {

    String keyValue = "key"
    String nonKeyValue = "nonKey"
    String keyFieldFieldName = "keyFieldName"
    String nonKeyFieldName = "nonKeyFieldName"
    DimensionField keyField = Mock(DimensionField)
    DimensionField nonKeyField = Mock(DimensionField)
    DimensionField missingKeyField = Mock(DimensionField)

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

    def "missing key throws error"() {
        when:
        DimensionRow testRow = new DimensionRow(missingKeyField, [(keyField): keyValue, (nonKeyField): nonKeyValue])

        then:
        thrown(IllegalArgumentException)
    }

    def "copyWithReplace produces a modified row"() {
        BiFunction replacer = new BiFunction<DimensionField, String, String>() {
            @Override
            String apply(final DimensionField dimensionField, final String s) {
                return (dimensionField == keyField) ? nonKeyValue : s
            }
        }
        DimensionRow testRow = new DimensionRow(keyField, [(keyField): keyValue, (nonKeyField): nonKeyValue])
        DimensionRow expectedRow = new DimensionRow(keyField, [(keyField): nonKeyValue, (nonKeyField): nonKeyValue])

        when:
        DimensionRow result = DimensionRow.copyWithReplace(testRow, replacer)

        then:
        // Note, == does the wrong thing here
        result.equals(expectedRow)
        ! expectedRow.equals(testRow)
    }
}
