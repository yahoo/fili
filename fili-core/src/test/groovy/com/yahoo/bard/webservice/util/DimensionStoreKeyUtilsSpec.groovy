// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util

import com.yahoo.bard.webservice.config.BardFeatureFlag
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class DimensionStoreKeyUtilsSpec extends Specification {

    @Shared boolean originalCaseSensitiveKeysEnabled

    def setupSpec() {
        originalCaseSensitiveKeysEnabled = BardFeatureFlag.CASE_SENSITIVE_KEYS.isOn()
    }

    def cleanupSpec() {
        BardFeatureFlag.CASE_SENSITIVE_KEYS.setOn(originalCaseSensitiveKeysEnabled)
    }

    @Unroll
    def "getRowKey #preserves case for #param when CASE_SENSITIVE_KEYS_ENABLED is #flagState"() {
        boolean originalFlagState

        setup:
        originalFlagState = BardFeatureFlag.CASE_SENSITIVE_KEYS.isOn()
        BardFeatureFlag.CASE_SENSITIVE_KEYS.setOn(flagState)

        expect:
        DimensionStoreKeyUtils.getRowKey(rowName, rowValue) == expectedValue

        cleanup:
        BardFeatureFlag.CASE_SENSITIVE_KEYS.setOn(originalFlagState)

        where:
        preserves           | flagState | param      | rowName | rowValue | expectedValue
        "does not preserve" | false     | "rowValue" | "foo"   | "FOO"    | "foo_foo_row_key"
        "does not preserve" | false     | "rowValue" | "foo"   | "1"      | "foo_1_row_key"
        "preserve"          | true      | "rowValue" | "foo"   | "FOO"    | "foo_FOO_row_key"
        "does not preserve" | false     | "rowName"  | "FOO"   | "foo"    | "foo_foo_row_key"
        "preserve"          | true      | "rowName"  | "FOO"   | "foo"    | "FOO_foo_row_key"
    }

    @Unroll
    def "getColumnKey #respects case for columnName when CASE_SENSITIVE_KEYS_ENABLED is #flagState"() {
        boolean originalFlagState

        setup:
        originalFlagState = BardFeatureFlag.CASE_SENSITIVE_KEYS.isOn()
        BardFeatureFlag.CASE_SENSITIVE_KEYS.setOn(flagState)

        expect:
        DimensionStoreKeyUtils.getColumnKey(columnName) == expectedValue

        cleanup:
        BardFeatureFlag.CASE_SENSITIVE_KEYS.setOn(originalFlagState)

        where:
        preserves           | flagState | columnName | expectedValue
        "does not preserve" | false     | "FOO"      | "foo_column_key"
        "preserve"          | true      | "FOO"      | "FOO_column_key"
    }
}
