// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util

import com.yahoo.bard.webservice.config.BardFeatureFlag
import spock.lang.Specification

class DimensionStoreKeyUtilsSpec extends Specification {

    def "Test getRowKey() with CASE_SENSITIVE_KEYS_ENABLED off"() {
        expect:
        DimensionStoreKeyUtils.getRowKey("FOO","1") == "foo_1_row_key"
    }

    def "Test getColumnKey() with CASE_SENSITIVE_KEYS_ENABLED off"() {
        expect:
        DimensionStoreKeyUtils.getColumnKey("FOO") == "foo_column_key"
    }

    def "Test getRowKey() with CASE_SENSITIVE_KEYS_ENABLED on"() {
        setup:
        BardFeatureFlag.CASE_SENSITIVE_KEYS.setOn(true)

        expect:
        DimensionStoreKeyUtils.getRowKey("FOO","1") == "FOO_1_row_key"

        cleanup:
        BardFeatureFlag.CASE_SENSITIVE_KEYS.setOn(false)
    }

    def "Test getColumnKey() with CASE_SENSITIVE_KEYS_ENABLED on"() {
        setup:
        BardFeatureFlag.CASE_SENSITIVE_KEYS.setOn(true)

        expect:
        DimensionStoreKeyUtils.getColumnKey("FOO") == "FOO_column_key"

        cleanup:
        BardFeatureFlag.CASE_SENSITIVE_KEYS.setOn(false)
    }
}
