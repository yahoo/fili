// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric

import com.yahoo.bard.webservice.util.GroovyTestUtils

import spock.lang.Specification

/**
 * Spec to check the different cases of MetricColumnWithValueType methods.
 */
class MetricColumnWithValueTypeSpec extends Specification {

    def "Get correct class for a given class name as string "() {
        setup:
        MetricColumnWithValueType metricColumn = new MetricColumnWithValueType(metricName, className)

        expect:
        GroovyTestUtils.compareObjects(metricColumn.classType, expected)

        where:
        metricName    | className               | expected
        "pageViews"   | "java.math.BigDecimal"  | BigDecimal.class
        "listPageView"| "java.util.List"        | List.class
        "nothing"     | null                    | null
    }
}
