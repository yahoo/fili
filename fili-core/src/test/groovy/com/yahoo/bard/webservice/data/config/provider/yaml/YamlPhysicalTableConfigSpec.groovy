// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.provider.yaml

import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig
import com.yahoo.bard.webservice.data.config.provider.ConfigurationDictionary
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import spock.lang.Specification

public class YamlPhysicalTableConfigSpec extends Specification {

    def "Basic constructor works"() {
        setup:
        def t =  new YamlPhysicalTableConfig(DefaultTimeGrain.HOUR, ["dim1"] as String[], ["m1"] as String[])
        t.setTableName("table")

        expect:
        t.getMetrics().collect({a -> a.asName()}) == ["m1"]
        // not checking result, but shouldn't fail
        t.buildPhysicalTable(new ConfigurationDictionary<DimensionConfig>())
    }

    def "TimeGrain required"() {
        when:
        new YamlPhysicalTableConfig(null, ["dim1"] as String[], ["m1"] as String[])
        then:
        RuntimeException ex = thrown()
        ex.message =~ /ZonelessTimeGrain required.*/
    }

    def "Dimensions required"() {
        when:
        new YamlPhysicalTableConfig(DefaultTimeGrain.HOUR, null, ["m1"] as String[])
        then:
        RuntimeException ex = thrown()
        ex.message =~ /.*with dimensions.*/
    }

    def "Metrics required"() {
        when:
        new YamlPhysicalTableConfig(DefaultTimeGrain.HOUR, ["dim1"] as String[], null)
        then:
        RuntimeException ex = thrown()
        ex.message =~ /.*with metrics.*/
    }
}
