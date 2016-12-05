// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.provider.yaml.serde

import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig
import com.yahoo.bard.webservice.data.config.provider.ConfigurationDictionary
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain
import org.joda.time.DateTimeZone
import spock.lang.Specification

import static com.yahoo.bard.webservice.data.config.provider.yaml.serde.DeserializationHelper.deserialize

public class YamlPhysicalTableConfigDeserializerSpec extends Specification {

    def "Parsing should populate table name"() {
        setup:
        def result = deserialize('{"table1": {"dimensions": ["dim1"], "metrics": ["m1"], "granularity": "MINUTE"}}', new YamlPhysicalTableConfigDeserializer())
        def physicalTable = result.get("table1").buildPhysicalTable(new ConfigurationDictionary<DimensionConfig>())

        expect:
        result.size() == 1
        physicalTable.getName().asName() == "table1"
        physicalTable.getGrain() == new ZonedTimeGrain(DefaultTimeGrain.MINUTE, DateTimeZone.UTC)
    }
}
