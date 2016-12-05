// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.provider.yaml.serde

import spock.lang.Specification

import static com.yahoo.bard.webservice.data.config.provider.yaml.serde.DeserializationHelper.deserialize

public class YamlDimensionFieldConfigDeserializerSpec extends Specification {
    def "Parsing should populate name"() {
        setup:
        def result = deserialize('{"field1": {"description": "some_desc"}}', new YamlDimensionFieldConfigDeserializer())
        expect:
        result.size() == 1
        result.containsKey("field1")
        result.get("field1").getName() == "field1"
    }
}
