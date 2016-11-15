// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.provider.yaml.serde

import spock.lang.Specification

import static com.yahoo.bard.webservice.data.config.provider.yaml.serde.DeserializationHelper.deserialize


public class YamlDimensionConfigDeserializerSpec extends Specification {

    def "Parsing should populate api name"() {
        setup:
        def result = deserialize('{"dim1": {"description": "some_desc"}}', new YamlDimensionConfigDeserializer())
        expect:
        result.size() == 1
        result.containsKey("dim1")
        result.get("dim1").getApiName() == "dim1"
    }
}
