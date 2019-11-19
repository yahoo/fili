// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.query

import com.fasterxml.jackson.databind.ObjectMapper

import spock.lang.Specification
import spock.lang.Unroll

class InsensitiveContainsSearchQuerySpecSpec extends Specification {

    static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()

    @Unroll
    def "Spec with value '#value' serializes to '#expectedJson'"() {
        expect:
        OBJECT_MAPPER.writeValueAsString(new InsensitiveContainsSearchQuerySpec(value)) == expectedJson

        where:
        value        || expectedJson
        "some_value" || '{"type":"insensitive_contains","value":"some_value"}'
        null         || '{"type":"insensitive_contains"}'
    }
}
