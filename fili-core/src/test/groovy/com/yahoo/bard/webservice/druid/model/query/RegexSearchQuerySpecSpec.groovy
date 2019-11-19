// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.query

import com.fasterxml.jackson.databind.ObjectMapper

import spock.lang.Specification
import spock.lang.Unroll

class RegexSearchQuerySpecSpec extends Specification {

    static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()

    @Unroll
    def "Spec with value '#value' serializes to '#expectedJson'"() {
        expect:
        OBJECT_MAPPER.writeValueAsString(new RegexSearchQuerySpec(pattern)) == expectedJson

        where:
        pattern || expectedJson
        "a*b"   || '{"type":"regex","pattern":"a*b"}'
        null    || '{"type":"regex"}'
    }
}
