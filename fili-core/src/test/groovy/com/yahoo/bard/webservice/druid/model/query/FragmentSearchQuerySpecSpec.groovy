// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.query

import com.fasterxml.jackson.databind.ObjectMapper

import spock.lang.Specification
import spock.lang.Unroll

class FragmentSearchQuerySpecSpec extends Specification {

    static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()

    @Unroll
    def "#caseSensitive spec on '#values' values serializes to '#expectedJson'"() {
        expect:
        OBJECT_MAPPER.writeValueAsString(new FragmentSearchQuerySpec(isCaseSensitive, values)) == expectedJson

        where:
        isCaseSensitive | values                     || expectedJson
        true            | ["fragment1", "fragment2"] || '{"type":"fragment","case_sensitive":true,"values":["fragment1","fragment2"]}'
        false           | ["fragment1", "fragment2"] || '{"type":"fragment","case_sensitive":false,"values":["fragment1","fragment2"]}'
        true            | null                       || '{"type":"fragment","case_sensitive":true}'
        false           | null                       || '{"type":"fragment","case_sensitive":false}'
        null            | ["fragment1", "fragment2"] || '{"type":"fragment","values":["fragment1","fragment2"]}'
        null            | null                       || '{"type":"fragment"}'

        caseSensitive = isCaseSensitive == null ? "Default" : (isCaseSensitive ? "Case sensitive" : "Case insensitive")
    }
}
