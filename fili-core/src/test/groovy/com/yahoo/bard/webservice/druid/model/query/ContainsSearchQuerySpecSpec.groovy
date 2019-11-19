// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.query

import com.fasterxml.jackson.databind.ObjectMapper

import spock.lang.Specification
import spock.lang.Unroll

class ContainsSearchQuerySpecSpec extends Specification {

    static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()

    @Unroll
    def "#caseSensitive spec on '#values' values serializes to '#expectedJson'"() {
        expect:
        OBJECT_MAPPER.writeValueAsString(new ContainsSearchQuerySpec(isCaseSensitive, value)) == expectedJson

        where:
        isCaseSensitive | value        || expectedJson
        true            | "some_value" || '{"type":"contains","case_sensitive":true,"value":"some_value"}'
        false           | "some_value" || '{"type":"contains","case_sensitive":false,"value":"some_value"}'
        true            | null         || '{"type":"contains","case_sensitive":true}'
        false           | null         || '{"type":"contains","case_sensitive":false}'
        null            | "some_value" || '{"type":"contains","value":"some_value"}'
        null            | null         || '{"type":"contains"}'

        caseSensitive = isCaseSensitive == null ? "Default" : (isCaseSensitive ? "Case sensitive" : "Case insensitive")
    }
}
