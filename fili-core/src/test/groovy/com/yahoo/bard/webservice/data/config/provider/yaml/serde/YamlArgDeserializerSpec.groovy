// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.provider.yaml.serde

import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.yahoo.bard.webservice.druid.model.postaggregation.SketchSetOperationPostAggFunction
import spock.lang.Specification

public class YamlArgDeserializerSpec extends Specification {

    public static class ArgTest {
        @JsonDeserialize(using = YamlArgDeserializer.class)
        public Object[] args;
    }

    def "Parsing basic arguments should work correctly"() {
        setup:
        ObjectMapper mapper = new ObjectMapper();
        def resp = mapper.readValue('{"args": [1, 2, 3]}', ArgTest.class)

        expect:
        Arrays.equals(resp.args, [1,2,3] as Object[])
    }

    def "Parsing complex arguments should work correctly"() {
        setup:
        ObjectMapper mapper = new ObjectMapper();
        def resp = mapper.readValue('{"args": [1, "foo", 3]}', ArgTest.class)

        expect:
        Arrays.equals(resp.args, [1,"foo",3] as Object[])
    }

    def "Parsing arguments that instantiate classes should work correctly"() {
        setup:
        ObjectMapper mapper = new ObjectMapper();
        def resp = mapper.readValue('{"args": [1, "!!com.yahoo.bard.webservice.druid.model.postaggregation.SketchSetOperationPostAggFunction \'NOT\'", 3]}', ArgTest.class)

        expect:
        Arrays.equals(resp.args, [1, SketchSetOperationPostAggFunction.NOT, 3] as Object[])
    }

    def "Exception should be thrown on failure"() {
        setup:
        ObjectMapper mapper = new ObjectMapper();
        when:
        mapper.readValue('{"args": [1, "!!no.class.here", 3]}', ArgTest.class)

        then:
        JsonMappingException ex = thrown()
        ex.message =~ /Can't construct a java object.*no.class.here/
    }
}
