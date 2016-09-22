// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.postaggregation

import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation
import com.yahoo.bard.webservice.druid.model.aggregation.LongSumAggregation
import com.yahoo.bard.webservice.util.GroovyTestUtils

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module

import spock.lang.Specification

class SketchSetOperationPostAggregationSpec extends Specification {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new Jdk8Module().configureAbsentsAsNulls(false))

    Aggregation agg1
    Aggregation agg2
    FieldAccessorPostAggregation field1
    FieldAccessorPostAggregation field2


    def setup() {
        agg1 = new LongSumAggregation("bnoy", "foo")
        agg2 = new LongSumAggregation("bwdy", "goo")
        field1 = new FieldAccessorPostAggregation(agg1)
        field2 = new FieldAccessorPostAggregation(agg2)
    }

    def "Check json serialization when size is not passed"() {
        setup:

        SketchSetOperationPostAggregation postAggregation = new SketchSetOperationPostAggregation(
                "bunreg",
                SketchSetOperationPostAggFunction.NOT,
                [field1, field2]
        )

        // Expected json
        String postAggDruidExp = ("""{
                                            "type":"sketchSetOper",
                                            "name": "bunreg",
                                            "func": "NOT",
                                            "fields": [
                                                        {
                                                            "type": "fieldAccess",
                                                            "fieldName": "bnoy"
                                                        },
                                                        {
                                                            "type": "fieldAccess",
                                                            "fieldName": "bwdy"
                                                        }
                                                      ]
                                        }""").replaceAll(/\s/, "")

        // serialized druid post agg
        String druidPostAggQuery1 = MAPPER.writeValueAsString(postAggregation)
        expect:
        GroovyTestUtils.compareJson(druidPostAggQuery1, postAggDruidExp)
    }

    def "Check json serialization when size is passed"() {
        setup:

        SketchSetOperationPostAggregation postAggregation = new SketchSetOperationPostAggregation(
                "bunreg",
                SketchSetOperationPostAggFunction.NOT,
                [field1, field2],
                1234
        )

        // Expected json
        String postAggDruidExp = ("""{
                                            "type":"sketchSetOper",
                                            "name": "bunreg",
                                            "func": "NOT",
                                            "fields": [
                                                        {
                                                            "type": "fieldAccess",
                                                            "fieldName": "bnoy"
                                                        },
                                                        {
                                                            "type": "fieldAccess",
                                                            "fieldName": "bwdy"
                                                        }
                                                      ],
                                            "size": 1234
                                        }""").replaceAll(/\s/, "")

        // serialized druid post agg
        String druidPostAggQuery1 = MAPPER.writeValueAsString(postAggregation)
        expect:
        GroovyTestUtils.compareJson(druidPostAggQuery1, postAggDruidExp)
    }
}
