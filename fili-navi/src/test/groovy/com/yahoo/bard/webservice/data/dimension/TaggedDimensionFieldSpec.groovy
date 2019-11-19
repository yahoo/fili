// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension

import static com.yahoo.bard.webservice.data.dimension.TestTaggedDimensionField.TEST_NO_TAG
import static com.yahoo.bard.webservice.data.dimension.TestTaggedDimensionField.TEST_ONE_TAG
import static com.yahoo.bard.webservice.data.dimension.TestTaggedDimensionField.TEST_TWO_TAG
import static com.yahoo.bard.webservice.data.dimension.impl.DefaultDimensionFieldTag.PRIMARY_KEY

import com.yahoo.bard.webservice.application.ObjectMappersSuite

import com.fasterxml.jackson.databind.ObjectMapper

import spock.lang.Specification

/**
 * Test tagged dimension field behavior and serialization.
 */
class TaggedDimensionFieldSpec extends Specification {

    ObjectMapper objectMapper
    Tag mockTag
    DimensionField noTagField
    TaggedDimensionField oneTagField
    TaggedDimensionField twoTagField

    def setup() {
        objectMapper = new ObjectMappersSuite().getMapper()

        mockTag = Mock(Tag)
        mockTag.getName() >> "mockTag"

        noTagField = TEST_NO_TAG
        oneTagField = TEST_ONE_TAG
        twoTagField = TEST_TWO_TAG

        noTagField.setTags([] as Set<Tag>)
        oneTagField.setTags([PRIMARY_KEY] as Set<Tag>)
        twoTagField.setTags([PRIMARY_KEY, mockTag] as Set<Tag>)
    }

    def "Dimension field interface should behave correctly"() {
        expect:
        oneTagField.getName() == "testOneTag"
        oneTagField.getDescription() == "testOneTag description"
        oneTagField.getTags() == [PRIMARY_KEY] as Set
        twoTagField.getTags() == [PRIMARY_KEY, mockTag] as Set
    }

    def "Tagged dimension fields serialize as expected"() {
        expect:
        objectMapper.writeValueAsString(noTagField) == '{"name":"testNoTag","description":"testNoTag description","tags":[]}'
        objectMapper.writeValueAsString(oneTagField) == '{"name":"testOneTag","description":"testOneTag description","tags":["primaryKey"]}'
        objectMapper.writeValueAsString(twoTagField) == '{"name":"testTwoTag","description":"testTwoTag description","tags":["primaryKey","mockTag"]}'
    }
}
