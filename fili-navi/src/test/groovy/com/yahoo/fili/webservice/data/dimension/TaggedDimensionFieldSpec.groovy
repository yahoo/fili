package com.yahoo.fili.webservice.data.dimension

import static com.yahoo.fili.webservice.data.dimension.TestTaggedDimensionField.TEST_TWO_TAG
import static com.yahoo.fili.webservice.data.dimension.TestTaggedDimensionField.TEST_ONE_TAG
import static com.yahoo.fili.webservice.data.dimension.TestTaggedDimensionField.TEST_NO_TAG
import static com.yahoo.fili.webservice.data.dimension.impl.DefaultDimensionFieldTag.PRIMARY_KEY

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
        objectMapper = new ObjectMapper()

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
        objectMapper.writeValueAsString(noTagField) == '{"name":"testNoTag","tags":[],"description":"testNoTag description"}'
        objectMapper.writeValueAsString(oneTagField) == '{"name":"testOneTag","tags":["primaryKey"],"description":"testOneTag description"}'
        objectMapper.writeValueAsString(twoTagField) == '{"name":"testTwoTag","tags":["primaryKey","mockTag"],"description":"testTwoTag description"}'
    }
}
