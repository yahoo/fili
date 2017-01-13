package com.yahoo.bard.webservice.data.dimension

import static com.yahoo.bard.webservice.data.dimension.TestTaggedDimensionField.TEST_PRIMARY_KEY
import static com.yahoo.bard.webservice.data.dimension.TestTaggedDimensionField.TEST_DESCRIPTION
import static com.yahoo.bard.webservice.data.dimension.impl.DefaultDimensionFieldTag.PRIMARY_KEY

import com.fasterxml.jackson.databind.ObjectMapper

import spock.lang.Specification

/**
 * Test tagged dimension field behavior and serialization.
 */
class TaggedDimensionFieldSpec extends Specification {

    ObjectMapper objectMapper
    TaggedDimensionField dimensionField1
    TaggedDimensionField dimensionField2
    DimensionField dimensionField3

    def setup() {
        objectMapper = new ObjectMapper()
        dimensionField1 = TEST_PRIMARY_KEY
        dimensionField2 = TEST_DESCRIPTION
        dimensionField3 = TEST_PRIMARY_KEY
    }

    def "Dimension field interface should behave correctly"() {
        expect:
        dimensionField1.getName() == "testPrimaryKey"
        dimensionField1.getDescription() == "testPrimaryKey description"
        dimensionField1.getTags() == [PRIMARY_KEY]
        dimensionField2.getTags() == []
    }

    def "Tagged dimension fields serialize as expected"() {
        expect:
        objectMapper.writeValueAsString(dimensionField1) == '{"name":"testPrimaryKey","tags":["primaryKey"],"description":"testPrimaryKey description"}'
        objectMapper.writeValueAsString(dimensionField2) == '{"name":"testDescription","tags":[],"description":"testDescription description"}'
        objectMapper.writeValueAsString(dimensionField3) == '{"name":"testPrimaryKey","tags":["primaryKey"],"description":"testPrimaryKey description"}'
    }
}
