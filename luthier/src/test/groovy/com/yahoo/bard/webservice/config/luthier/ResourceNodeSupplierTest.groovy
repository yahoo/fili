package com.yahoo.bard.webservice.config.luthier

import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import spock.lang.Specification

class ResourceNodeSupplierTest extends Specification {
    ResourceNodeSupplier testResourceNodeSupplier
    void setup() {
        testResourceNodeSupplier = new ResourceNodeSupplier("DimensionConfig.json")
    }

    def "default ResourceNodeSupplier only returns null when supplied null"() {
        given:
            ResourceNodeSupplier defaultResourceNodeSupplier = new ResourceNodeSupplier(null)
        when:
            ObjectNode defaultNode = defaultResourceNodeSupplier.get()
            ObjectNode dimensionNode = testResourceNodeSupplier.get()
        then:
            defaultNode == null
            dimensionNode != null
    }

    def "a LuthierFactoryException is thrown when .json is not formatted correctly"() {
        given:
            ResourceNodeSupplier nonExistentResourceNodeSupplier = new ResourceNodeSupplier("NON_EXISTENT.json")
        when:
            ObjectNode nonExistentNode = nonExistentResourceNodeSupplier.get()
        then:
            thrown(LuthierFactoryException)
    }

    def "Fields are loaded and content is correct"() {
        when:
            ObjectNode node = testResourceNodeSupplier.get().get("testDimension")
            String apiName = node.get("apiName").textValue()
            String longName = node.get("longName").textValue()
            ArrayNode fields = node.get("fields")
            String category = node.get("category").textValue()
            String description = node.get("description").textValue()
            String searchProvider = node.get("searchProvider").textValue()
            String keyValueStore = node.get("keyValueStore").textValue()
        then:
            apiName == "testDimension"
            longName == "a longName for testing"
            fields.size() == 4
            fields.get(0).get("name").textValue() == "TEST_PK"
            fields.get(0).get("tags").get(0).textValue() == "primaryKey"
            fields.get(1).get("name").textValue() == "TEST_FIELD_1"
            fields.get(2).get("name").textValue() == "TEST_FIELD_2"
            fields.get(3).get("name").textValue() == "TEST_FIELD_3"
            category == "a category for testing"
            description == "a description for testing"
            searchProvider == "com.yahoo.bard.webservice.data.dimension.impl.NoOpSearchProvider"
            keyValueStore == "com.yahoo.bard.webservice.data.dimension.MapStore"
    }
}
