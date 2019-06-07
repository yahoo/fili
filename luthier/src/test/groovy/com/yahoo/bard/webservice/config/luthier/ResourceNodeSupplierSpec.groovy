package com.yahoo.bard.webservice.config.luthier

import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import spock.lang.Specification

class ResourceNodeSupplierSpec extends Specification {
    ResourceNodeSupplier testResourceNodeSupplier

    void setup() {
        testResourceNodeSupplier = new ResourceNodeSupplier("DimensionConfig.json")
    }

    def "default ResourceNodeSupplier only returns null when supplied null"() {
        given:
            ResourceNodeSupplier defaultResourceNodeSupplier = new ResourceNodeSupplier(null)
        when:
            ObjectNode defaultNode = defaultResourceNodeSupplier.get()
            ObjectNode dimensionsNode = testResourceNodeSupplier.get()
        then:
            defaultNode == null
            dimensionsNode != null
    }

    def "a LuthierFactoryException is thrown when .json is not formatted correctly"() {
        given:
            ResourceNodeSupplier nonExistentResourceNodeSupplier = new ResourceNodeSupplier("NON_EXISTENT.json")
        when:
            ObjectNode nonExistentNode = nonExistentResourceNodeSupplier.get()
        then:
            thrown(LuthierFactoryException)
    }

    def "All contents of a test dimension is correct"() {
        when:
            ObjectNode node = testResourceNodeSupplier.get().get("testDimension")
            String longName = node.get("longName").textValue()
            ArrayNode fields = node.get("fields")
            String category = node.get("category").textValue()
            String description = node.get("description").textValue()
            String searchProvider = node.get("searchProvider").textValue()
            String keyValueStore = node.get("keyValueStore").textValue()
        then:
            longName == "a longName for testing"
            fields.size() == 4
            fields.get(0).get("tags").get(0).textValue() == "primaryKey"
            List expectedFieldNames = ["TEST_PK", "TEST_FIELD_1", "TEST_FIELD_2", "TEST_FIELD_3"]
            List expectedFieldTags = [ ["primarykey"], [], [], [] ]
            for (int i = 0; i < fields.size(); i++) {
                fields.get(i).get("name").textValue() == expectedFieldNames[i]
                fields.get(i).get("tags").textValue() == expectedFieldTags[i]
            }

            category == "a category for testing"
            description == "a description for testing"
            searchProvider == "com.yahoo.bard.webservice.data.dimension.impl.NoOpSearchProvider"
            keyValueStore == "com.yahoo.bard.webservice.data.dimension.MapStore"
    }

    def "All dimensions contain necessary keys (apiName, type, field, etc.), regardless their values"() {
        when:
            ObjectNode dimensionsNode = testResourceNodeSupplier.get()
        then:
            dimensionsNode.every {
                it.has("type")
                it.has("keyValueStore")
                it.has("longName")
                it.has("searchProvider")
                it.has("description")
                it.has("fields")
                it.get("fields").every {
                    it.has("name")
                    it.has("tags")
                }
            }
    }
}
