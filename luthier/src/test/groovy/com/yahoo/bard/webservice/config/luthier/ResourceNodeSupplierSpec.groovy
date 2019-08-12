// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config.luthier

import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.yahoo.bard.webservice.data.config.ResourceNodeSupplier
import com.yahoo.bard.webservice.exceptions.LuthierFactoryException
import spock.lang.Specification

class ResourceNodeSupplierSpec extends Specification {
    ResourceNodeSupplier dimensionNodeSupplier
    ResourceNodeSupplier searchProviderNodeSupplier

    void setup() {
        dimensionNodeSupplier = new ResourceNodeSupplier("DimensionConfig.json")
        searchProviderNodeSupplier = new ResourceNodeSupplier("SearchProviderConfig.json")
    }

    def "default ResourceNodeSupplier only returns null when supplied null"() {
        given:
            ResourceNodeSupplier defaultResourceNodeSupplier = new ResourceNodeSupplier(null)
        when:
            ObjectNode defaultNode = defaultResourceNodeSupplier.get()
            ObjectNode dimensionsNode = dimensionNodeSupplier.get()
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

    def "All content of a test domain in the searchProviderConfig.json is correct"() {
        when:
            ObjectNode node = searchProviderNodeSupplier.get().get("testDomain")
            String type = node.get("type").textValue()
            int maxResults = node.get("maxResults").intValue()
            int searchTimeout = node.get("searchTimeout").intValue()
            String indexPath = node.get("indexPath").textValue()

        then:
            type == "com.yahoo.bard.webservice.data.dimension.impl.LuceneSearchProvider"
            maxResults == 100000
            searchTimeout == 600000
            indexPath == "./target/tmp/lucene/"
    }

    def tagComp(List textNodeList, List strlist) {
        for (int i = 0; i < textNodeList.size(); i++) {
            assert textNodeList[i].textValue() == strlist[i]
        }
    }

    def "All contents of a test dimension is correct"() {
        when:
            ObjectNode node = dimensionNodeSupplier.get().get("testDimension")
            String longName = node.get("longName").textValue()
            ArrayNode fields = node.get("fields")
            ArrayNode defaultFields = node.get("defaultFields")
            String category = node.get("category").textValue()
            String description = node.get("description").textValue()
            Boolean isAggregatable = node.get("isAggregatable").booleanValue()
            String dimensionDomain = node.get("dimensionDomain").textValue()
            String searchProvider = node.get("searchProvider").textValue()
            String keyValueStore = node.get("keyValueStore").textValue()
            String storageStrategy = node.get("storageStrategy").textValue()
        then:
            longName == "a longName for testing"
            fields.size() == 4
            fields.get(0).get("tags").get(0).textValue() == "primaryKey"
            List expectedFieldNames = ["TEST_PK", "TEST_FIELD_1", "TEST_FIELD_2", "TEST_FIELD_3"]
            def expectedFieldTags = [ ["primaryKey"], [], [], [] ]
            for (int i = 0; i < fields.size(); i++) {
                assert fields.get(i).get("name").textValue() == expectedFieldNames[i]
                tagComp(fields.get(i).get("tags").asList(), expectedFieldTags[i])
            }
            List expectedDefaultFieldNames = ["TEST_PK", "TEST_FIELD_1"]
            List nonexpectedDefaultFieldNames = ["TEST_FIELD_2", "TEST_FIELD_3"]
            for (int i = 0; i < defaultFields.size(); i++) {
                String currentFieldName = defaultFields.get(i).textValue()
                assert currentFieldName == expectedDefaultFieldNames[i]
                assert !nonexpectedDefaultFieldNames.contains(currentFieldName)
            }
            !isAggregatable
            category == "a category for testing"
            description == "a description for testing"
            dimensionDomain == "testDomain"
            searchProvider == "lucene"
            keyValueStore == "memory"
            storageStrategy == "LOADED"
    }

    def "All dimensions contain necessary keys (apiName, type, field, etc.), regardless their values"() {
        when:
            ObjectNode dimensionsNode = dimensionNodeSupplier.get()
        then:
            dimensionsNode.every {
                it.has("type")
                it.has("keyValueStore")
                it.has("isAggregatable")
                it.has("category")
                it.has("longName")
                it.has("domain")
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
