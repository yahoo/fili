// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints.views

import com.yahoo.bard.webservice.data.QueryBuildingTestingResources
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.web.MetadataObject

import spock.lang.Specification

import javax.ws.rs.core.UriBuilder
import javax.ws.rs.core.UriInfo

class DimensionMetadataFormattingSpec extends Specification {

    QueryBuildingTestingResources resources = new QueryBuildingTestingResources();
    DimensionMetadataFormatter dimensionFormatter = new DimensionMetadataFormatter();

    UriInfo uriInfo = Mock(UriInfo)
    UriBuilder uriBuilder = Mock(UriBuilder)
    URI uri = new URI("http://foo/bar")

    def setup() {
        uriInfo.getBaseUriBuilder() >> uriBuilder
        uriBuilder.path(_ as Class) >> uriBuilder
        uriBuilder.path(_ as Class, _ as String) >> uriBuilder
        uriBuilder.build(_) >> uri
    }

    def "Summary view has all expected fields"() {
        setup:
        Dimension d1 = resources.d1
        MetadataObject object = dimensionFormatter.formatDimensionSummary(resources.d1, uriInfo)
        List<String> fields = ["category", "name", "longName", "uri", "cardinality", "storageStrategy"]

        expect:
        object.keySet() == (fields as Set)
        object["name"] == d1.apiName
        object["longName"] == d1.longName
        object["uri"] == uri.toASCIIString()
        object["cardinality"] == d1.cardinality
        object["storageStrategy"] == d1.storageStrategy
    }

    def "Detailed view has table joined and all expected fields"() {
        setup:
        TableMetadataFormatter tableFormatter = new TableMetadataFormatter()
        LogicalTable t1 = resources.lt12
        LogicalTable t2 = resources.lt13
        Dimension d1 = resources.d1
        uriInfo.getBaseUriBuilder()
        MetadataObject object = dimensionFormatter.formatDimensionWithJoins(resources.d1, resources.logicalDictionary, uriInfo)
        List<String> fields = ["category", "name", "longName", "description",
                               "fields", "values", "cardinality", "storageStrategy", "tables"]

        expect:
        object.keySet() == (fields as Set)
        object["name"] == d1.apiName
        object["longName"] == d1.longName
        object["description"] == d1.description
        object["fields"] == d1.dimensionFields
        object["values"] == uri.toASCIIString()
        object["cardinality"] == d1.cardinality
        object["storageStrategy"] == d1.storageStrategy
        object["tables"]  == ([ tableFormatter.formatTableSummary(t1, uriInfo),
                               tableFormatter.formatTableSummary(t2, uriInfo)] as Set)
    }

    def "List view contains summary records"() {
        setup:
        Dimension d1 = resources.d1
        Set<MetadataObject> objects = dimensionFormatter.formatDimensionSummaryList([d1], uriInfo)
        List<String> fields = ["category", "name", "longName", "uri", "cardinality", "storageStrategy"]

        MetadataObject object = (objects as List)[0]

        expect:
        object.keySet() == (fields as Set)
        object["name"] == d1.apiName
        object["longName"] == d1.longName
        object["uri"] == uri.toASCIIString()
        object["cardinality"] == d1.cardinality
        object["storageStrategy"] == d1.storageStrategy
    }
}
