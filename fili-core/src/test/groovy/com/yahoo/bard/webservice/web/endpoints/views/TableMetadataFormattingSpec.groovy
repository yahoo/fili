// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints.views

import com.yahoo.bard.webservice.data.QueryBuildingTestingResources
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.web.MetadataObject

import spock.lang.Specification

import javax.ws.rs.core.UriBuilder
import javax.ws.rs.core.UriInfo

class TableMetadataFormattingSpec extends Specification {

    TableMetadataFormatter tableMetadataFormatter
    UriInfo uriInfo = Mock(UriInfo)
    UriBuilder uriBuilder = Mock(UriBuilder)
    URI uri = new URI("http://foo/bar")

    QueryBuildingTestingResources resources = new QueryBuildingTestingResources()

    def setup() {
        uriInfo.getBaseUriBuilder() >> uriBuilder
        uriBuilder.path(_ as Class) >> uriBuilder
        uriBuilder.path(_ as Class, _ as String) >> uriBuilder
        uriBuilder.build(_) >> uri
    }

    def "Detailed view has dimensions and metrics joined and all expected fields"() {
        setup:
        Dimension d1 = resources.d1
        LogicalMetric m1 = resources.m1
        tableMetadataFormatter = new TableMetadataFormatter()
        uriInfo.getBaseUriBuilder()
        MetadataObject object = tableMetadataFormatter.formatTable(resources.lt12, uriInfo)
        List<String> fields = ["availableIntervals", "category", "description", "dimensions", "granularity",
                               "longName", "metrics", "name", "retention"]

        expect:
        object.keySet() == (fields as Set)
        object["name"] == "base12"
        object["longName"] == "base12"
        object["granularity"] == "hour"
        MetadataObject dimension = (object["dimensions"] as List)[0] as MetadataObject
        dimension["name"] == d1.getApiName()
        dimension["longName"] == d1.getLongName()
        dimension["uri"] == uri.toASCIIString()
        dimension["storageStrategy"] == d1.storageStrategy
        MetadataObject metric = (object["metrics"] as List)[0] as MetadataObject
        metric.keySet() == (["category", "name", "longName", "type", "uri"] as Set)
        metric["name"]  == m1.name
        metric["category"]  == m1.category
        metric["longName"]  == m1.longName
        metric["type"]  == m1.type
    }

    def "Summary view has expected fields"() {
        setup:
        tableMetadataFormatter = new TableMetadataFormatter()
        uriInfo.getBaseUriBuilder()
        MetadataObject object = tableMetadataFormatter.formatTableSummary(resources.lt12, uriInfo)
        List<String> fields = ["category", "granularity", "longName", "name", "uri"]

        expect:
        object.keySet() == (fields as Set)
        object["name"] == "base12"
        object["longName"] == "base12"
        object["granularity"] == "hour"
        object["uri"] == uri.toASCIIString()
    }

    def "List view has summary fields in records"() {
        setup:
        tableMetadataFormatter = new TableMetadataFormatter()
        uriInfo.getBaseUriBuilder()
        Set<MetadataObject> objects = tableMetadataFormatter.formatTables([resources.lt12], uriInfo)
        List<String> fields = ["category", "granularity", "longName", "name", "uri"]
        MetadataObject object = (objects as List)[0]

        expect:
        object.keySet() == (fields as Set)
        object["name"] == "base12"
        object["longName"] == "base12"
        object["granularity"] == "hour"
        object["uri"] == uri.toASCIIString()
    }
}
