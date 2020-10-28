// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints.views

import com.yahoo.bard.webservice.data.QueryBuildingTestingResources
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.web.MetadataObject

import spock.lang.Specification

import javax.ws.rs.core.UriBuilder
import javax.ws.rs.core.UriInfo

class MetricMetadataFormattingSpec extends Specification {

    QueryBuildingTestingResources resources = new QueryBuildingTestingResources();
    MetricMetadataFormatter metricFormatter = new MetricMetadataFormatter();

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
        LogicalMetric m1 = resources.m1
        uriInfo.getBaseUriBuilder()
        MetadataObject object = metricFormatter.formatMetricSummary(resources.m1, uriInfo)
        List<String> fields = ["category", "name", "longName", "uri", "type"]

        expect:
        object.keySet() == (fields as Set)
        object["name"] == m1.name
        object["longName"] == m1.longName
        object["uri"] == uri.toASCIIString()
        object["type"] == m1.type
    }

    def "Detailed view has table joined and all expected fields"() {
        setup:
        TableMetadataFormatter tableFormatter = new TableMetadataFormatter()
        LogicalTable t1 = resources.lt12
        LogicalTable t2 = resources.lt13
        LogicalTable t3 = resources.ltna
        LogicalMetric m1 = resources.m1
        uriInfo.getBaseUriBuilder()
        MetadataObject object = metricFormatter.formatLogicalMetricWithJoins(m1, resources.logicalDictionary, uriInfo)
        List<String> fields = ["category", "description", "name", "longName", "type", "tables"]

        expect:
        object.keySet() == (fields as Set)
        object["name"] == m1.name
        object["longName"] == m1.longName
        object["type"] == m1.type
        object["tables"]  == ([ tableFormatter.formatTableSummary(t1, uriInfo),
                               tableFormatter.formatTableSummary(t2, uriInfo),
                                tableFormatter.formatTableSummary(t3, uriInfo)
                              ] as Set)
    }

    def "List view contains summary records"() {
        setup:
        LogicalMetric m1 = resources.m1
        uriInfo.getBaseUriBuilder()
        Set<MetadataObject> objects = metricFormatter.formatMetricSummaryList([m1], uriInfo)
        List<String> fields = ["category", "name", "longName", "uri", "type"]

        MetadataObject object = (objects as List)[0]

        expect:
        object.keySet() == (fields as Set)
        object["name"] == m1.name
        object["longName"] == m1.longName
        object["uri"] == uri.toASCIIString()
        object["type"] == m1.type
    }
}
