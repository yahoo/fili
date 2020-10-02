// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints.views

import com.yahoo.bard.webservice.data.QueryBuildingTestingResources
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.table.LogicalTableDictionary
import com.yahoo.bard.webservice.web.MetadataObject
import com.yahoo.bard.webservice.web.apirequest.TablesApiRequestImpl
import com.yahoo.bard.webservice.web.endpoints.TablesServlet

import org.apache.commons.lang.StringUtils

import spock.lang.Specification
import spock.lang.Timeout

import javax.ws.rs.core.UriBuilder
import javax.ws.rs.core.UriInfo

@Timeout(30)    // Fail test if hangs
class TableFullViewFormatterSpec extends Specification {

    QueryBuildingTestingResources resources = new QueryBuildingTestingResources()

    LogicalTableDictionary fullDictionary = resources.logicalDictionary

    TableFullViewFormatter fullViewProcessor
    UriInfo uriInfo = Mock(UriInfo)
    UriBuilder uriBuilder = Mock(UriBuilder)
    String baseUri = "http://localhost:9998/v1/"

    def setup() {
        uriInfo.getBaseUriBuilder() >> uriBuilder
        uriBuilder.path(_ as Class) >> uriBuilder
        uriBuilder.path(_ as Class, _ as String) >> uriBuilder
        uriBuilder.build(_) >> baseUri

        uriBuilder.build(_) >> { List<List<String>> args -> new URI(baseUri+args[0][0]) }

        TablesServlet tablesServlet = Mock(TablesServlet)
        tablesServlet.getLogicalTableDictionary() >> fullDictionary
        TablesApiRequestImpl tablesApiRequestImpl = new TablesApiRequestImpl(
                null,  // tableName
                null,  // granularity
                null,  // format
                "",  // perPage
                "",  // page
                tablesServlet
        )
        fullViewProcessor = new TableFullViewFormatter()
    }

    def "Check single table summary has no joins"() {
        setup:
        String expectedResponse = "[name:base12, longName:base12, description:base12, category:General]"
        LogicalTable table = resources.lt12

        when:
        MetadataObject tableView = fullViewProcessor.rollupGroupHeader(table, uriInfo)

        then:
        tableView.toString() == expectedResponse
    }

    def "Check table with rollup joins for a single grain"() {
        setup:
        LogicalTable table = resources.lt13
        Dimension dimension = resources.d1
        LogicalMetric m1 = resources.m1
        LogicalMetric m2 = resources.m2
        LogicalMetric m3 = resources.m3
        String capitalName = StringUtils.capitalize(table.getGranularity().toString())

        String expectedResponse =
        "[description:The ${table.name} ${table.granularity} grain, " +
        // Dimensions
        "dimensions:[[category:General, " +
        "name:${dimension.apiName}, longName:${dimension.longName}, cardinality:${dimension.cardinality}, " +
        "fields:${dimension.dimensionFields}, storageStrategy:${dimension.storageStrategy}]], " +
        "longName:${capitalName}, " +
        // Metrics
        "metrics:[" +
        "[category:General, name:${m1.name}, longName:${m1.longName}, type:${m1.type}], " +
        "[category:General, name:${m2.name}, longName:${m2.longName}, type:${m2.type}], " +
        "[category:General, name:${m3.name}, longName:${m3.longName}, type:${m3.type}]" +
        "], " +
        "name:${table.granularity}, retention:]"

        when:
        MetadataObject tableGrainView = fullViewProcessor.formatTable(table, uriInfo)

        then:
        tableGrainView.sort().toString() == expectedResponse
    }

    def "check all the tables full grouped and joined by grain"() {
        setup:
        String expectedKeyset = "[name, longName, description, category, timeGrains]"
        List<LogicalTable> tables = [resources.lt13, resources.lt13All]

        when:
        Set<MetadataObject> tableViews = fullViewProcessor.formatTables(tables, uriInfo)

        then:
        tableViews.size() == 1
        MetadataObject tableObject = (tableViews as List)[0]
        tableObject["name"] == resources.lt13["name"]
        tableObject["description"] == resources.lt13["description"]
        tableObject["longName"] == resources.lt13["longName"]
        tableObject["category"] == resources.lt13["category"]

        Set<MetadataObject> grainObjects = tableObject["timeGrains"]
        grainObjects.size() == 2

        when:
        MetadataObject grainObject = (grainObjects as List)[1]

        then:
        grainObject["name"] == "all"
        grainObject["longName"] == "All"
        grainObject["description"]  == "The base13 all grain"

        when:
        grainObject = (grainObjects as List)[0]

        then:
        grainObject["name"] == "day"
        grainObject["longName"] == "Day"
        grainObject["description"]  == "The base13 day grain"

        Set<MetadataObject> dimensions = grainObject["dimensions"]
        MetadataObject dimensionObject = (grainObject["dimensions"] as List)[0]
        dimensionObject["name"] == resources.d1.apiName
        dimensionObject["longName"] == resources.d1.longName
        dimensionObject["category"] == resources.d1.category
        ((Set) dimensionObject["fields"]).size() == resources.d1.dimensionFields.size()
        dimensionObject["storageStrategy"] == resources.d1.storageStrategy

        Set<MetadataObject> metrics = grainObject["metrics"]
        MetadataObject metricObject = (metrics as List)[0]
        metricObject.size() == 4
        metricObject["category"] == resources.m1.category
        metricObject["name"] == resources.m1.name
        metricObject["longName"] == resources.m1.longName
        metricObject["type"] == resources.m1.type
    }
}
