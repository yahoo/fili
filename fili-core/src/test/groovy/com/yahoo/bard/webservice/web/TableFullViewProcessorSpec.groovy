// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.table.LogicalTableDictionary
import com.yahoo.bard.webservice.web.apirequest.TablesApiRequestImpl
import com.yahoo.bard.webservice.web.endpoints.MetricsServlet
import com.yahoo.bard.webservice.web.endpoints.TablesServlet

import org.apache.commons.lang.StringUtils

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Timeout

import javax.ws.rs.core.UriBuilder
import javax.ws.rs.core.UriInfo

@Timeout(30)    // Fail test if hangs
class TableFullViewProcessorSpec extends Specification {

    JerseyTestBinder jtb

    @Shared
    LogicalTableDictionary fullDictionary

    TableFullViewProcessor fullViewProcessor
    UriInfo uriInfo = Mock(UriInfo)
    UriBuilder builder = Mock(UriBuilder)
    String baseUri = "http://localhost:9998/v1/"
    Set<LogicalTable> petsShapesTables

    def setup() {
        jtb = new JerseyTestBinder(TablesServlet.class)
        fullDictionary = jtb.configurationLoader.logicalTableDictionary

        uriInfo.getBaseUriBuilder() >> builder
        builder.path(_) >> builder
        builder.path(_, _) >> builder

        builder.build(_) >> { List<List<String>> args -> new URI(baseUri+args[0][0]) }

        TablesServlet tablesServlet = Mock(TablesServlet)
        tablesServlet.getLogicalTableDictionary() >> fullDictionary
        TablesApiRequestImpl tablesApiRequestImpl = new TablesApiRequestImpl(null, null, null, "", "", null, tablesServlet)
        Set<LogicalTable> logicalTableSet = tablesApiRequestImpl.getTables();

        petsShapesTables= new HashSet<>()
        petsShapesTables = logicalTableSet.findAll {it.getName() == "pets" || it.getName() == "shapes"}
        fullViewProcessor = new TableFullViewProcessor()
    }

    def cleanup() {
        jtb.tearDown()
    }

    def "Check table meta data info at table level"() {
        setup:
        String expectedResponse = "[name:shapes, description:shapes, category:General, longName:shapes]"
        LogicalTable shapesTable = petsShapesTables.find {it.getName() == "shapes"}

        when:
        TableView tableView = fullViewProcessor.formatTable(shapesTable, uriInfo)

        then:
        tableView.toString() == expectedResponse
    }

    def "Check table meta data info at grain level"() {

        setup:
        String expectedResponse = "[description:The pets all grain, dimensions:[[category:General, name:breed, longName:breed, uri:http://localhost:9998/v1/breed, cardinality:0, fields:[id, desc]], [category:General, name:sex, longName:sex, uri:http://localhost:9998/v1/sex, cardinality:0, fields:[id, desc]], [category:General, name:species, longName:species, uri:http://localhost:9998/v1/species, cardinality:0, fields:[id, desc]]], longName:All, metrics:[[category:General, name:rowNum, longName:rowNum, uri:http://localhost:9998/v1/rowNum], [category:General, name:limbs, longName:limbs, uri:http://localhost:9998/v1/limbs], [category:General, name:dayAvgLimbs, longName:dayAvgLimbs, uri:http://localhost:9998/v1/dayAvgLimbs]], name:all, retention:P1Y]"
        LogicalTable petsTable = petsShapesTables.find {it.getName() == "pets"}

        when:
        TableGrainView tableGrainView = fullViewProcessor.formatTableGrain(petsTable, "all", uriInfo)

        then:
        tableGrainView.sort().toString() == expectedResponse
    }

    def "check all the tables full view at grain level"() {

        setup:
        String expectedKeyset = "[name, description, category, longName, timeGrains]"
        Set<LogicalTable> petsTables = petsShapesTables.findAll {it.getName() == "pets"}

        when:
        List<TableView> tableViews = fullViewProcessor.formatTables(petsTables, uriInfo)

        then:
        getExpectedOutput(petsTables).sort() == tableViews.sort()
        tableViews.first().keySet().toString() == expectedKeyset
        tableViews.size() == 1
    }

    //Prepare an expected output for the given logical table
    public ArrayList<TableView> getExpectedOutput(Set<LogicalTable> petsTables) {

        TableView tableView = new TableView()
        tableView.put("name", "pets")
        tableView.put("description", "pets")
        tableView.put("longName", "pets")
        tableView.put("category", "General")
        List<TableGrainView> resultSet = new ArrayList<>()

        for (LogicalTable lt : petsTables) {
            TableGrainView resultRow = new TableGrainView()
            resultRow.put("name", lt.granularity.getName())
            resultRow.put("longName", StringUtils.capitalize(lt.granularity.getName()))
            resultRow.put("description", "The " + lt.getName() + " " + lt.granularity.getName() + " grain")
            resultRow.put("retention", lt.getRetention().toString())
            resultRow.put(
                    "dimensions",
                    new TableFullViewProcessor().getDimensionListFullView(lt.dimensions, uriInfo)
            )
            resultRow.put("metrics", MetricsServlet.getLogicalMetricListSummaryView(lt.getLogicalMetrics(), uriInfo))
            resultSet.add(resultRow)
        }
        tableView.put("timeGrains", resultSet)

        return [tableView]
    }
}
