// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.application.metadataViews.DimensionSummaryViewWithFieldsProvider
import com.yahoo.bard.webservice.application.metadataViews.IndividualTableViewProvider
import com.yahoo.bard.webservice.application.metadataViews.LogicalMetricSummaryViewProvider
import com.yahoo.bard.webservice.application.metadataViews.MetadataViewProvider
import com.yahoo.bard.webservice.data.config.dimension.DefaultDimensionField
import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionField
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.table.LogicalTableDictionary
import com.yahoo.bard.webservice.util.GroovyTestUtils
import com.yahoo.bard.webservice.util.JsonSortStrategy
import com.yahoo.bard.webservice.web.apirequest.TablesApiRequestImpl
import com.yahoo.bard.webservice.web.endpoints.MetricsServlet
import com.yahoo.bard.webservice.web.endpoints.TablesServlet

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper

import org.apache.commons.lang.StringUtils

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Timeout

import java.util.stream.Collectors

import javax.ws.rs.container.ContainerRequestContext
import javax.ws.rs.core.UriBuilder
import javax.ws.rs.core.UriInfo

@Timeout(30)    // Fail test if hangs
class TableFullViewProcessorSpec extends Specification {
    private static ObjectMapper MAPPER = new ObjectMappersSuite().getMapper()

    JerseyTestBinder jtb

    @Shared
    LogicalTableDictionary fullDictionary

    TableFullViewProcessor fullViewProcessor
    ContainerRequestContext containerRequestContext = Mock()
    UriBuilder builder = Mock(UriBuilder)
    String baseUri = "http://localhost:9998/v1/"
    Set<LogicalTable> petsShapesTables

    MetadataViewProvider<LogicalTable> singleTableViewProvider
    MetadataViewProvider<Dimension> dimensionSummaryWithFieldsViewProvider
    MetadataViewProvider<LogicalMetric> metricSummaryViewProvider

    def setup() {
        jtb = new JerseyTestBinder(TablesServlet.class)
        fullDictionary = jtb.configurationLoader.logicalTableDictionary

        UriInfo uriInfo = Mock()
        uriInfo.getBaseUriBuilder() >> builder
        containerRequestContext.getUriInfo() >> uriInfo

        singleTableViewProvider = new IndividualTableViewProvider()
        dimensionSummaryWithFieldsViewProvider = new DimensionSummaryViewWithFieldsProvider()
        metricSummaryViewProvider = new LogicalMetricSummaryViewProvider()

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
        TableView tableView = fullViewProcessor.formatTable(shapesTable, containerRequestContext, singleTableViewProvider)

        then:
        tableView.toString() == expectedResponse
    }

    def "Check table meta data info at grain level"() {

        setup:
        Set<DimensionField> dimensionFields = new HashSet<>()
        dimensionFields.add(BardDimensionField.ID)
        dimensionFields.add(BardDimensionField.DESC)

        Map<String, Object> dimensions_1 = new HashMap<>()
        dimensions_1.put("category", "General")
        dimensions_1.put("name", "breed")
        dimensions_1.put("longName", "breed")
        dimensions_1.put("uri", "http://localhost:9998/v1/breed")
        dimensions_1.put("cardinality", 0)
        dimensions_1.put("storageStrategy", "loaded")
        dimensions_1.put("fields", dimensionFields)

        Map<String, Object> dimensions_2 = new HashMap<>()
        dimensions_2.put("category", "General")
        dimensions_2.put("name", "sex")
        dimensions_2.put("longName", "sex")
        dimensions_2.put("uri", "http://localhost:9998/v1/sex")
        dimensions_2.put("cardinality", 0)
        dimensions_2.put("storageStrategy", "loaded")
        dimensions_2.put("fields", dimensionFields)

        Map<String, Object> dimensions_3 = new HashMap<>()
        dimensions_3.put("category", "General")
        dimensions_3.put("name", "species")
        dimensions_3.put("longName", "species")
        dimensions_3.put("uri", "http://localhost:9998/v1/species")
        dimensions_3.put("cardinality", 0)
        dimensions_3.put("storageStrategy", "loaded")
        dimensions_3.put("fields", dimensionFields)

        Set<Map<String, Object>> allDimensions = new HashSet<>()
        allDimensions.add(dimensions_1)
        allDimensions.add(dimensions_2)
        allDimensions.add(dimensions_3)

        Map<String, String> metrics_1 = new HashMap<>()
        metrics_1.put("category", "General")
        metrics_1.put("name", "rowNum")
        metrics_1.put("longName", "rowNum")
        metrics_1.put("type", "number")
        metrics_1.put("uri", "http://localhost:9998/v1/rowNum")

        Map<String, String> metrics_2 = new HashMap<>()
        metrics_2.put("category", "General")
        metrics_2.put("name", "limbs")
        metrics_2.put("longName", "limbs")
        metrics_2.put("type", "number")
        metrics_2.put("uri", "http://localhost:9998/v1/limbs")

        Map<String, String> metrics_3 = new HashMap<>()
        metrics_3.put("category", "General")
        metrics_3.put("name", "dayAvgLimbs")
        metrics_3.put("longName", "dayAvgLimbs")
        metrics_3.put("type", "number")
        metrics_3.put("uri", "http://localhost:9998/v1/dayAvgLimbs")

        Set<Map<String, String>> allMetrics = new HashSet<>()
        allMetrics.add(metrics_1)
        allMetrics.add(metrics_2)
        allMetrics.add(metrics_3)

        TableGrainView expectedGrainView = new TableGrainView()
        expectedGrainView.put("description", "The pets all grain")
        expectedGrainView.put("dimensions", allDimensions)
        expectedGrainView.put("longName", "All")
        expectedGrainView.put("metrics", allMetrics)
        expectedGrainView.put("name","all")
        expectedGrainView.put("retention", "P1Y")

        LogicalTable petsTable = petsShapesTables.find {it.getName() == "pets"}

        when:
        TableGrainView tableGrainView = fullViewProcessor.formatTableGrain(petsTable, "all", containerRequestContext, dimensionSummaryWithFieldsViewProvider, metricSummaryViewProvider)

        String actualJsonResult = MAPPER.writeValueAsString(tableGrainView)
        String expectedJsonResult = MAPPER.writeValueAsString(expectedGrainView)

        then:
        GroovyTestUtils.compareJson(actualJsonResult, expectedJsonResult, JsonSortStrategy.SORT_BOTH)
    }

    def "check all the tables full view at grain level"() {

        setup:
        String expectedKeyset = "[name, description, category, longName, timeGrains]"
        Set<LogicalTable> petsTables = petsShapesTables.findAll {it.getName() == "pets"}

        when:
        List<TableView> tableViews = fullViewProcessor.formatTables(
                petsTables,
                containerRequestContext,
                [
                        "dimensions.summary.withfields.view" : dimensionSummaryWithFieldsViewProvider,
                        "metrics.summary.view" : metricSummaryViewProvider,
                        "tables.singletable.view": singleTableViewProvider
                ]
        )

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
                    new TableFullViewProcessor().getDimensionListFullView(lt.dimensions, containerRequestContext, dimensionSummaryWithFieldsViewProvider)
            )
            resultRow.put(
                    "metrics",
                    lt.getLogicalMetrics()
                            .stream()
                            .map({logicalMetric -> metricSummaryViewProvider.apply(
                            containerRequestContext,
                            logicalMetric
                    )})
                            .collect(Collectors.toSet())
            )
            resultSet.add(resultRow)
        }
        tableView.put("timeGrains", resultSet)

        return [tableView]
    }
}
