// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.auto;

import com.yahoo.bard.webservice.data.time.DefaultTimeGrain;
import com.yahoo.bard.webservice.data.time.GranularityDictionary;
import com.yahoo.bard.webservice.data.time.StandardGranularityParser;
import com.yahoo.bard.webservice.data.time.TimeGrain;
import com.yahoo.bard.webservice.druid.client.DruidWebService;
import com.yahoo.bard.webservice.druid.client.SuccessCallback;

import com.fasterxml.jackson.databind.JsonNode;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Searches for all datasources and their dimensions, metrics, and timegrains from druid.
 */
public class DruidNavigator implements Supplier<List<? extends DataSourceConfiguration>> {
    private static final Logger LOG = LoggerFactory.getLogger(DruidNavigator.class);
    private static final int COORDINATOR_PORT = 8081;
    private static final String COORDINATOR_BASE = "http://localhost:" + COORDINATOR_PORT + "/druid/coordinator/v1/";
    private final DruidWebService druidWebService;
    private final List<TableConfig> tableConfigurations;

    /**
     * Constructs a DruidNavigator to load datasources from druid.
     * @param druidWebService  The DruidWebService to be used when talking to druid.
     */
    public DruidNavigator(DruidWebService druidWebService) {
        this.druidWebService = druidWebService;
        tableConfigurations = new ArrayList<>();
    }

    /**
     * Queries druid for all datasources and loads each of their configurations.
     * @return a list of {@link DataSourceConfiguration} containing all known druid datasources configs.
     */
    @Override
    public List<? extends DataSourceConfiguration> get() {
        String url = COORDINATOR_BASE + "datasources/";
        queryDruid(rootNode -> {
            if (rootNode.isArray()) {
                for (final JsonNode objNode : rootNode) {
                    TableConfig tableConfig = new TableConfig(objNode.asText());
                    loadTable(tableConfig);
                    tableConfigurations.add(tableConfig);
                }
            }
        }, url);

        return tableConfigurations;
    }

    /**
     * Load a specific table with all reported metrics, dimensions, and timegrains.
     * @param table  The TableConfig to be loaded with queries against druid.
     */
    public void loadTable(TableConfig table) {
        String url = COORDINATOR_BASE + "datasources/" + table.getName() + "/?full";
        queryDruid(rootNode -> {
            //TODO: handle errors
            JsonNode segments = rootNode.get("segments").get(0);
            loadMetrics(table, segments);
            loadDimensions(table, segments);
            loadTimeGrains(table, segments);
            LOG.info("Loaded table " + table.getName());
        }, url);
    }

    /**
     * Add all metrics from druid query to the {@link TableConfig}.
     * @param table  The TableConfig to be loaded.
     * @param segmentJson  The JsonNode containing a list of metrics.
     */
    private void loadMetrics(TableConfig table, JsonNode segmentJson) {
        JsonNode metricsArray = segmentJson.get("metrics");
        String[] metrics = metricsArray.asText().split(",");
        for (String metric : metrics) {
            table.addMetric(metric);
        }
    }

    /**
     * Add all dimensions from druid query to the {@link TableConfig}.
     * @param tableName  The TableConfig to be loaded.
     * @param segmentJson  The JsonNode containing a list of dimensions.
     */
    private void loadDimensions(TableConfig tableName, JsonNode segmentJson) {
        JsonNode dimensionsArray = segmentJson.get("dimensions");
        String[] dimensions = dimensionsArray.asText().split(",");
        for (String dimension : dimensions) {
            tableName.addDimension(dimension);
        }
    }

    /**
     * Find a valid timegrain from druid query to the {@link TableConfig}.
     * @param tableConfig  The TableConfig to be loaded.
     * @param segmentJson  The JsonNode containing a time interval.
     */
    private void loadTimeGrains(TableConfig tableConfig, JsonNode segmentJson) {
        JsonNode timeInterval = segmentJson.get("interval");
        String[] utcTimes = timeInterval.asText().split("/");
        if (utcTimes.length >= 2) {
            DateTime start = new DateTime(utcTimes[0], DateTimeZone.UTC);
            DateTime end = new DateTime(utcTimes[1], DateTimeZone.UTC);
            TimeGrain timeGrain = getTimeGrain(start, end);
            if (timeGrain != null) {
                tableConfig.addTimeGrain(timeGrain);
            }
        }
    }

    /**
     * Find a valid timegrain for the datasource based on the start and end date of the interval.
     * @param start  Start of interval.
     * @param end  End of interval.
     * @return the valid timegrain spanned by the interval.
     */
    private TimeGrain getTimeGrain(DateTime start, DateTime end) {
        GranularityDictionary grains = StandardGranularityParser.getDefaultGrainMap();
        Optional<TimeGrain> grain = grains.values().stream()
                .filter(granularity -> granularity instanceof TimeGrain)
                .map(granularity -> (TimeGrain) granularity)
                .filter(timeGrain -> timeGrain.aligns(start))
                .filter(timeGrain -> timeGrain.aligns(end))
                .filter(timeGrain -> start.plus(timeGrain.getPeriod()).equals(end))
                .findFirst();

        if (!grain.isPresent()) {
            LOG.info("Couldn't detect default timegrain for " + start + "-" + end + ", defaulting to DAY TimeGrain.");
        }
        return grain.orElse(DefaultTimeGrain.DAY);
    }

    /**
     * Send a request to druid.
     * @param successCallback  The callback to be done if the query succeeds.
     * @param url  The url to send the query to.
     */
    private void queryDruid(SuccessCallback successCallback, String url) {
        LOG.debug("Fetching " + url);
        druidWebService.getJsonObject(
                rootNode -> {
                    LOG.debug("Succesfully fetched " + url);
                    successCallback.invoke(rootNode);
                },
                (statusCode, reasonPhrase, responseBody) -> {
                    LOG.info("HTTPError " + statusCode + " - " + reasonPhrase + " for " + url);
                },
                (throwable) -> {
                    LOG.info("Error thrown while fetching " + url + ". " + throwable.getMessage());
                },
                url
        );
    }
}
