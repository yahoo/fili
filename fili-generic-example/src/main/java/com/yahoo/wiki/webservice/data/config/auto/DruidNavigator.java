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
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Searches for all datasources and their dimensions, metrics, and timegrains from druid.
 */
public class DruidNavigator implements Supplier<List<? extends DataSourceConfiguration>> {
    private static final Logger LOG = LoggerFactory.getLogger(DruidNavigator.class);
    private static final String DATASOURCES = "/datasources/";
    private final DruidWebService druidWebService;
    private final List<TableConfig> tableConfigurations;

    /**
     * Constructs a DruidNavigator to load datasources from druid.
     *
     * @param druidWebService The DruidWebService to be used when talking to druid.
     */
    public DruidNavigator(DruidWebService druidWebService) {
        this.druidWebService = druidWebService;
        tableConfigurations = new ArrayList<>();
    }

    /**
     * Queries druid for all datasources and loads each of their configurations.
     *
     * @return a list of {@link DataSourceConfiguration} containing all known druid datasources configs.
     */
    @Override
    public List<? extends DataSourceConfiguration> get() {
        if (tableConfigurations.isEmpty()) {
            loadAllDatasources();
            LOG.info("Loading all datasources");
            try {
                //TODO Fix this so it waits for druid's responses
                // instead of waiting 1 second for everything to be configured
                // in order to wait for druid's response, DruidWebService will have to return a future
                Thread.sleep(1000);
                LOG.info("Finished loading");
            } catch (InterruptedException e) {
                LOG.info("Interrupted while waiting for druid to respond - {}", e);
            }
        }

        return tableConfigurations;
    }

    /**
     * Queries druid for all datasources and loads all of their tables.
     */
    private void loadAllDatasources() {
        queryDruid(rootNode -> {
            if (rootNode.isArray()) {
                rootNode.forEach(jsonNode -> {
                    TableConfig tableConfig = new TableConfig(jsonNode.asText());
                    loadTable(tableConfig);
                    tableConfigurations.add(tableConfig);
                });
            }
        }, DATASOURCES);
    }

    /**
     * Load a specific table with all reported metrics, dimensions, and timegrains.
     *
     * @param table The TableConfig to be loaded with queries against druid.
     */
    private void loadTable(TableConfig table) {
        String url = DATASOURCES + table.getName() + "/?full";
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
     *
     * @param table       The TableConfig to be loaded.
     * @param segmentJson The JsonNode containing a list of metrics.
     */
    private void loadMetrics(TableConfig table, JsonNode segmentJson) {
        JsonNode metricsArray = segmentJson.get("metrics");
        Arrays.asList(metricsArray.asText().split(",")).forEach(table::addMetric);
        LOG.info("loaded metrics {}", table.getMetrics());
    }

    /**
     * Add all dimensions from druid query to the {@link TableConfig}.
     *
     * @param table   The TableConfig to be loaded.
     * @param segmentJson The JsonNode containing a list of dimensions.
     */
    private void loadDimensions(TableConfig table, JsonNode segmentJson) {
        JsonNode dimensionsArray = segmentJson.get("dimensions");
        Arrays.asList(dimensionsArray.asText().split(",")).forEach(table::addDimension);
        LOG.info("loaded dimensions {}", table.getDimensions());
    }

    /**
     * Find a valid timegrain from druid query to the {@link TableConfig}.
     *
     * @param tableConfig The TableConfig to be loaded.
     * @param segmentJson The JsonNode containing a time interval.
     */
    private void loadTimeGrains(TableConfig tableConfig, JsonNode segmentJson) {
        JsonNode timeInterval = segmentJson.get("interval");
        String[] utcTimes = timeInterval.asText().split("/");
        Optional<TimeGrain> timeGrain = Optional.empty();
        try {
            if (utcTimes.length == 2) {
                DateTime start = new DateTime(utcTimes[0], DateTimeZone.UTC);
                DateTime end = new DateTime(utcTimes[1], DateTimeZone.UTC);
                timeGrain = getTimeGrain(start, end);
            }
        } catch (IllegalArgumentException ignored) {
            LOG.debug("Unable to parse time intervals {} correctly", Arrays.toString(utcTimes));
        }

        if (!timeGrain.isPresent()) {
            LOG.info("Couldn't detect timegrain for " + timeInterval.asText() + ", defaulting to DAY TimeGrain.");
        }
        tableConfig.addTimeGrain(timeGrain.orElse(DefaultTimeGrain.DAY));
    }

    /**
     * Find a valid timegrain for the datasource based on the start and end date of the interval.
     *
     * @param start Start of interval.
     * @param end   End of interval.
     * @return the valid timegrain spanned by the interval.
     */
    private Optional<TimeGrain> getTimeGrain(DateTime start, DateTime end) {
        GranularityDictionary grains = StandardGranularityParser.getDefaultGrainMap();
        return grains.values().stream()
                .filter(granularity -> granularity instanceof TimeGrain)
                .map(granularity -> (TimeGrain) granularity)
                .filter(timeGrain -> timeGrain.aligns(start))
                .filter(timeGrain -> timeGrain.aligns(end))
                .filter(timeGrain -> start.plus(timeGrain.getPeriod()).equals(end))
                .findFirst();
    }

    /**
     * Send a request to druid.
     *
     * @param successCallback The callback to be done if the query succeeds.
     * @param url             The url to send the query to.
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
