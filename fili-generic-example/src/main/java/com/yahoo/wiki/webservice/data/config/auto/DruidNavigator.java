// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.auto;

import com.yahoo.bard.webservice.data.time.DefaultTimeGrain;
import com.yahoo.bard.webservice.data.time.TimeGrain;
import com.yahoo.bard.webservice.druid.client.DruidWebService;
import com.yahoo.bard.webservice.druid.client.SuccessCallback;
import com.yahoo.bard.webservice.util.IntervalUtils;

import com.fasterxml.jackson.databind.JsonNode;

import org.asynchttpclient.Response;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * Searches for all datasources and their dimensions, metrics, and timegrains from druid.
 * The first segment returned from a druid table's schema is used for configuration.
 */
public class DruidNavigator implements Supplier<List<? extends DataSourceConfiguration>> {
    private static final Logger LOG = LoggerFactory.getLogger(DruidNavigator.class);
    private static final String COORDINATOR_TABLES_PATH = "/datasources/";
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
        }

        return tableConfigurations;
    }

    /**
     * Queries druid for all datasources and loads all of the discovered tables.
     *
     * The expected response is:
     * ["wikiticker"]
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
        }, COORDINATOR_TABLES_PATH);
    }

    /**
     * Load a specific table with all reported metrics, dimensions, and timegrains.
     * The schema from the first segment in druid's response is used for configuration.
     * The expected response is:
     * {
     *      ...
     *      "segments": [
     *          {
     *              "dataSource": "wikiticker",
     *              "interval": "2015-09-12T00:00:00.000Z/2015-09-13T00:00:00.000Z",
     *              ...,
     *              "dimensions": "channel,cityName,comment,...",
     *              "metrics": "count,added,deleted,delta,user_unique",
     *              ...
     *          }
     *      ]
     * }
     *
     * @param table The TableConfig to be loaded with queries against druid.
     */
    private void loadTable(TableConfig table) {
        String url = COORDINATOR_TABLES_PATH + table.getName() + "/?full";
        String segmentsPath = "segments";
        queryDruid(rootNode -> {
            if (rootNode.get(segmentsPath).size() == 0) {
                LOG.error("The segments list returned from {} was empty.", url);
                throw new RuntimeException("Can't configure table without segment data.");
            }
            JsonNode segments = rootNode.get(segmentsPath).get(0);
            loadMetrics(table, segments);
            loadDimensions(table, segments);
            loadTimeGrains(table, segments);
            LOG.debug("Loaded table " + table.getName());
        }, url);
    }

    /**
     * Add all metrics from druid query to the {@link TableConfig}.
     *
     * The expected response is:
     * {
     *      ...,
     *      "metrics": "count,added,deleted,delta,user_unique",
     *      ...
     * }
     *
     * @param table  The TableConfig to be loaded.
     * @param segmentJson The JsonNode containing a list of metrics.
     */
    private void loadMetrics(TableConfig table, JsonNode segmentJson) {
        JsonNode metricsArray = segmentJson.get("metrics");
        Arrays.asList(metricsArray.asText().split(",")).forEach(table::addMetric);
        LOG.debug("loaded metrics {}", table.getMetrics());
    }

    /**
     * Add all dimensions from druid query to the {@link TableConfig}.
     *
     * The expected response is:
     * {
     *      ...,
     *      "dimensions": "channel,cityName,comment,...",
     *      ...
     * }
     *
     * @param table  The TableConfig to be loaded.
     * @param segmentJson The JsonNode containing a list of dimensions.
     */
    private void loadDimensions(TableConfig table, JsonNode segmentJson) {
        JsonNode dimensionsArray = segmentJson.get("dimensions");
        Arrays.asList(dimensionsArray.asText().split(",")).forEach(table::addDimension);
        LOG.debug("loaded dimensions {}", table.getDimensions());
    }

    /**
     * Find a valid timegrain from druid query to the {@link TableConfig}.
     *
     * {
     *      ...,
     *      "interval": "2015-09-12T00:00:00.000Z/2015-09-13T00:00:00.000Z",
     *      ...
     * }
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
                Interval interval = new Interval(start.toInstant(), end.toInstant());
                timeGrain = IntervalUtils.getTimeGrain(interval);
            }
        } catch (IllegalArgumentException ignored) {
            LOG.warn("Unable to parse time intervals {} correctly", Arrays.toString(utcTimes));
        }

        if (!timeGrain.isPresent()) {
            LOG.warn("Couldn't detect timegrain for {}, defaulting to DAY TimeGrain.", timeInterval.asText());
        }
        tableConfig.setTimeGrain(timeGrain.orElse(DefaultTimeGrain.DAY));
    }

    /**
     * Send a blocking request to druid. All queries are finished before continuing.
     *
     * @param successCallback  The callback to be done if the query succeeds.
     * @param url  The url to send the query to.
     */
    private void queryDruid(SuccessCallback successCallback, String url) {
        LOG.debug("Fetching " + url);
        Future<Response> responseFuture = druidWebService.getJsonObject(
                rootNode -> {
                    LOG.debug("Succesfully fetched " + url);
                    successCallback.invoke(rootNode);
                },
                (statusCode, reasonPhrase, responseBody) -> {
                    LOG.error("HTTPError {} - {} for {}", statusCode, reasonPhrase, url);
                    throw new RuntimeException("HTTPError " + statusCode + " occurred while contacting druid");
                },
                (throwable) -> {
                    LOG.error("Error thrown while fetching " + url + ". " + throwable.getMessage());
                    throw new RuntimeException(throwable);
                },
                url
        );

        try {
            //calling get so we wait until responses are loaded before returning and processing continues
            responseFuture.get(30, TimeUnit.SECONDS);
        } catch (TimeoutException | InterruptedException | ExecutionException e) {
            LOG.error("Interrupted while waiting for a response from druid", e);
            throw new RuntimeException("Unable to automatically configure correctly, no response from druid.", e);
        }
    }
}
