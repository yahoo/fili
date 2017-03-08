// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.auto;

import com.yahoo.bard.webservice.data.time.DefaultTimeGrain;
import com.yahoo.bard.webservice.data.time.TimeGrain;
import com.yahoo.bard.webservice.druid.client.DruidWebService;
import com.yahoo.bard.webservice.druid.client.SuccessCallback;

import com.fasterxml.jackson.databind.JsonNode;

import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by kevin on 2/28/2017.
 */
public class DruidNavigator implements ConfigLoader {
    private static final int COORDINATOR_PORT = 8081;
    private static final String COORDINATOR_BASE = "http://localhost:" + COORDINATOR_PORT + "/druid/coordinator/v1/";
    private static final Logger LOG = LoggerFactory.getLogger(DruidNavigator.class);
    private DruidWebService druidWebService;
    private List<TableConfig> tableConfigurations;

    public DruidNavigator(DruidWebService druidWebService) {
        this.druidWebService = druidWebService;
        tableConfigurations = new ArrayList<>();
    }

    @Override
    public List<? extends DruidConfig> getTableNames() {
        String url = COORDINATOR_BASE + "datasources/";
        getJson(rootNode -> {
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

    public void loadTable(TableConfig table) {
        String url = COORDINATOR_BASE + "datasources/" + table.getName() + "/?full";
        getJson(rootNode -> {
            JsonNode segments = rootNode.get("segments").get(0);
            loadMetrics(table, segments);
            loadDimensions(table, segments);
            loadTimeGrains(table, segments);
            LOG.info("Loaded table " + table.getName());
        }, url);
    }

    private void loadMetrics(final TableConfig table, final JsonNode rootNode) {
        JsonNode metricsArray = rootNode.get("metrics");
        String[] metrics = metricsArray.asText().split(",");
        for (String m : metrics) {
            table.addMetric(m);
        }
    }

    private void loadDimensions(final TableConfig tableName, final JsonNode rootNode) {
        JsonNode dimensionsArray = rootNode.get("dimensions");
        String[] dimensions = dimensionsArray.asText().split(",");
        for (String d : dimensions) {
            tableName.addDimension(d);
        }
    }

    private void loadTimeGrains(final TableConfig tableConfig, final JsonNode rootNode) {
        JsonNode timeInterval = rootNode.get("interval");
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

    //TODO check for " Minute must start and end on the 1st second of a minute." compliance
    private TimeGrain getTimeGrain(DateTime start, DateTime end) {
        Duration diff = new Duration(start, end);
        if (diff.getStandardMinutes() == 1) {
            return DefaultTimeGrain.MINUTE;
        } else if (diff.getStandardHours() == 1) {
            return DefaultTimeGrain.HOUR;
        } else if (diff.getStandardDays() == 1) {
            return DefaultTimeGrain.DAY;
        } else if (diff.getStandardDays() == 7) {
            return DefaultTimeGrain.WEEK;
        } else if (start.getMonthOfYear() + 1 == end.getMonthOfYear() && start.getDayOfMonth() == 1 && end
                .getDayOfMonth() == 1) {
            return DefaultTimeGrain.MONTH;
        } else if (/* detect if quarter */ false) { //TODO detect quarterly
            return DefaultTimeGrain.QUARTER;
        } else if (start.getYear() + 1 == end.getYear() && start.getMonthOfYear() == DateTimeConstants.JANUARY &&
                start.getDayOfMonth() == 1 && end
                .getMonthOfYear() == DateTimeConstants.JANUARY && end.getDayOfMonth() == 1) {
            return DefaultTimeGrain.YEAR;
        }
        LOG.info("Couldn't detect default timegrain " + diff.getStandardDays() + " " + start.getHourOfDay());
        return null;
    }

    //TODO: handle errors
    private void getJson(SuccessCallback successCallback, String url) {
        LOG.debug("Fetching " + url);
        druidWebService.getJsonObject(
                rootNode -> {
                    LOG.debug("Succesfully fetched " + url);
                    successCallback.invoke(rootNode);
                },
                (statusCode, reasonPhrase, responseBody) -> {
                    LOG.info("HTTPError " + statusCode + " - " + reasonPhrase);
                },
                (throwable) -> {
                    LOG.info("Error thrown while fetching " + url + ". " + throwable.getMessage());
                },
                url
        );
    }
}
