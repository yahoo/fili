package com.yahoo.wiki.webservice.data.config.auto;

import com.yahoo.bard.webservice.druid.client.DruidWebService;
import com.yahoo.bard.webservice.druid.client.SuccessCallback;

import com.fasterxml.jackson.databind.JsonNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by kevin on 2/28/2017.
 */
public class DruidNavigator implements ConfigLoader {
    private static final int COORDINATOR_PORT = 8081;
    private static final Logger LOG = LoggerFactory.getLogger(DruidNavigator.class);
    private DruidWebService druidWebService;
    private List<TableConfig> tableConfigurations;

    public DruidNavigator(DruidWebService druidWebService) {
        this.druidWebService = druidWebService;
        tableConfigurations = new ArrayList<>();
    }

    public List<TableConfig> getAllLoadedTables() {
        for (TableConfig tableConfig : getTableNames()) {
            loadTable(tableConfig);
        }
        return tableConfigurations;
    }

    public List<TableConfig> getTableNames() {
        String url = "http://localhost:" + COORDINATOR_PORT + "/druid/coordinator/v1/datasources/";
        getJson(rootNode -> {
            if (rootNode.isArray()) {
                for (final JsonNode objNode : rootNode) {
                    tableConfigurations.add(new TableConfig(objNode.asText()));
                }
            }
        }, url);

        return tableConfigurations;
    }

    public void loadTable(TableConfig table) {
        String url = "http://localhost:" + COORDINATOR_PORT + "/druid/coordinator/v1/datasources/" + table
                .getName() + "/?full";
        getJson(rootNode -> {
            JsonNode segments = rootNode.get("segments").get(0);
            loadMetrics(table, segments);
            loadDimensions(table, segments);
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
