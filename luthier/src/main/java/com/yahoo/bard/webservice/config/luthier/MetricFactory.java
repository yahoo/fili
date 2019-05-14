package com.yahoo.bard.webservice.config.luthier;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Map;

/**
 * A factory method to create LogicalMetrics using Json config and the LuthierIndustrialPark to resolve dependencies.
 */
public interface MetricFactory {
    LogicalMetric build(String name, Map<String, JsonNode> configTable, LuthierIndustrialPark resourceFactories);
}
