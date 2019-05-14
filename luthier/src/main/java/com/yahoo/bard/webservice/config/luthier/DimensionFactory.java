package com.yahoo.bard.webservice.config.luthier;

import com.yahoo.bard.webservice.data.dimension.Dimension;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Map;

/**
 * A factory method to create Dimension using Json config and the LuthierIndustrialPark to resolve dependencies.
 */
public interface DimensionFactory {
    Dimension build(String name, Map<String, JsonNode> configTable, LuthierIndustrialPark resourceFactories);
}
