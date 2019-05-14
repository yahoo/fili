// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config.luthier;

import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Map;

/**
 * Dependency Injection container for Config Objects
 */
public class LuthierIndustrialPark {


    ResourceDictionaries resourceDictionaries;

    Map<String, DimensionFactory> dimensionFactories;

    public LuthierIndustrialPark(ResourceDictionaries resourceDictionaries) {
        this.resourceDictionaries = resourceDictionaries;
    }
/*
    LogicalTable getLogicalTable(String factoryName, String tableName, Map<String, JsonNode> configTable);
    PhysicalTable getPhysicalTable(String factoryName, String tableName, Map<String, JsonNode> configTable);
    LogicalMetric getLogicalMetric(String factoryName, String metricName, Map<String, JsonNode> configTable);
*/
    Dimension getDimension(String factoryName, String dimensionName, Map<String, JsonNode> configTable) {
        DimensionDictionary dimensionDictionary = resourceDictionaries.getDimensionDictionary();
        if (dimensionDictionary.findByApiName(dimensionName) != null) {
            return dimensionDictionary.findByApiName(dimensionName);
        }

        if (!dimensionFactories.containsKey(factoryName)) {
            throw new LuthierFactoryException("Missing factory named: foo, for dimension");
        }
        Dimension dimension = dimensionFactories.get(factoryName).build(dimensionName, configTable, this);
        resourceDictionaries.getDimensionDictionary().add(dimension);
        return dimension;
    };
/*
    SearchProvider getSearchProvider(String factoryName, Map<String, JsonNode> configTable);
    KeyValueStore getKeyValueStore(String factoryName, Map<String, JsonNode> configTable);
    */

}
