// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.luthier;

import com.yahoo.bard.webservice.data.config.luthier.table.LogicalTableGroup;
import com.yahoo.bard.webservice.data.config.metric.makers.MetricMaker;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.KeyValueStore;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.table.ConfigPhysicalTable;

/**
 * Concepts represent the categories of things that have configuration files in Luthier, as well as the corresponding
 * factories for producing those types of objects.
 *
 * This is not an enum, because we don't want to block extending concepts beyond code.
 *
 * This is not an interface, because it doesn't need to be yet.
 *
 * @param <T> The type within the config system, e.g. Dimension, LogicalTable, SearchProvider
 */
// TODO Make an interface for this
public class ConceptType<T> {

    public static final ConceptType<Dimension> DIMENSION = new ConceptType<>("dimension", "DimensionConfig");

    public static final ConceptType<SearchProvider> SEARCH_PROVIDER = new ConceptType<>(
            "searchProvider",
            "SearchProviderConfig"
    );

    public static final ConceptType<KeyValueStore> KEY_VALUE_STORE = new ConceptType<>(
            "keyValueStore",
            "KeyValueStoreConfig"
    );

    public static final ConceptType<LogicalMetric> METRIC = new ConceptType<>("metric", "MetricConfig");

    public static final ConceptType<MetricMaker> METRIC_MAKER = new ConceptType<>(
            "metricMaker",
            "MetricMakerConfig"
    );

    public static final ConceptType<ConfigPhysicalTable> PHYSICAL_TABLE = new ConceptType<>(
            "physicalTable",
            "PhysicalTableConfig"
    );

    public static final ConceptType<LogicalTableGroup> LOGICAL_TABLE_GROUP = new ConceptType<>(
            "logicalTableGroup",
            "LogicalTableConfig"
    );

    String conceptKey;

    String resourceName;

    /**
     * Constructor.
     *
     * @param name  Internal name for the concept.
     * @param resourceName  Name of the expected resource for this concept.
     */
    public ConceptType(String name, String resourceName) {
        this.conceptKey = name;
        this.resourceName = resourceName;
    }

    public String getConceptKey() {
        return conceptKey;
    }

    public String getResourceName() {
        return resourceName;
    }
}
