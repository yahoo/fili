// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.data.config.dimension.DimensionLoader;
import com.yahoo.bard.webservice.data.config.metric.MetricLoader;
import com.yahoo.bard.webservice.data.config.table.TableLoader;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService;
import com.yahoo.bard.webservice.table.LogicalTableDictionary;
import com.yahoo.bard.webservice.table.PhysicalTableDictionary;

import org.apache.lucene.search.BooleanQuery;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

/**
 * Connects the resource dictionaries with the loaders.
 */
public class ConfigurationLoader {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigurationLoader.class);
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    protected final ResourceDictionaries dictionaries = new ResourceDictionaries();

    protected final DimensionLoader dimensionLoader;
    protected final TableLoader tableLoader;
    protected final MetricLoader metricLoader;

    protected final DataSourceMetadataService metadataService;

    // Default JodaTime zone to UTC
    public static final String TIMEZONE = SYSTEM_CONFIG.getStringProperty(
            SYSTEM_CONFIG.getPackageVariableName("timezone"),
            "UTC"
    );

    /**
     * Constructor.
     *
     * @param dimensionLoader  DimensionLoader to load dimensions from
     * @param metricLoader  MetricLoader to load metrics from
     * @param tableLoader  TableLoader to load tables from
     * @param metadataService datasource metadata service containing segments for building table
     */
    @Inject
    public ConfigurationLoader(
            DimensionLoader dimensionLoader,
            MetricLoader metricLoader,
            TableLoader tableLoader,
            DataSourceMetadataService metadataService
    ) {
        DateTimeZone.setDefault(DateTimeZone.forID(TIMEZONE));

        // Set the max lucene query clauses as high as it can go
        BooleanQuery.setMaxClauseCount(Integer.MAX_VALUE);

        this.dimensionLoader = dimensionLoader;
        this.metricLoader = metricLoader;
        this.tableLoader = tableLoader;
        this.metadataService = metadataService;
    }

    /**
     * Load the Dimensions, Metrics, and Tables.
     */
    public void load() {
        dimensionLoader.loadDimensionDictionary(dictionaries.getDimensionDictionary());
        metricLoader.loadMetricDictionary(dictionaries.getMetricDictionary());
        tableLoader.loadTableDictionary(dictionaries, metadataService);

        LOG.info("Initialized ConfigurationLoader");
        LOG.info(dictionaries.toString());
    }

    public DimensionDictionary getDimensionDictionary() {
        return dictionaries.getDimensionDictionary();
    }

    public MetricDictionary getMetricDictionary() {
        return dictionaries.getMetricDictionary();
    }

    public LogicalTableDictionary getLogicalTableDictionary() {
        return dictionaries.getLogicalDictionary();
    }

    public PhysicalTableDictionary getPhysicalTableDictionary() {
        return dictionaries.getPhysicalDictionary();
    }

    public ResourceDictionaries getDictionaries() {
        return dictionaries;
    }
}
