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
import com.yahoo.bard.webservice.table.LogicalTableDictionary;
import com.yahoo.bard.webservice.table.PhysicalTableDictionary;

import org.apache.lucene.search.BooleanQuery;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

/**
 * This class acts as the topmost factory for all the configuration objects in the system.
 *  It uses the load method on each of the dimension loaders with which is it constructed to supply the system
 *  with configured tables, metrics, and dimensions.
 **
 * Once {@link #load()} is called, all the dictionaries in this object should be populated and ready to use.
 */
public class DefaultConfigurationLoader implements ConfigurationLoader {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultConfigurationLoader.class);
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    protected final ResourceDictionaries dictionaries = new ResourceDictionaries();

    protected final DimensionLoader dimensionLoader;
    protected final TableLoader tableLoader;
    protected final MetricLoader metricLoader;

    // Default JodaTime zone to UTC
    private static final String TIMEZONE = SYSTEM_CONFIG.getStringProperty(
            SYSTEM_CONFIG.getPackageVariableName("timezone"),
            "UTC"
    );

    /**
     * Constructor.
     *
     * @param dimensionLoader  DimensionLoader to load dimensions from
     * @param metricLoader  MetricLoader to load metrics from
     * @param tableLoader  TableLoader to load tables from
     */
    @Inject
    public DefaultConfigurationLoader(
            DimensionLoader dimensionLoader,
            MetricLoader metricLoader,
            TableLoader tableLoader
    ) {
        DateTimeZone.setDefault(DateTimeZone.forID(TIMEZONE));

        // Set the max lucene query clauses as high as it can go
        BooleanQuery.setMaxClauseCount(Integer.MAX_VALUE);

        this.dimensionLoader = dimensionLoader;
        this.metricLoader = metricLoader;
        this.tableLoader = tableLoader;
    }

    /**
     * Load the Dimensions, Metrics, and Tables.
     */
    @Override
    public void load() {
        dimensionLoader.loadDimensionDictionary(dictionaries.getDimensionDictionary());
        // metric loader might dependent on dimension dictionary, so load dimension first
        metricLoader.loadMetricDictionary(dictionaries.getMetricDictionary(), dictionaries.getDimensionDictionary());
        tableLoader.loadTableDictionary(dictionaries);

        LOG.info("Initialized ConfigurationLoader");
        LOG.info(dictionaries.toString());
    }

    @Override
    public DimensionDictionary getDimensionDictionary() {
        return dictionaries.getDimensionDictionary();
    }

    @Override
    public MetricDictionary getMetricDictionary() {
        return dictionaries.getMetricDictionary();
    }

    @Override
    public LogicalTableDictionary getLogicalTableDictionary() {
        return dictionaries.getLogicalDictionary();
    }

    @Override
    public PhysicalTableDictionary getPhysicalTableDictionary() {
        return dictionaries.getPhysicalDictionary();
    }

    @Override
    public ResourceDictionaries getDictionaries() {
        return dictionaries;
    }
}
