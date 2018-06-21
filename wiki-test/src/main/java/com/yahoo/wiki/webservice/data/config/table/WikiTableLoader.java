// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.table;

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY;
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.HOUR;

import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.names.ApiMetricName;
import com.yahoo.bard.webservice.data.config.names.FieldName;
import com.yahoo.bard.webservice.data.config.table.BaseTableLoader;
import com.yahoo.bard.webservice.data.config.table.ConcretePhysicalTableDefinition;
import com.yahoo.bard.webservice.data.config.table.PhysicalTableDefinition;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.time.AllGranularity;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService;
import com.yahoo.bard.webservice.table.TableGroup;
import com.yahoo.bard.webservice.util.Utils;
import com.yahoo.wiki.webservice.data.config.dimension.WikiDimensionsLoader;
import com.yahoo.wiki.webservice.data.config.names.WikiDruidTableName;
import com.yahoo.wiki.webservice.data.config.names.WikiLogicalTableName;

import org.joda.time.DateTimeZone;

import java.util.*;

/**
 * Load the Wikipedia-specific table configuration.
 */
public class WikiTableLoader extends BaseTableLoader {

    private final Map<WikiLogicalTableName, Set<Granularity>> validGrains =
            new EnumMap<>(WikiLogicalTableName.class);

    // Set up the metrics
    private final Map<WikiLogicalTableName, Set<FieldName>> druidMetricNames =
            new EnumMap<>(WikiLogicalTableName.class);
    private final Map<WikiLogicalTableName, Set<ApiMetricName>> apiMetricNames =
            new EnumMap<>(WikiLogicalTableName.class);

    // Set up the table definitions
    private final Map<WikiLogicalTableName, Set<PhysicalTableDefinition>> tableDefinitions =
            new EnumMap<>(WikiLogicalTableName.class);

    /**
     * Constructor.
     *
     * @param metadataService  Service containing the segment data for constructing tables
     */
    public WikiTableLoader(DataSourceMetadataService metadataService) {
        super(metadataService);
    }

    /**
     * Set up the tables for this table loader.
     *
     * @param wikiDimensions  The dimensions to load into test tables.
     * @param metricDictionary  The dictionary to use when looking up metrics for this table
     */
    private void configureSample(WikiDimensionsLoader wikiDimensions, MetricDictionary metricDictionary) {

        // Dimensions
        Set<DimensionConfig> dimsBasefactDruidTableName = wikiDimensions.getDimensionConfigurationsByConfigInfo();
        LinkedHashSet<FieldName> druidMetrics = new LinkedHashSet<>();
        LinkedHashSet<ApiMetricName> apiMetrics = new LinkedHashSet<>();

        for (LogicalMetric metric : metricDictionary.values()) {
            druidMetrics.add(metric);
            apiMetrics.add(metric);
        }

        druidMetricNames.put(
                WikiLogicalTableName.WIKIPEDIA,
                druidMetrics
        );

        apiMetricNames.put(
                WikiLogicalTableName.WIKIPEDIA,
                apiMetrics
        );

        // Physical Tables
        Set<PhysicalTableDefinition> samplePhysicalTableDefinition = Utils.asLinkedHashSet(
                new ConcretePhysicalTableDefinition(
                        WikiDruidTableName.WIKITICKER,
                        HOUR.buildZonedTimeGrain(DateTimeZone.UTC),
                        druidMetricNames.get(WikiLogicalTableName.WIKIPEDIA),
                        dimsBasefactDruidTableName
                )
        );

        tableDefinitions.put(WikiLogicalTableName.WIKIPEDIA, samplePhysicalTableDefinition);

        validGrains.put(WikiLogicalTableName.WIKIPEDIA, Utils.asLinkedHashSet(HOUR, DAY, AllGranularity.INSTANCE));
    }

    @Override
    public void loadTableDictionary(ResourceDictionaries dictionaries) {

        configureSample(new WikiDimensionsLoader(), dictionaries.metric);

        for (WikiLogicalTableName table : WikiLogicalTableName.values()) {
            TableGroup tableGroup = buildDimensionSpanningTableGroup(
                    apiMetricNames.get(table),
                    druidMetricNames.get(table),
                    tableDefinitions.get(table),
                    dictionaries
            );
            Set<Granularity> validGranularities = validGrains.get(table);
            loadLogicalTableWithGranularities(table.asName(), tableGroup, validGranularities, dictionaries);
        }
    }
}
