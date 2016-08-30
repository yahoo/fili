// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.table;

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY;
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.HOUR;

import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.names.ApiMetricName;
import com.yahoo.bard.webservice.data.config.names.FieldName;
import com.yahoo.bard.webservice.data.config.table.BaseTableLoader;
import com.yahoo.bard.webservice.data.config.table.PhysicalTableDefinition;
import com.yahoo.bard.webservice.druid.model.query.AllGranularity;
import com.yahoo.bard.webservice.druid.model.query.Granularity;
import com.yahoo.bard.webservice.table.TableGroup;
import com.yahoo.bard.webservice.util.Utils;
import com.yahoo.wiki.webservice.data.config.dimension.WikiDimensions;
import com.yahoo.wiki.webservice.data.config.names.WikiApiDimensionName;
import com.yahoo.wiki.webservice.data.config.names.WikiApiMetricName;
import com.yahoo.wiki.webservice.data.config.names.WikiDruidMetricName;
import com.yahoo.wiki.webservice.data.config.names.WikiDruidTableName;
import com.yahoo.wiki.webservice.data.config.names.WikiLogicalTableName;

import java.util.EnumMap;
import java.util.Map;
import java.util.Set;

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
     */
    public WikiTableLoader() {
        WikiDimensions wikiDimensions = new WikiDimensions();

        configureSample(wikiDimensions);
    }

    /**
     * Set up the tables for this table loader.
     *
     * @param wikiDimensions  The dimensions to load into test tables.
     */
    private void configureSample(WikiDimensions wikiDimensions) {

        // Dimensions
        Set<DimensionConfig> dimsBasefactDruidTableName = wikiDimensions.getDimensionConfigurationsByApiName(
                WikiApiDimensionName.values()
        );

        // Physical Tables
        Set<PhysicalTableDefinition> samplePhysicalTableDefinition = Utils.asLinkedHashSet(
                new PhysicalTableDefinition(
                        WikiDruidTableName.WIKIPEDIA,
                        HOUR,
                        dimsBasefactDruidTableName
                )
        );

        tableDefinitions.put(WikiLogicalTableName.WIKIPEDIA, samplePhysicalTableDefinition);

        druidMetricNames.put(
                WikiLogicalTableName.WIKIPEDIA,
                Utils.<FieldName>asLinkedHashSet(WikiDruidMetricName.values())
        );

        apiMetricNames.put(
                WikiLogicalTableName.WIKIPEDIA,
                Utils.<ApiMetricName>asLinkedHashSet(WikiApiMetricName.values())
        );

        validGrains.put(WikiLogicalTableName.WIKIPEDIA, Utils.asLinkedHashSet(HOUR, DAY, AllGranularity.INSTANCE));
    }

    @Override
    public void loadTableDictionary(ResourceDictionaries dictionaries) {
        for (WikiLogicalTableName table : WikiLogicalTableName.values()) {
            TableGroup tableGroup = buildTableGroup(
                    table.asName(),
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
