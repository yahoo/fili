// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.table;

import static com.yahoo.fili.webservice.data.time.DefaultTimeGrain.DAY;
import static com.yahoo.fili.webservice.data.time.DefaultTimeGrain.HOUR;

import com.yahoo.fili.webservice.data.config.ResourceDictionaries;
import com.yahoo.fili.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.fili.webservice.data.config.names.ApiMetricName;
import com.yahoo.fili.webservice.data.config.names.FieldName;
import com.yahoo.fili.webservice.data.config.table.BaseTableLoader;
import com.yahoo.fili.webservice.data.config.table.ConcretePhysicalTableDefinition;
import com.yahoo.fili.webservice.data.config.table.PhysicalTableDefinition;
import com.yahoo.fili.webservice.druid.model.query.AllGranularity;
import com.yahoo.fili.webservice.druid.model.query.Granularity;
import com.yahoo.fili.webservice.metadata.DataSourceMetadataService;
import com.yahoo.fili.webservice.table.TableGroup;
import com.yahoo.fili.webservice.util.Utils;
import com.yahoo.wiki.webservice.data.config.dimension.WikiDimensions;
import com.yahoo.wiki.webservice.data.config.names.WikiApiDimensionConfigInfo;
import com.yahoo.wiki.webservice.data.config.names.WikiApiMetricName;
import com.yahoo.wiki.webservice.data.config.names.WikiDruidMetricName;
import com.yahoo.wiki.webservice.data.config.names.WikiDruidTableName;
import com.yahoo.wiki.webservice.data.config.names.WikiLogicalTableName;

import org.joda.time.DateTimeZone;

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
     *
     * @param metadataService  Service containing the segment data for constructing tables
     */
    public WikiTableLoader(DataSourceMetadataService metadataService) {
        super(metadataService);

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
        Set<DimensionConfig> dimsBasefactDruidTableName = wikiDimensions.getDimensionConfigurationsByConfigInfo(
            WikiApiDimensionConfigInfo.values()
        );

        druidMetricNames.put(
                WikiLogicalTableName.WIKIPEDIA,
                Utils.<FieldName>asLinkedHashSet(WikiDruidMetricName.values())
        );

        apiMetricNames.put(
                WikiLogicalTableName.WIKIPEDIA,
                Utils.<ApiMetricName>asLinkedHashSet(WikiApiMetricName.values())
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
