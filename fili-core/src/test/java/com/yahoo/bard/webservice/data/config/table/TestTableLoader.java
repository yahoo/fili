// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.table;

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY;
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.HOUR;
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.MONTH;
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.WEEK;

import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.data.config.dimension.TestDimensions;
import com.yahoo.bard.webservice.data.config.names.TestApiMetricName;
import com.yahoo.bard.webservice.data.config.names.TestDruidMetricName;
import com.yahoo.bard.webservice.data.config.names.TestLogicalTableName;
import com.yahoo.bard.webservice.druid.model.query.AllGranularity;
import com.yahoo.bard.webservice.druid.model.query.Granularity;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService;
import com.yahoo.bard.webservice.table.TableGroup;
import com.yahoo.bard.webservice.util.Utils;

import org.joda.time.DateTimeZone;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Load the test-specific table configuration.
 */
public class TestTableLoader extends BaseTableLoader {

    private final Set<Granularity> validGrains = Utils.asLinkedHashSet(DAY, WEEK, MONTH);

    private Map<TestLogicalTableName, Set<PhysicalTableDefinition>> logicalTableTableDefinitions;

    /**
     * Constructor.
     * <p>
     * Uses a new TestDimension and sets the timezone to UTC.
     *
     * @param metadataService  Service containing the segment data for constructing tables
     */
    public TestTableLoader(DataSourceMetadataService metadataService) {
        this(new TestDimensions(), DateTimeZone.UTC, metadataService);
    }

    /**
     * Constructor.
     *
     * @param testDimensions  Dimensions to use when loading the tables
     * @param defaultTimeZone  Timezone to default the tables to
     * @param metadataService  Service containing the segment data for constructing tables
     */
    public TestTableLoader(
            TestDimensions testDimensions,
            DateTimeZone defaultTimeZone,
            DataSourceMetadataService metadataService
    ) {
        super(defaultTimeZone, metadataService);
        logicalTableTableDefinitions = new HashMap<>();
        // Set up the table definitions
        logicalTableTableDefinitions.put(
                TestLogicalTableName.SHAPES,
                TestPhysicalTableDefinitionUtils.buildShapeTableDefinitions(
                        testDimensions,
                        TestDruidMetricName.getByLogicalTable(TestLogicalTableName.SHAPES)
                )
        );
        logicalTableTableDefinitions.put(
                TestLogicalTableName.PETS,
                TestPhysicalTableDefinitionUtils.buildPetTableDefinitions(
                        testDimensions,
                        TestDruidMetricName.getByLogicalTable(TestLogicalTableName.PETS)
                )
        );
        logicalTableTableDefinitions.put(
                TestLogicalTableName.MONTHLY,
                TestPhysicalTableDefinitionUtils.buildMonthlyTableDefinitions(
                        testDimensions,
                        TestDruidMetricName.getByLogicalTable(TestLogicalTableName.MONTHLY)
                )
        );
        logicalTableTableDefinitions.put(
                TestLogicalTableName.HOURLY,
                TestPhysicalTableDefinitionUtils.buildHourlyTableDefinitions(
                        testDimensions,
                        TestDruidMetricName.getByLogicalTable(TestLogicalTableName.HOURLY)
                )
        );
        logicalTableTableDefinitions.put(
                TestLogicalTableName.HOURLY_MONTHLY,
                TestPhysicalTableDefinitionUtils.buildHourlyMonthlyTableDefinitions(
                        testDimensions,
                        TestDruidMetricName.getByLogicalTable(TestLogicalTableName.HOURLY_MONTHLY)
                )
        );
    }

    @Override
    public void loadTableDictionary(ResourceDictionaries dictionaries) {
        Map<String, TableGroup> logicalTableTableGroup = new LinkedHashMap<>();
        for (TestLogicalTableName logicalTableName : TestLogicalTableName.values()) {
            TableGroup tableGroup = buildDimensionSpanningTableGroup(
                    TestApiMetricName.getByLogicalTable(logicalTableName),
                    logicalTableTableDefinitions.get(logicalTableName),
                    dictionaries
            );
            logicalTableTableGroup.put(logicalTableName.asName(), tableGroup);
        }
        validGrains.add(AllGranularity.INSTANCE);

        loadLogicalTablesWithGranularities(logicalTableTableGroup, validGrains, dictionaries);

        Map<String, TableGroup> hourlyGroup = new HashMap<>();
        hourlyGroup.put(
                TestLogicalTableName.HOURLY.asName(),
                logicalTableTableGroup.get(TestLogicalTableName.HOURLY.asName())
        );
        loadLogicalTablesWithGranularities(hourlyGroup, Utils.asLinkedHashSet(HOUR), dictionaries);

        Map<String, TableGroup> hourlyMonthlyGroup = new HashMap<>();
        hourlyMonthlyGroup.put(
                TestLogicalTableName.HOURLY_MONTHLY.asName(),
                logicalTableTableGroup.get(TestLogicalTableName.HOURLY_MONTHLY.asName())
        );
        loadLogicalTablesWithGranularities(hourlyMonthlyGroup, Utils.asLinkedHashSet(HOUR, MONTH), dictionaries);
    }
}
