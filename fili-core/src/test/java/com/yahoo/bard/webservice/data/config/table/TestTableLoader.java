// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.table;

import static com.yahoo.bard.webservice.data.config.names.TestApiDimensionName.BREED;
import static com.yahoo.bard.webservice.data.config.names.TestApiDimensionName.COLOR;
import static com.yahoo.bard.webservice.data.config.names.TestApiDimensionName.MODEL;
import static com.yahoo.bard.webservice.data.config.names.TestApiDimensionName.OTHER;
import static com.yahoo.bard.webservice.data.config.names.TestApiDimensionName.SEX;
import static com.yahoo.bard.webservice.data.config.names.TestApiDimensionName.SHAPE;
import static com.yahoo.bard.webservice.data.config.names.TestApiDimensionName.SIZE;
import static com.yahoo.bard.webservice.data.config.names.TestApiDimensionName.SPECIES;
import static com.yahoo.bard.webservice.data.config.names.TestDruidTableName.ALL_PETS;
import static com.yahoo.bard.webservice.data.config.names.TestDruidTableName.ALL_SHAPES;
import static com.yahoo.bard.webservice.data.config.names.TestDruidTableName.COLOR_SHAPES;
import static com.yahoo.bard.webservice.data.config.names.TestDruidTableName.COLOR_SHAPES_MONTHLY;
import static com.yahoo.bard.webservice.data.config.names.TestDruidTableName.COLOR_SIZE_SHAPES;
import static com.yahoo.bard.webservice.data.config.names.TestDruidTableName.COLOR_SIZE_SHAPE_SHAPES;
import static com.yahoo.bard.webservice.data.config.names.TestDruidTableName.HOURLY;
import static com.yahoo.bard.webservice.data.config.names.TestDruidTableName.MONTHLY;
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
import com.yahoo.bard.webservice.table.TableGroup;
import com.yahoo.bard.webservice.util.Utils;

import org.joda.time.DateTimeZone;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Load the Digits-specific table configuration.
 */
public class TestTableLoader extends BaseTableLoader {

    private final Set<Granularity> validGrains = Utils.asLinkedHashSet(DAY, WEEK, MONTH);
    private final TestDimensions testDimensions;

    private Map<TestLogicalTableName, Set<PhysicalTableDefinition>> logicalTableTableDefinitions;


    public TestTableLoader() {
        this(new TestDimensions(), DateTimeZone.UTC);
    }

    public TestTableLoader(TestDimensions testDimensions, DateTimeZone defaultTimeZone) {
        super(defaultTimeZone);
        this.testDimensions = testDimensions;
        logicalTableTableDefinitions = new HashMap<>();
        // Set up the table definitions
        logicalTableTableDefinitions.put(TestLogicalTableName.SHAPES, buildShapeTableDefinitions());
        logicalTableTableDefinitions.put(TestLogicalTableName.PETS, buildPetTableDefinitions());
        logicalTableTableDefinitions.put(TestLogicalTableName.MONTHLY, buildMonthlyTableDefinitions());
        logicalTableTableDefinitions.put(TestLogicalTableName.HOURLY, buildHourlyTableDefinitions());
    }

    private LinkedHashSet<PhysicalTableDefinition> buildHourlyTableDefinitions() {
        return Utils.asLinkedHashSet(
                new PhysicalTableDefinition(
                        HOURLY,
                        HOUR,
                        testDimensions.getDimensionConfigurationsByApiName(OTHER)
                )
        );
    }

    private LinkedHashSet<PhysicalTableDefinition> buildMonthlyTableDefinitions() {
        return Utils.asLinkedHashSet(
                new PhysicalTableDefinition(
                        MONTHLY,
                        MONTH,
                        testDimensions.getDimensionConfigurationsByApiName(OTHER)
                )
        );
    }

    private LinkedHashSet<PhysicalTableDefinition> buildPetTableDefinitions() {
        return Utils.asLinkedHashSet(
                new PhysicalTableDefinition(
                        ALL_PETS,
                        DAY,
                        testDimensions.getDimensionConfigurationsByApiName(BREED, SEX, SPECIES)
                )
        );
    }

    private LinkedHashSet<PhysicalTableDefinition> buildShapeTableDefinitions() {
        return Utils.asLinkedHashSet(
                new PhysicalTableDefinition(
                        ALL_SHAPES,
                        DAY,
                        testDimensions.getDimensionConfigurationsByApiName(COLOR, SIZE, SHAPE, OTHER, MODEL)
                ),
                new PhysicalTableDefinition(
                        COLOR_SHAPES,
                        HOUR,
                        testDimensions.getDimensionConfigurationsByApiName(COLOR)
                ),
                new PhysicalTableDefinition(
                        COLOR_SHAPES,
                        DAY,
                        testDimensions.getDimensionConfigurationsByApiName(COLOR)
                ),
                new PhysicalTableDefinition(
                        COLOR_SHAPES_MONTHLY,
                        MONTH,
                        testDimensions.getDimensionConfigurationsByApiName(COLOR)
                ),
                new PhysicalTableDefinition(
                        COLOR_SIZE_SHAPES,
                        DAY,
                        testDimensions.getDimensionConfigurationsByApiName(COLOR, SIZE)
                ),
                new PhysicalTableDefinition(
                        COLOR_SIZE_SHAPE_SHAPES,
                        DAY,
                        testDimensions.getDimensionConfigurationsByApiName(COLOR, SIZE, SHAPE)
                )
        );
    }

    @Override
    public void loadTableDictionary(ResourceDictionaries dictionaries) {
        Map<String, TableGroup> logicalTableTableGroup = new HashMap<>();
        for (TestLogicalTableName logicalTableName : TestLogicalTableName.values()) {
            TableGroup tableGroup = buildTableGroup(
                    logicalTableName.asName(),
                    TestApiMetricName.getByLogicalTable(logicalTableName),
                    TestDruidMetricName.getByLogicalTable(logicalTableName),
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
    }
}
