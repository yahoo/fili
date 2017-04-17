// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
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
import static com.yahoo.bard.webservice.data.config.names.TestDruidTableName.COLOR_SHAPES_HOURLY;
import static com.yahoo.bard.webservice.data.config.names.TestDruidTableName.COLOR_SHAPES_MONTHLY;
import static com.yahoo.bard.webservice.data.config.names.TestDruidTableName.COLOR_SIZE_SHAPES;
import static com.yahoo.bard.webservice.data.config.names.TestDruidTableName.COLOR_SIZE_SHAPE_SHAPES;
import static com.yahoo.bard.webservice.data.config.names.TestDruidTableName.HOURLY;
import static com.yahoo.bard.webservice.data.config.names.TestDruidTableName.MONTHLY;
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY;
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.HOUR;
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.MONTH;

import com.yahoo.bard.webservice.data.config.dimension.TestDimensions;
import com.yahoo.bard.webservice.data.config.names.FieldName;
import com.yahoo.bard.webservice.data.config.names.TestDruidMetricName;
import com.yahoo.bard.webservice.util.Utils;

import org.joda.time.DateTimeZone;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utilities for constructing test sets of PhysicalTableDefinitions.
 */
public class TestPhysicalTableDefinitionUtils {
    /**
     * Build the hourly table definitions.
     *
     * @param testDimensions  Dimensions to build the tables with
     * @param metricNames  The field name of metrics to build with
     *
     * @return the hourly table definitions
     */
    public static LinkedHashSet<PhysicalTableDefinition> buildHourlyTableDefinitions(
            TestDimensions testDimensions,
            Set<FieldName> metricNames
    ) {
        return Utils.asLinkedHashSet(
                new ConcretePhysicalTableDefinition(
                        HOURLY,
                        HOUR.buildZonedTimeGrain(DateTimeZone.UTC),
                        new LinkedHashSet<>(Arrays.asList(TestDruidMetricName.values())),
                        testDimensions.getDimensionConfigurationsByApiName(OTHER)
                )
        );
    }

    /**
     * Build the monthly table definitions.
     *
     * @param testDimensions  Dimensions to build the tables with
     * @param metricNames  The field name of metrics to build with
     *
     * @return the monthly table definitions
     */
    public static LinkedHashSet<PhysicalTableDefinition> buildMonthlyTableDefinitions(
            TestDimensions testDimensions,
            Set<FieldName> metricNames
    ) {
        return Utils.asLinkedHashSet(
                new ConcretePhysicalTableDefinition(
                        MONTHLY,
                        MONTH.buildZonedTimeGrain(DateTimeZone.UTC),
                        new LinkedHashSet<>(Arrays.asList(TestDruidMetricName.values())),
                        testDimensions.getDimensionConfigurationsByApiName(OTHER)
                )
        );
    }

    /**
     * Build hourly monthly table definitions.
     *
     * @param testDimensions  Dimensions to load in the tables
     * @param metricNames  The field name of metrics to build with
     *
     * @return the hourly monthly table definitions
     */
    public static LinkedHashSet<PhysicalTableDefinition> buildHourlyMonthlyTableDefinitions(
            TestDimensions testDimensions,
            Set<FieldName> metricNames
    ) {
        return Stream.concat(
                buildHourlyTableDefinitions(testDimensions, metricNames).stream(),
                buildMonthlyTableDefinitions(testDimensions, metricNames).stream()
        ).collect(Collectors.toCollection(LinkedHashSet::new));
    }


    /**
     * Build the pet table definitions.
     *
     * @param testDimensions  Dimensions to build the tables with
     * @param metricNames  The field name of metrics to build with
     *
     * @return the pet table definitions
     */
    public static LinkedHashSet<PhysicalTableDefinition> buildPetTableDefinitions(
            TestDimensions testDimensions,
            Set<FieldName> metricNames
    ) {
        return Utils.asLinkedHashSet(
                new ConcretePhysicalTableDefinition(
                        ALL_PETS,
                        DAY.buildZonedTimeGrain(DateTimeZone.UTC),
                        metricNames,
                        testDimensions.getDimensionConfigurationsByApiName(BREED, SEX, SPECIES)
                )
        );
    }

    /**
     * Build the shape table definitions.
     *
     * @param testDimensions  Dimensions to build the tables with
     * @param metricNames  The field name of metrics to build with
     *
     * @return the shape table definitions
     */
    public static LinkedHashSet<PhysicalTableDefinition> buildShapeTableDefinitions(
            TestDimensions testDimensions,
            Set<FieldName> metricNames
    ) {
        return Utils.asLinkedHashSet(
                new ConcretePhysicalTableDefinition(
                        ALL_SHAPES,
                        DAY.buildZonedTimeGrain(DateTimeZone.UTC),
                        metricNames,
                        testDimensions.getDimensionConfigurationsByApiName(COLOR, SIZE, SHAPE, OTHER, MODEL)
                ),
                new ConcretePhysicalTableDefinition(
                        COLOR_SHAPES_HOURLY,
                        HOUR.buildZonedTimeGrain(DateTimeZone.UTC),
                        metricNames,
                        testDimensions.getDimensionConfigurationsByApiName(COLOR)
                ),
                new ConcretePhysicalTableDefinition(
                        COLOR_SHAPES,
                        DAY.buildZonedTimeGrain(DateTimeZone.UTC),
                        metricNames,
                        testDimensions.getDimensionConfigurationsByApiName(COLOR)
                ),
                new ConcretePhysicalTableDefinition(
                        COLOR_SHAPES_MONTHLY,
                        MONTH.buildZonedTimeGrain(DateTimeZone.UTC),
                        metricNames,
                        testDimensions.getDimensionConfigurationsByApiName(COLOR)
                ),
                new ConcretePhysicalTableDefinition(
                        COLOR_SIZE_SHAPES,
                        DAY.buildZonedTimeGrain(DateTimeZone.UTC),
                        metricNames,
                        testDimensions.getDimensionConfigurationsByApiName(COLOR, SIZE)
                ),
                new ConcretePhysicalTableDefinition(
                        COLOR_SIZE_SHAPE_SHAPES,
                        DAY.buildZonedTimeGrain(DateTimeZone.UTC),
                        metricNames,
                        testDimensions.getDimensionConfigurationsByApiName(COLOR, SIZE, SHAPE)
                )
        );
    }
}
