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
import com.yahoo.bard.webservice.util.Utils;

import org.joda.time.DateTimeZone;

import java.util.LinkedHashSet;
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
     *
     * @return the hourly table definitions
     */
    public static LinkedHashSet<PhysicalTableDefinition> buildHourlyTableDefinitions(TestDimensions testDimensions) {
        return Utils.asLinkedHashSet(
                new PhysicalTableDefinition(
                        HOURLY,
                        HOUR.buildZonedTimeGrain(DateTimeZone.UTC),
                        testDimensions.getDimensionConfigurationsByApiName(OTHER)
                )
        );
    }

    /**
     * Build the monthly table definitions.
     *
     * @param testDimensions  Dimensions to build the tables with
     *
     * @return the monthly table definitions
     */
    public static LinkedHashSet<PhysicalTableDefinition> buildMonthlyTableDefinitions(TestDimensions testDimensions) {
        return Utils.asLinkedHashSet(
                new PhysicalTableDefinition(
                        MONTHLY,
                        MONTH.buildZonedTimeGrain(DateTimeZone.UTC),
                        testDimensions.getDimensionConfigurationsByApiName(OTHER)
                )
        );
    }

    /**
     * Build hourly monthly table definitions.
     *
     * @param testDimensions  Dimensions to load in the tables
     *
     * @return the hourly monthly table definitions
     */
    public static LinkedHashSet<PhysicalTableDefinition> buildHourlyMonthlyTableDefinitions(
            TestDimensions testDimensions
    ) {
        return Stream.concat(
                buildHourlyTableDefinitions(testDimensions).stream(),
                buildMonthlyTableDefinitions(testDimensions).stream()
        ).collect(Collectors.toCollection(LinkedHashSet::new));
    }


    /**
     * Build the pet table definitions.
     *
     * @param testDimensions  Dimensions to build the tables with
     *
     * @return the pet table definitions
     */
    public static LinkedHashSet<PhysicalTableDefinition> buildPetTableDefinitions(TestDimensions testDimensions) {
        return Utils.asLinkedHashSet(
                new PhysicalTableDefinition(
                        ALL_PETS,
                        DAY.buildZonedTimeGrain(DateTimeZone.UTC),
                        testDimensions.getDimensionConfigurationsByApiName(BREED, SEX, SPECIES)
                )
        );
    }

    /**
     * Build the shape table definitions.
     *
     * @param testDimensions  Dimensions to build the tables with
     *
     * @return the shape table definitions
     */
    public static LinkedHashSet<PhysicalTableDefinition> buildShapeTableDefinitions(TestDimensions testDimensions) {
        return Utils.asLinkedHashSet(
                new PhysicalTableDefinition(
                        ALL_SHAPES,
                        DAY.buildZonedTimeGrain(DateTimeZone.UTC),
                        testDimensions.getDimensionConfigurationsByApiName(COLOR, SIZE, SHAPE, OTHER, MODEL)
                ),
                new PhysicalTableDefinition(
                        COLOR_SHAPES_HOURLY,
                        HOUR.buildZonedTimeGrain(DateTimeZone.UTC),
                        testDimensions.getDimensionConfigurationsByApiName(COLOR)
                ),
                new PhysicalTableDefinition(
                        COLOR_SHAPES,
                        DAY.buildZonedTimeGrain(DateTimeZone.UTC),
                        testDimensions.getDimensionConfigurationsByApiName(COLOR)
                ),
                new PhysicalTableDefinition(
                        COLOR_SHAPES_MONTHLY,
                        MONTH.buildZonedTimeGrain(DateTimeZone.UTC),
                        testDimensions.getDimensionConfigurationsByApiName(COLOR)
                ),
                new PhysicalTableDefinition(
                        COLOR_SIZE_SHAPES,
                        DAY.buildZonedTimeGrain(DateTimeZone.UTC),
                        testDimensions.getDimensionConfigurationsByApiName(COLOR, SIZE)
                ),
                new PhysicalTableDefinition(
                        COLOR_SIZE_SHAPE_SHAPES,
                        DAY.buildZonedTimeGrain(DateTimeZone.UTC),
                        testDimensions.getDimensionConfigurationsByApiName(COLOR, SIZE, SHAPE)
                )
        );
    }
}
