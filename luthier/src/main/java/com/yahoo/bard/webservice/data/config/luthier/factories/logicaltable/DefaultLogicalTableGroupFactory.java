// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.luthier.factories.logicaltable;

import com.yahoo.bard.webservice.application.luthier.LuthierConfigNode;
import com.yahoo.bard.webservice.data.config.luthier.Factory;
import com.yahoo.bard.webservice.data.config.luthier.LuthierIndustrialPark;
import com.yahoo.bard.webservice.data.config.luthier.LuthierValidationUtils;
import com.yahoo.bard.webservice.data.config.luthier.names.LuthierApiMetricName;
import com.yahoo.bard.webservice.data.config.luthier.table.LogicalTableGroup;
import com.yahoo.bard.webservice.data.config.names.ApiMetricName;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.data.time.GranularityParser;
import com.yahoo.bard.webservice.exceptions.LuthierFactoryException;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.table.PhysicalTable;
import com.yahoo.bard.webservice.table.TableGroup;
import com.yahoo.bard.webservice.table.TableIdentifier;
import com.yahoo.bard.webservice.util.GranularityParseException;

import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.ReadablePeriod;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * A factory that is used by default to support LogicalTableGroup.
 *
 * A LogicalTableGroup is essentially a map from TableIdentifier to LogicalTable.
 * This concept exists because one configuration string (name) defines multiple LogicalTableIdentifiers
 * (name, granularity pair). LogicalTableGroup allows us to collect all the distinct LogicalTables for a single name,
 * mapping LogicalTableIdentifier to LogicalTable.
 *
 * At the external configuration layer, configuration users will define a logical table group (keyed by name)
 * with a list of granularities. The factory will produce the collection of LogicalTables keyed by identifier
 * (name, grain pairs), to be merged into the table dictionary.
 */
public class DefaultLogicalTableGroupFactory implements Factory<LogicalTableGroup> {
    private static final String ENTITY_TYPE = "default logical table group";
    private static final String GRANULARITY_DICTIONARY_MISSING = "granularityDictionary missing from the " +
            "LuthierIndustrialPark. Will not build " + ENTITY_TYPE + ": %s.";
    private static final String CATEGORY = "category";
    private static final String LONG_NAME = "longName";
    private static final String GRANULARITIES = "granularities";
    private static final String RETENTION = "retention";
    private static final String DESCRIPTION = "description";
    private static final String DATE_TIME_ZONE = "dateTimeZone";
    private static final String DIMENSIONS = "dimensions";
    private static final String PHYSICAL_TABLES = "physicalTables";
    private static final String METRICS = "metrics";

    /**
     * Build a group of LogicalTable, it has one LogicalTable per granularity.
     *
     * @param name  apiName of the LogicalTable
     * @param configTable  the LogicalTableGroup's configuration
     * @param resourceFactories  the source for locating dependent objects
     *
     * @return  A newly constructed config instance for the name and config provided
     */
    @Override
    public LogicalTableGroup build(
            String name,
            LuthierConfigNode configTable,
            LuthierIndustrialPark resourceFactories
    ) {
        if (resourceFactories.getGranularityParser() == null) {
            throw new LuthierFactoryException(String.format(GRANULARITY_DICTIONARY_MISSING, name));
        }
        LuthierValidationUtils.validateFields(
                configTable,
                ENTITY_TYPE,
                name,
                CATEGORY,
                LONG_NAME,
                GRANULARITIES,
                RETENTION,
                DESCRIPTION,
                DATE_TIME_ZONE,
                DIMENSIONS,
                PHYSICAL_TABLES,
                METRICS
        );

        String category = configTable.get(CATEGORY).textValue();
        String longName = configTable.get(LONG_NAME).textValue();
        ReadablePeriod retention = Period.parse(configTable.get(RETENTION).textValue());
        String description = configTable.get(DESCRIPTION).textValue();
        MetricDictionary metricDictionary = resourceFactories.getMetricDictionary();

        LinkedHashSet<Dimension> dimensions = buildDimensions(configTable, resourceFactories);
        LinkedHashSet<PhysicalTable> physicalTables = buildPhysicalTables(configTable, resourceFactories);
        List<Granularity> granularities = buildGranularities(configTable, resourceFactories);
        LinkedHashSet<ApiMetricName> metricNames = buildMetricNames(configTable, resourceFactories, granularities);
        TableGroup tableGroup = new TableGroup(physicalTables, metricNames, dimensions);

        /* return a LogicalTableGroup that contains a LogicalTable for each granularity */
        return granularities.stream()
                .collect(Collectors.toMap(
                        granularity -> new TableIdentifier(name, granularity),
                        granularity -> new LogicalTable(
                                name,
                                category,
                                longName,
                                granularity,
                                retention,
                                description,
                                tableGroup,
                                metricDictionary
                        ),
                        (a, b) -> b,
                        LogicalTableGroup::new
                ));
    }

    /**
     * Build a set of Dimensions.
     *
     * @param configTable  LuthierConfigNode that points to the value of corresponding table entry in config file
     * @param resourceFactories  the source for locating dependent objects
     *
     * @return set of Dimensions built according to the configTable
     */
    private LinkedHashSet<Dimension> buildDimensions(
            LuthierConfigNode configTable,
            LuthierIndustrialPark resourceFactories
    ) {
        return StreamSupport.stream(configTable.get(DIMENSIONS).spliterator(), false)
                .map(LuthierConfigNode::textValue)
                .map(resourceFactories::getDimension)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Build a set of PhysicalTables.
     *
     * @param configTable  LuthierConfigNode that points to the value of corresponding table entry in config file
     * @param resourceFactories  the source for locating dependent objects
     *
     * @return set of PhysicalTables built according to the configTable
     */
    private LinkedHashSet<PhysicalTable> buildPhysicalTables(
            LuthierConfigNode configTable,
            LuthierIndustrialPark resourceFactories
    ) {
        return StreamSupport.stream(
                configTable.get(PHYSICAL_TABLES).spliterator(),
                false
        )
                .map(LuthierConfigNode::textValue)
                .map(resourceFactories::getPhysicalTable)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Build a set of metric names.
     *
     * @param configTable  LuthierConfigNode that points to the value of corresponding table entry in config file
     * @param resourceFactories  the source for locating dependent objects
     * @param granularities  list of granularities the metric supports
     *
     * @return set of ApiMetricName built according to the configTable
     */
    private LinkedHashSet<ApiMetricName> buildMetricNames(
            LuthierConfigNode configTable,
            LuthierIndustrialPark resourceFactories,
            List<Granularity> granularities
    ) {
        return StreamSupport.stream(configTable.get(METRICS).spliterator(), false)
                .map(LuthierConfigNode::textValue)
                .peek(resourceFactories::getMetric)
                .map(metricName -> new LuthierApiMetricName(metricName, granularities))
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Build a set of Granularities.
     *
     * @param configTable  LuthierConfigNode that points to the value of corresponding table entry in config file
     * @param resourceFactories  the source for locating dependent objects
     *
     * @return set of Granularities built according to the configTable
     */
    private List<Granularity> buildGranularities(
            LuthierConfigNode configTable,
            LuthierIndustrialPark resourceFactories
    ) {
        GranularityParser parser = resourceFactories.getGranularityParser();
        DateTimeZone dateTimeZone = DateTimeZone.forID(configTable.get(DATE_TIME_ZONE).textValue());
        return StreamSupport.stream(configTable.get(GRANULARITIES).spliterator(), false)
                .map(LuthierConfigNode::textValue)
                .map(grainName -> {
                    try {
                        return parser.parseGranularity(grainName, dateTimeZone);
                    } catch (GranularityParseException e) {
                        throw new LuthierFactoryException(e.getMessage(), e);
                    }
                })
                .collect(Collectors.toList());
    }
}
