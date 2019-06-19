// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config.luthier.factories;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.yahoo.bard.webservice.config.luthier.Factory;
import com.yahoo.bard.webservice.config.luthier.LuthierFactoryException;
import com.yahoo.bard.webservice.config.luthier.LuthierIndustrialPark;
import com.yahoo.bard.webservice.config.luthier.LuthierValidationUtils;
import com.yahoo.bard.webservice.data.config.LogicalTableGroup;
import com.yahoo.bard.webservice.data.config.names.ApiMetricName;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.table.PhysicalTable;
import com.yahoo.bard.webservice.table.TableGroup;
import com.yahoo.bard.webservice.table.TableIdentifier;
import com.yahoo.bard.webservice.util.GranularityParseException;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.ReadablePeriod;

import java.util.LinkedHashSet;

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
    private static final String GRANULARITY_PARSING_ERROR =
            "granularity: %s does not exist in the granularityDictionary";
    private static final String GRANULARITY_DICTIONARY_MISSING = "granularityDictionary missing from the " +
            "LuthierIndustrialPark. Will not build " + ENTITY_TYPE + ": %s.";

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
    public LogicalTableGroup build(String name, ObjectNode configTable, LuthierIndustrialPark resourceFactories) {
        if (resourceFactories.getGranularityParser() == null) {
            throw new LuthierFactoryException(String.format(GRANULARITY_DICTIONARY_MISSING, name));
        }
        LogicalTableGroup logicalTableGroup = new LogicalTableGroup();
        validateFields(name, configTable);
        String category = configTable.get("category").textValue();
        String longName = configTable.get("longName").textValue();
        ReadablePeriod retention = Period.parse(configTable.get("retention").textValue());
        String description = configTable.get("description").textValue();

        MetricDictionary metricDictionary = resourceFactories.getMetricDictionary();
        LinkedHashSet<Dimension> dimensions = new LinkedHashSet<>();
        LinkedHashSet<PhysicalTable> physicalTables = new LinkedHashSet<>();
        LinkedHashSet<ApiMetricName> apiMetricNames = new LinkedHashSet<>();
        configTable.get("dimensions").forEach(
                node -> dimensions.add(resourceFactories.getDimension(node.textValue()))
        );
        configTable.get("physicalTables").forEach(
                node -> physicalTables.add(resourceFactories.getPhysicalTable(node.textValue()))
        );
        configTable.get("metrics").forEach(
                node -> {
                    /* go to the LIP to retrieve the metric */
                    /* Must be done after having actual metric builders! */
                    // apiMetricNames.add(new FiliApiMetricName(node.textValue(), granularity));
                    // metricDictionary.add();
                }
        );
        TableGroup tableGroup = new TableGroup(physicalTables, apiMetricNames, dimensions);

        configTable.get("granularities").forEach(
                 node -> {
                    DateTimeZone dateTimeZone = DateTimeZone.forID(configTable.get("dateTimeZone").textValue());
                    Granularity granularity;
                    try {
                        granularity = resourceFactories.getGranularityParser()
                                .parseGranularity(node.textValue(), dateTimeZone);
                    } catch (GranularityParseException e) {
                        throw new LuthierFactoryException(
                                String.format(GRANULARITY_PARSING_ERROR, node.textValue()),
                                e
                        );
                    }
                    LogicalTable logicalTable = new LogicalTable(
                            name,
                            category,
                            longName,
                            granularity,
                            retention,
                            description,
                            tableGroup,
                            metricDictionary
                    );
                    logicalTableGroup.put(new TableIdentifier(logicalTable), logicalTable);
                }
        );

        return logicalTableGroup;
    }

    /**
     * makes sure the necessary fields exist in the json config.
     *
     * @param name  the config dictionary name (normally the apiName)
     * @param configTable  ObjectNode that points to the value of corresponding table entry in config file
     */
    private void validateFields(String name, ObjectNode configTable) {
        LuthierValidationUtils.validateField(configTable.get("category"), ENTITY_TYPE, name, "category");
        LuthierValidationUtils.validateField(configTable.get("longName"), ENTITY_TYPE, name, "longName");
        LuthierValidationUtils.validateField(configTable.get("granularities"), ENTITY_TYPE, name, "granularities");
        LuthierValidationUtils.validateField(configTable.get("retention"), ENTITY_TYPE, name, "retention");
        LuthierValidationUtils.validateField(configTable.get("description"), ENTITY_TYPE, name, "description");
        LuthierValidationUtils.validateField(configTable.get("dateTimeZone"), ENTITY_TYPE, name, "dateTimeZone");
        LuthierValidationUtils.validateField(configTable.get("dimensions"), ENTITY_TYPE, name, "dimensions");
        LuthierValidationUtils.validateField(configTable.get("physicalTables"), ENTITY_TYPE, name, "physicalTables");
        LuthierValidationUtils.validateField(configTable.get("metrics"), ENTITY_TYPE, name, "metrics");
    }
}
