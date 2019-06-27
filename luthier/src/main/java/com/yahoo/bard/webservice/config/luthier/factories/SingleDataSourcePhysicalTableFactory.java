// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config.luthier.factories;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.yahoo.bard.webservice.config.luthier.Factory;
import com.yahoo.bard.webservice.config.luthier.LuthierIndustrialPark;
import com.yahoo.bard.webservice.config.luthier.LuthierValidationUtils;
import com.yahoo.bard.webservice.data.config.LuthierTableName;
import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService;
import com.yahoo.bard.webservice.table.Column;
import com.yahoo.bard.webservice.table.PhysicalTable;
import com.yahoo.bard.webservice.table.availability.Availability;
import org.apache.avro.data.Json;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * A factory that is used by default to support Simple (non-Composite) Physical Table.
 */
public abstract class SingleDataSourcePhysicalTableFactory implements Factory<PhysicalTable> {
    // public static final String DEFAULT_FIELD_NAME_ERROR =
    //         "Base physical table '%s': defaultField name '%s' not found in fields '%s'";

    protected TableName tableName;
    protected ZonedTimeGrain timeGrain;
    protected Set<Column> columns;
    protected Map<String, String> logicalToPhysicalColumnNames;
    protected DataSourceMetadataService metadataService;

    public static final String PHYSICAL_TABLE = "single data source physical table";


    /**
     * Parse the config file to prepare the subclasses for building the Physical Tables.
     *
     * @param name  the config dictionary name (normally the apiName)
     * @param configTable  the json tree describing this config entity
     * @param resourceFactories  the source for locating dependent objects
     */
    public void prepare(String name, ObjectNode configTable, LuthierIndustrialPark resourceFactories) {
        this.tableName = new LuthierTableName(name);

        // TODO: Time grain not done yet
        LuthierValidationUtils.validateField(configTable.get("granularity"), PHYSICAL_TABLE, name, "granularity");
        String longName = configTable.get("granularity").textValue();

        // TODO: columns not done yet
        this.columns = new LinkedHashSet<>();

        // TODO: logicalToPhysicalColumnNames not tested yet
        this.logicalToPhysicalColumnNames = new LinkedHashMap<>();
        LuthierValidationUtils.validateField(
                configTable.get("logicalToPhysicalColumnNames"),
                PHYSICAL_TABLE,
                name,
                "searchProvider"
        );
        JsonNode columnNameMapNode = configTable.get("logicalToPhysicalColumnNames");
        columnNameMapNode.forEach(
                node -> logicalToPhysicalColumnNames.put(
                        node.get("logicalName").textValue(),
                        node.get("physicalName").textValue()
                )
        );


        this.metadataService = resourceFactories.getMetadataService();
    }
}
