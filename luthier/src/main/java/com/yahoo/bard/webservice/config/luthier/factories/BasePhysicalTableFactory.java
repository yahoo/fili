// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config.luthier.factories;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.yahoo.bard.webservice.config.luthier.Factory;
import com.yahoo.bard.webservice.config.luthier.LuthierIndustrialPark;
import com.yahoo.bard.webservice.config.luthier.LuthierValidationUtils;
import com.yahoo.bard.webservice.data.config.LuthierTableName;
import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.table.BasePhysicalTable;
import com.yahoo.bard.webservice.table.Column;
import com.yahoo.bard.webservice.table.PhysicalTable;
import com.yahoo.bard.webservice.table.availability.Availability;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A factory that is used by default to support Base (non-Composite) Physical Table.
 */
public class BasePhysicalTableFactory implements Factory<PhysicalTable> {
    // public static final String DEFAULT_FIELD_NAME_ERROR =
    //         "Base physical table '%s': defaultField name '%s' not found in fields '%s'";

    public static final String PHYSICAL_TABLE = "base physical table";



    /**
     * Create a physical table.
     *
     * @param name  Fili name of the physical table
     * @param timeGrain  time grain of the table
     * @param columns The columns for this physical table
     * @param logicalToPhysicalColumnNames  Mappings from logical to physical names
     * @param availability  The availability of columns in this table
     */
    public BasePhysicalTable(
            @NotNull TableName name,
            @NotNull ZonedTimeGrain timeGrain,
            @NotNull Iterable<Column> columns,
            @NotNull Map<String, String> logicalToPhysicalColumnNames,
            @NotNull Availability availability
    );

    private List<Column> columnsParser(JsonNode node) {
        List<Column> columns = new ArrayList<>();
        for (JsonNode columnNode : node) {
            columns.add(Columns)
        }
    }

    /**
     * Build a dimension instance.
     *
     * @param name  the config dictionary name (normally the apiName)
     * @param configTable  the json tree describing this config entity
     * @param resourceFactories  the source for locating dependent objects
     *
     * @return  A newly constructed config instance for the name and config provided
     */
    @Override
    public PhysicalTable build(String name, ObjectNode configTable, LuthierIndustrialPark resourceFactories) {
        TableName tableName = new LuthierTableName(name);

        // TODO: complete the info in JSON to proceed
        LuthierValidationUtils.validateField(configTable.get("granularity"), PHYSICAL_TABLE, name, "granularity");
        String longName = configTable.get("granularity").textValue();

        LuthierValidationUtils.validateField(configTable.get("dimensions"), PHYSICAL_TABLE, name, "dimensions");
        JsonNode category = configTable.get("dimensions");
        List<Column> columns =

        LuthierValidationUtils.validateField(configTable.get("description"), PHYSICAL_TABLE, name, "description");
        String description = configTable.get("description").textValue();

        LuthierValidationUtils.validateField(configTable.get("keyValueStore"), PHYSICAL_TABLE, name, "keyValueStore");

        LuthierValidationUtils.validateField(configTable.get("searchProvider"), PHYSICAL_TABLE, name, "searchProvider");

        TableName name;
        ZonedTimeGrain timeGrain;
        Iterable<Column> columns;
        Map<String, String> logicalToPhysicalColumnNames;
        Availability availability;
        return new BasePhysicalTable(
                name,
                longName,
                category,
                description,
                dimensionFields,
                keyValueStore,
                searchProvider,
                defaultDimensionFields,
                isAggregatable
        );
    }
}
