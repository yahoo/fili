// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.LinkedHashSet;
import java.util.Objects;

/**
 * Table configuration template.
 * <p>
 * An example:
 * <p>
 *      {
 *          "name" : "WIKI_TEST",
 *          "physicalTables": {
 *              PhysicalTableInfoTemplate
 *          },
 *          "logicalTables": {
 *              LogicalTableInfoTemplate
 *          }
 *      }
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TableConfigTemplate {

    private final LinkedHashSet<PhysicalTableInfoTemplate> physicalTables;
    private final LinkedHashSet<LogicalTableInfoTemplate> logicalTables;

    /**
     * Constructor used by json parser.
     *
     * @param physicalTables   json property druidTables
     * @param logicalTables json property logicalTables
     */
    @JsonCreator
    public TableConfigTemplate(
            @JsonProperty("druidTables") LinkedHashSet<PhysicalTableInfoTemplate> physicalTables,
            @JsonProperty("logicalTables") LinkedHashSet<LogicalTableInfoTemplate> logicalTables
    ) {
        this.physicalTables = (Objects.isNull(physicalTables) ? null : physicalTables);
        this.logicalTables = (Objects.isNull(logicalTables) ? null : logicalTables);
    }

    /**
     * Get physical table set.
     *
     * @return druid table info
     */
    public LinkedHashSet<PhysicalTableInfoTemplate> getPhysicalTables() {
        return this.physicalTables;
    }

    /**
     * Get logical table set.
     *
     * @return logical table info
     */
    public LinkedHashSet<LogicalTableInfoTemplate> getLogicalTables() {
        return this.logicalTables;
    }
}
