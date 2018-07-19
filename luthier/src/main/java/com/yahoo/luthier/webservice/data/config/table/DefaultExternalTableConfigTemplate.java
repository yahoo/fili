// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.luthier.webservice.data.config.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

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
public class DefaultExternalTableConfigTemplate implements ExternalTableConfigTemplate {

    private final Set<PhysicalTableInfoTemplate> physicalTables;
    private final Set<LogicalTableInfoTemplate> logicalTables;

    /**
     * Constructor used by json parser.
     *
     * @param physicalTables   json property druidTables
     * @param logicalTables json property logicalTables
     */
    @JsonCreator
    public DefaultExternalTableConfigTemplate(
            @JsonProperty("physicalTables") Set<PhysicalTableInfoTemplate> physicalTables,
            @JsonProperty("logicalTables") Set<LogicalTableInfoTemplate> logicalTables
    ) {
        this.physicalTables = ImmutableSet.copyOf(physicalTables);
        this.logicalTables = ImmutableSet.copyOf(logicalTables);
    }

    @Override
    public Set<PhysicalTableInfoTemplate> getPhysicalTables() {
        return this.physicalTables;
    }

    @Override
    public Set<LogicalTableInfoTemplate> getLogicalTables() {
        return this.logicalTables;
    }
}
