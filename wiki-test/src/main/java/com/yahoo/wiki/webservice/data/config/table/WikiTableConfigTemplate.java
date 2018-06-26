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
 *              WikiPhysicalTableInfoTemplate
 *          },
 *          "logicalTables": {
 *              WikiLogicalTableInfoTemplate
 *          }
 *      }
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class WikiTableConfigTemplate {

    private final LinkedHashSet<WikiPhysicalTableInfoTemplate> physicalTables;
    private final LinkedHashSet<WikiLogicalTableInfoTemplate> logicalTables;

    /**
     * Constructor used by json parser.
     *
     * @param physicalTables   json property druidTables
     * @param logicalTables json property logicalTables
     */
    @JsonCreator
    public WikiTableConfigTemplate(
            @JsonProperty("druidTables") LinkedHashSet<WikiPhysicalTableInfoTemplate> physicalTables,
            @JsonProperty("logicalTables") LinkedHashSet<WikiLogicalTableInfoTemplate> logicalTables
    ) {
        this.physicalTables = (Objects.isNull(physicalTables) ? null : physicalTables);
        this.logicalTables = (Objects.isNull(logicalTables) ? null : logicalTables);
    }

    /**
     * Get physical table set.
     *
     * @return druid table info
     */
    public LinkedHashSet<WikiPhysicalTableInfoTemplate> getPhysicalTables() {
        return this.physicalTables;
    }

    /**
     * Get logical table set.
     *
     * @return logical table info
     */
    public LinkedHashSet<WikiLogicalTableInfoTemplate> getLogicalTables() {
        return this.logicalTables;
    }
}
