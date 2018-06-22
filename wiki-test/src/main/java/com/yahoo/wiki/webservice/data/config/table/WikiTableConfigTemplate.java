// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Table configuration template.
 * <p>
 * An example:
 * <p>
 *      {
 *          "name" : "WIKI_TEST",
 *          "druidTable": {
 *              WikiTableInfoTemplate
 *          },
 *          "logicalTable": {
 *              WikiTableInfoTemplate
 *          }
 *      }
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class WikiTableConfigTemplate {

    @JsonProperty("name")
    private String name;

    @JsonProperty("druidTable")
    private WikiTableInfoTemplate druidTable;

    @JsonProperty("logicalTable")
    private WikiTableInfoTemplate logicalTable;

    /**
     * Constructor used by json parser.
     *
     * @param name         json property name
     * @param druidTable   json property druidTable
     * @param logicalTable json property logicalTable
     */
    @JsonCreator
    public WikiTableConfigTemplate(
            @JsonProperty("name") String name,
            @JsonProperty("druidTable") WikiTableInfoTemplate druidTable,
            @JsonProperty("logicalTable") WikiTableInfoTemplate logicalTable
    ) {
        this.name = (Objects.isNull(name) ? "" : name);
        this.druidTable = (Objects.isNull(druidTable) ? null : druidTable);
        this.logicalTable = (Objects.isNull(logicalTable) ? null : logicalTable);
    }

    /**
     * Get table name.
     *
     * @return table's name
     */
    public String getName() {
        return this.name;
    }

    /**
     * Get druid table.
     *
     * @return druid table info
     */
    public WikiTableInfoTemplate getDruidTable() {
        return this.druidTable;
    }

    /**
     * Get logical table.
     *
     * @return logical table info
     */
    public WikiTableInfoTemplate getLogicalTable() {
        return this.logicalTable;
    }
}
