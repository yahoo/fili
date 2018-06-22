// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.LinkedHashSet;
import java.util.Objects;

/**
 * Table set template.
 * <p>
 * An example:
 *
 *       {
 *          "tables" :
 *              [
 *                  a list of WikiTableConfigTemplate
 *              ]
 *       }
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class WikiTableSetTemplate {

    @JsonProperty("tables")
    private LinkedHashSet<WikiTableConfigTemplate> tables;

    /**
     * Constructor used by json parser.
     *
     * @param tables  json property tables, set of tables
     */
    @JsonCreator
    public WikiTableSetTemplate (
            @JsonProperty("tables") LinkedHashSet<WikiTableConfigTemplate> tables
    ) {
        this.tables = (Objects.isNull(tables) ? new LinkedHashSet<>() : new LinkedHashSet<>(tables));
    }

    /**
     * Get a set of tables.
     *
     * @return a set of tables
     */
    public LinkedHashSet<WikiTableConfigTemplate> getTables() {
        return this.tables;
    }
}
