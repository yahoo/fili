// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.util.EnumUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Table info.
 * <p>
 * An example:
 * <p>
 *      {
 *          "name": "WIKITICKER",
 *          "metrics": [
 *              "COUNT",
 *              "ADDED",
 *              "DELTA",
 *              "DELETED"
 *          ]
 *      }
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class WikiTableInfoTemplate implements TableName {

    @JsonProperty("name")
    private String name;

    @JsonProperty("metrics")
    private List<String> metrics;

    /**
     * Constructor used by json parser.
     *
     * @param name    json property name, table's name
     * @param metrics json property metrics, a set of metrics this table depends on
     */
    @JsonCreator
    public WikiTableInfoTemplate(
            @JsonProperty("name") String name,
            @JsonProperty("metrics") LinkedHashSet<String> metrics
    ) {
        this.name = EnumUtils.camelCase(name);
        this.metrics = (Objects.isNull(metrics) ?
                Collections.emptyList() : metrics.stream()
                .map(metric -> EnumUtils.camelCase(metric))
                .collect(Collectors.toList()));
        ;
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
     * Get metrics of this table.
     *
     * @return a set of metrics' name
     */
    public List<String> getMetrics() {
        return this.metrics;
    }

    @Override
    public String asName() {
        return this.name;
    }
}
