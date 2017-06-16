// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.response;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

/**
 * A row of results in a GroupByQuery.
 */
@JsonPropertyOrder({"version", "timestamp", "event"})
public class GroupByResultRow extends DruidResultRow {
    @JsonIgnore
    private final String version;

    @JsonProperty
    private final Map<String, Object> event;

    /**
     * Creates a row with the given timestamp.
     *
     * @param timestamp  The timestamp to set the result for.
     * @param version  The version of GroupByResult.
     */
    public GroupByResultRow(DateTime timestamp, Version version) {
        super(timestamp);
        this.version = version.name;
        event = new HashMap<>();
    }

    @Override
    public void add(String key, String value) {
        event.put(key, value);
    }

    @Override
    public void add(String key, Number value) {
        event.put(key, value);
    }

    /**
     * Gets the version of the GroupByResult.
     *
     * @return "v1" or "v2" for corresponding versions.
     */
    @JsonProperty
    public String getVersion() {
        return version;
    }

    /**
     * Enum to clarify the version being used.
     */
    public enum Version {
        V1("v1"),
        V2("v2");

        private final String name;

        /**
         * Creates the version with a name.
         *
         * @param name  The name to be shown in results.
         */
        Version(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
