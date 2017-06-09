// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.response;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

/**
 * A row of results in a GroupByQuery.
 * todo: use and test
 */
@JsonPropertyOrder({"version", "timestamp", "events"})
public class GroupByResultRow extends DruidResultRow {
    @JsonIgnore
    public final Version version;

    @JsonProperty
    private final Map<String, Object> events = new HashMap<>();

    /**
     * Creates a row with the given timestamp.
     *
     * @param timestamp  The timestamp to set the result for.
     * @param version  The version of GroupByResult.
     */
    public GroupByResultRow(DateTime timestamp, Version version) {
        super(timestamp);
        this.version = version;
    }

    /**
     * Adds a json key/value pair to the row.
     *
     * @param key  The key to be added.
     * @param value  The value of the key.
     */
    public void add(String key, Object value) {
        events.put(key, value);
    }

    /**
     * Gets the version of the GroupByResult.
     *
     * @return "v1" or "v2" for corresponding versions.
     */
    @JsonProperty
    public String getVersion() {
        return version.toString();
    }

    /**
     * Enum to clarify the version being used.
     */
    public enum Version {
        V1("v1"),
        V2("v2");

        private String name;

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
