// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.mock;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by hinterlong on 6/1/17.
 */
@JsonPropertyOrder({"version", "timestamp", "events"})
public class GroupByResult extends DruidResult {
    @JsonIgnore
    public final Version version;

    @JsonProperty
    private final Map<String, Object> events = new HashMap<>();

    public GroupByResult(DateTime timestamp, Version version) {
        super(timestamp);
        this.version = version;
    }

    public void add(String key, Object value) {
        events.put(key, value);
    }

    @JsonProperty
    public String getVersion() {
        return version.toString();
    }

    public enum Version {
        V1("v1"),
        V2("v2");

        private String name;

        Version(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }

}
