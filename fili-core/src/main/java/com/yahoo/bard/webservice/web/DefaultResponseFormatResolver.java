// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import java.util.LinkedHashMap;
import java.util.Map;

import javax.ws.rs.container.ContainerRequestContext;

/**
 * A Fili default implementation of ResponseFormatResolver. This implementation works with three formats: json, jsonapi
 * and csv.
 */
public class DefaultResponseFormatResolver implements ResponseFormatResolver {
    public static String ACCEPT_HEADER_JSON = "application/json";
    public static String ACCEPT_HEADER_JSONAPI = "application/vnd.api+json";
    public static String ACCEPT_HEADER_CSV = "text/csv";
    private Map<String, String> formatsMap;

    /**
     * Constructor.
     */
    public DefaultResponseFormatResolver() {
        formatsMap = new LinkedHashMap<>();
        formatsMap.put(ACCEPT_HEADER_JSON, "json");
        formatsMap.put(ACCEPT_HEADER_JSONAPI, "jsonapi");
        formatsMap.put(ACCEPT_HEADER_CSV, "csv");
    }

    @Override
    public String apply(final String format, final ContainerRequestContext containerRequestContext) {
        String headerFormat = containerRequestContext.getHeaderString("Accept");
        if (format != null || headerFormat == null) {
            return format;
        }
        return formatsMap.entrySet().stream()
                .filter(entry -> headerFormat.contains(entry.getKey()))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElse(null);
    }
}
