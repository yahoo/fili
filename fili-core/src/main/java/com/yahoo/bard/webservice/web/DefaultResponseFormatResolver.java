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
    Map<String, String> formatsMap;

    /**
     * Constructor.
     */
    public DefaultResponseFormatResolver() {
        formatsMap = new LinkedHashMap<>();
        formatsMap.put("application/json", "json");
        formatsMap.put("application/vnd.api+json", "jsonapi");
        formatsMap.put("text/csv", "csv");
    }

    @Override
    public String accept(final String format, final ContainerRequestContext containerRequestContext) {
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
