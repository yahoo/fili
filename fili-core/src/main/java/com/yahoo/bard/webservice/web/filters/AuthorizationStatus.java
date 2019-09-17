// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.filters;

import com.yahoo.bard.webservice.util.EnumUtils;

import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Authorization status code.
 */
public enum AuthorizationStatus {
    ACCESS_DENIED(-1, "Access Denied", "Request access denied."),
    ACCESS_ALLOWED(1, "Access Allowed", "Good to go!");

    private final String name;
    private final String description;
    private final int statusCode;

    /**
     * Constructor.
     *
     * @param statusCode  Code from authorization filters  -1 for failure, 1 for success
     * @param name  Name of the status
     * @param description  Human-readable description of the status
     */
    AuthorizationStatus(int statusCode, String name, String description) {
        this.name = name;
        this.statusCode = statusCode;
        this.description = description;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public int getStatusCode() {
        return statusCode;
    }

    // Hold the mapping to speed up lookup
    private static final Map<Integer, AuthorizationStatus> CODE_TO_STATUS = new LinkedHashMap<>();

    // Initialize the lookup mapping when the class is loaded
    static {
        for (AuthorizationStatus value : EnumSet.allOf(AuthorizationStatus.class)) {
            CODE_TO_STATUS.put(value.getStatusCode(), value);
        }
    }

    /**
     * Get the value for the given status code value.
     *
     * @param statusCode  Pass-through value to get the AuthorizationStatus
     *
     * @return The matched value
     *
     * @throws IllegalArgumentException if this enum type has no constant with the specified status code value
     */
    public static AuthorizationStatus forStatusCode(int statusCode) {
        return EnumUtils.forKey(statusCode, CODE_TO_STATUS, AuthorizationStatus.class);
    }
}
