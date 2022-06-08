// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.filters;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import javax.validation.constraints.NotNull;

/**
 * Authorization status code.
 */
public class AuthorizationStatus {

    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    public static final String REQUEST_ACCESS_DENIED = "Request access denied.";
    public static final String GOOD_TO_GO = "Good to go!";
    private static final Map<Integer, AuthorizationStatus> CODE_TO_STATUS = new LinkedHashMap<>();

    public static final String FAILURE_MESSAGE = SYSTEM_CONFIG.getStringProperty(
            SYSTEM_CONFIG.getPackageVariableName("authorization_status_failure_message"),
            REQUEST_ACCESS_DENIED
    );

    public static final String SUCCESS_MESSAGE = SYSTEM_CONFIG.getStringProperty(
            SYSTEM_CONFIG.getPackageVariableName("authorization_status_success_message"),
            GOOD_TO_GO
    );

    public static final AuthorizationStatus ACCESS_DENIED = new AuthorizationStatus(
            -1,
            "Access Denied",
            FAILURE_MESSAGE
    );

    public static final AuthorizationStatus ACCESS_ALLOWED = new AuthorizationStatus(
            1,
            "Access Allowed",
            SUCCESS_MESSAGE
    );

    public static Set<AuthorizationStatus> values = new HashSet<>();
    // Hold the mapping to speed up lookup

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
    AuthorizationStatus(int statusCode, @NotNull String name, @NotNull  String description) {
        this.name = name;
        this.statusCode = statusCode;
        this.description = description;
        CODE_TO_STATUS.put(statusCode, this);
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
        return CODE_TO_STATUS.get(statusCode);
    }
}
