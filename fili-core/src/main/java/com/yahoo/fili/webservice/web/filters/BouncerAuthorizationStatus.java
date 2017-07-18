// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.fili.webservice.web.filters;

import com.yahoo.fili.webservice.util.EnumUtils;

import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Bouncer pass through status enum.
 */
public enum BouncerAuthorizationStatus {
    YBC_MISSING(-2, "YBC Missing", "YBC cookie not present, but it is required to access this request."),
    ACCESS_DENIED(-1, "Access Denied", "Request access denied."),
    GENERIC_ERROR(0, "Generic Error", "YBY cookie not present or is invalid."),
    ACCESS_ALLOWED(1, "Access Allowed", "Good to go!");

    private final String name;
    private final String description;
    private final int bouncerValue;

    /**
     * Constructor.
     *
     * @param bouncerValue  Value from Bouncer filter
     * @param name  Name of the status
     * @param description  Human-readable description of the status
     */
    BouncerAuthorizationStatus(int bouncerValue, String name, String description) {
        this.name = name;
        this.bouncerValue = bouncerValue;
        this.description = description;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public int getBouncerValue() {
        return bouncerValue;
    }

    // Hold the mapping to speed up lookup
    private static final Map<Integer, BouncerAuthorizationStatus> BOUNCER_VALUE_TO_STATUS = new LinkedHashMap<>();

    // Initialize the lookup mapping when the class is loaded
    static {
        for (BouncerAuthorizationStatus value : EnumSet.allOf(BouncerAuthorizationStatus.class)) {
            BOUNCER_VALUE_TO_STATUS.put(value.getBouncerValue(), value);
        }
    }

    /**
     * Get the value for the given bouncer value.
     *
     * @param bouncerValue  Bouncer pass-through value to get the BouncerAuthorizationStatus for
     *
     * @return The matched value
     *
     * @throws IllegalArgumentException if this enum type has no constant with the specified bouncer value
     */
    public static BouncerAuthorizationStatus forBouncerValue(int bouncerValue) {
        return EnumUtils.forKey(bouncerValue, BOUNCER_VALUE_TO_STATUS, BouncerAuthorizationStatus.class);
    }
}
