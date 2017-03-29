// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config;

/**
 * Feature flags bind an object to a system configuration name.
 */
public enum BardFeatureFlag implements FeatureFlag {
    PARTIAL_DATA("partial_data_enabled"),
    DRUID_CACHE("druid_cache_enabled"),
    DRUID_CACHE_V2("druid_cache_v2_enabled"),
    QUERY_SPLIT("query_split_enabled"),
    TOP_N("top_n_enabled"),
    DATA_FILTER_SUBSTRING_OPERATIONS("data_filter_substring_operations_enabled"),
    INTERSECTION_REPORTING("intersection_reporting_enabled"),
    UPDATED_METADATA_COLLECTION_NAMES("updated_metadata_collection_names_enabled"),
    DRUID_COORDINATOR_METADATA("druid_coordinator_metadata_enabled"),
    DRUID_DIMENSIONS_LOADER("druid_dimensions_loader_enabled"),
    CASE_SENSITIVE_KEYS("case_sensitive_keys_enabled");

    private final String propertyName;
    private Boolean on;

    /**
     * Constructor.
     *
     * @param propertyName  Name of the SystemConfig property to use for the feature flag.
     */
    BardFeatureFlag(String propertyName) {
        SystemConfig systemConfig = SystemConfigProvider.getInstance();
        this.propertyName = propertyName;
        this.on = systemConfig.getBooleanProperty(systemConfig.getPackageVariableName(propertyName), false);
    }

    @Override
    public String getName() {
        return propertyName;
    }

    @Override
    public boolean isOn() {
        return on;
    }

    @Override
    public void setOn(Boolean newValue) {
        on = newValue;
    }
}
