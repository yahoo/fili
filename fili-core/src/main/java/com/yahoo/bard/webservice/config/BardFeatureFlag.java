// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config;

/**
 * Feature flags bind an object to a system configuration name.
 */
public enum BardFeatureFlag implements FeatureFlag {

    CURRENT_MACRO_USES_LATEST("current_macro_uses_latest"),
    CURRENT_TIME_ZONE_ADJUSTMENT("current_time_zone_adjustment"),
    ADJUSTED_TIME_ZONE("adjusted_time_zone"),

    /** Partial Data is the old form global flag.  It will only govern behavior is the new flags are off.*/
    PARTIAL_DATA("partial_data_enabled"),

    /** If true, use the PartialDataResultSetMapper to prune partial from responses. */
    PARTIAL_DATA_PROTECTION("partial_data_protection"),
    /** If true, use the partial and volatile data info to prefer tables in query planning. */
    PARTIAL_DATA_QUERY_OPTIMIZATION("partial_data_query_optimization"),


    /** Use {@link CacheFeatureFlag#TTL} instead. */
    @Deprecated DRUID_CACHE("druid_cache_enabled"),
    /** Use {@link CacheFeatureFlag#LOCAL_SIGNATURE} instead. */
    @Deprecated DRUID_CACHE_V2("druid_cache_v2_enabled"),
    QUERY_SPLIT("query_split_enabled"),
    CACHE_PARTIAL_DATA("cache_partial_data"),
    TOP_N("top_n_enabled"),
    DATA_FILTER_SUBSTRING_OPERATIONS("data_filter_substring_operations_enabled"),
    INTERSECTION_REPORTING("intersection_reporting_enabled"),
    UPDATED_METADATA_COLLECTION_NAMES("updated_metadata_collection_names_enabled"),
    DRUID_COORDINATOR_METADATA("druid_coordinator_metadata_enabled"),
    DRUID_LOOKUP_METADATA("druid_lookup_metadata_enabled"),
    DRUID_DIMENSIONS_LOADER("druid_dimensions_loader_enabled"),
    CASE_SENSITIVE_KEYS("case_sensitive_keys_enabled"),
    DEFAULT_IN_FILTER("default_in_filter_enabled"),
    REQUIRE_METRICS_QUERY("require_metrics_in_query")
    ;

    static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    private final String propertyName;
    private Boolean on = null;

    /**
     * Constructor.
     *
     * @param propertyName  Name of the SystemConfig property to use for the feature flag.
     */
    BardFeatureFlag(String propertyName) {
        this.propertyName = propertyName;
    }

    @Override
    public String getName() {
        return propertyName;
    }

    @Override
    public boolean isOn() {
        if (on == null) {
            on = SYSTEM_CONFIG.getBooleanProperty(SYSTEM_CONFIG.getPackageVariableName(propertyName), false);
        }
        return on;
    }

    @Override
    public void setOn(Boolean newValue) {
        SYSTEM_CONFIG.setProperty(SYSTEM_CONFIG.getPackageVariableName(propertyName), newValue.toString());
        on = newValue;
    }

    @Override
    public void reset() {
        SYSTEM_CONFIG.clearProperty(SYSTEM_CONFIG.getPackageVariableName(propertyName));
        on = null;
    }
}
