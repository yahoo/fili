package com.yahoo.slurper.webservice.data.config.names;

import com.yahoo.bard.webservice.data.config.names.FieldName;

import java.util.Locale;

public enum WikiDruidMetricName implements FieldName {
    COUNT,
    ADDED,
    DELTA,
    DELETED;

    private final String lowerCaseName;

    /**
     * Create a physical metric name instance.
     */
    WikiDruidMetricName() {
        this.lowerCaseName = name().toLowerCase(Locale.ENGLISH);
    }

    @Override
    public String toString() {
        return lowerCaseName;
    }

    @Override
    public String asName() {
        return toString();
    }
}
