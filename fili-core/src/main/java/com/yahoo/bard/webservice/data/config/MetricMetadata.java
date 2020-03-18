package com.yahoo.bard.webservice.data.config;

public interface MetricMetadata extends GlobalMetadata {
    String getLongName();

    String getCategory();

    String getType();
}
