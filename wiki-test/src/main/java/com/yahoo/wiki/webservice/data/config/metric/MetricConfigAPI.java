package com.yahoo.wiki.webservice.data.config.metric;

import com.yahoo.bard.webservice.data.config.names.ApiMetricName;
import com.yahoo.bard.webservice.data.time.TimeGrain;

import java.util.List;
import java.util.Locale;

public interface MetricConfigAPI extends ApiMetricName {

    /**
     * Set metrics info.
     */
    void setApiName(String apiName);

    void setMakerName(String makerName);

    void setDependencyMetricNames(List<String> dependencyMetricNames);

    /**
     * Get metrics info.
     */
    String getMakerName();

    List<String> getDependencyMetricNames();
}
