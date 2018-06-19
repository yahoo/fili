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

    void setLongName(String longName);

    void setMakerName(String makerName);

    void setDescription(String description);

    void setDependencyMetricNames(List<String> dependencyMetricNames);

    /**
     * Get metrics info.
     */

    String getLongName();

    String getMakerName();

    String getDescription();

    List<String> getDependencyMetricNames();
}
