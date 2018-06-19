package com.yahoo.wiki.webservice.data.config.metric;

import com.yahoo.bard.webservice.data.config.names.ApiMetricName;
import java.util.List;

/**
 * Wiki metric config API
 */
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
