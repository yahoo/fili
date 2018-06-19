package com.yahoo.wiki.webservice.data.config.metric;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.yahoo.wiki.webservice.data.config.Template;

import java.util.LinkedHashSet;

/**
 * Metric Config template.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class WikiMetricConfigTemplate extends Template {

    @JsonProperty("metrics")
    private LinkedHashSet<WikiMetricTemplate> metrics;

    /**
     * Constructor.
     */
    public WikiMetricConfigTemplate() {
    }

    /**
     * Set metrics info.
     */
    public void setMetrics(LinkedHashSet<WikiMetricTemplate> metrics) {
        this.metrics = metrics;
    }

    /**
     * Get metrics configuration info.
     */
    public LinkedHashSet<WikiMetricTemplate> getMetrics() {
        return this.metrics;
    }
}
