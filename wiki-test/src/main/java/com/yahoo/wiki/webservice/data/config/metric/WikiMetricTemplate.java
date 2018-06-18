package com.yahoo.wiki.webservice.data.config.metric;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.yahoo.bard.webservice.data.config.names.ApiMetricName;
import com.yahoo.bard.webservice.data.time.TimeGrain;
import com.yahoo.bard.webservice.util.EnumUtils;
import com.yahoo.wiki.webservice.data.config.Template;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.HOUR;

@JsonIgnoreProperties(ignoreUnknown = true)
public class WikiMetricTemplate extends Template implements MetricConfigAPI {

    @JsonProperty("apiName")
    private String apiName;

    @JsonProperty("maker")
    private String makerName;

    @JsonProperty("dependencyMetricNames")
    private List<String> dependencyMetricNames;

    private List<TimeGrain> satisfyingGrains;

    /**
     * Constructor
     */
    WikiMetricTemplate() { }

    /**
     * Create a Wiki Metric descriptor with a fixed set of satisfying grains.
     *
     * @param apiName  The api name for the metric.
     * @param satisfyingGrains  The grains that satisfy this metric.
     */
    WikiMetricTemplate(String apiName, TimeGrain... satisfyingGrains) {
        // to camelCase
        this.apiName = (apiName == null ? EnumUtils.camelCase(this.getApiName()) : apiName);
        this.satisfyingGrains = Arrays.asList(satisfyingGrains);
    }

    /**
     * Set metrics info.
     */
    @Override
    public void setApiName(String apiName) {
        this.apiName = (apiName == null ? EnumUtils.camelCase(this.apiName) : apiName);
    }

    @Override
    public void setMakerName(String makerName) {
        this.makerName = makerName;
    }

    @Override
    public void setDependencyMetricNames(List<String> dependencyMetricNames){
        this.dependencyMetricNames = dependencyMetricNames;
    };

    /**
     * Get metrics info.
     */
    @Override
    public String getApiName() {
        return EnumUtils.camelCase(this.apiName);
    }

    @Override
    public String getMakerName() {
        return makerName;
    }

    @Override
    public List<String> getDependencyMetricNames(){
        if (dependencyMetricNames == null) return Collections.emptyList();
        return dependencyMetricNames.stream().map(name -> EnumUtils.camelCase(name)).collect(Collectors.toList());
    };

    @Override
    public String toString() {
        return this.getApiName().toLowerCase(Locale.ENGLISH);
    }

    @Override
    public String asName() {
        return getApiName();
    }

    @Override
    public boolean isValidFor(TimeGrain grain) {
        // As long as the satisfying grains of this metric satisfy the requested grain
        return satisfyingGrains.stream().anyMatch(grain::satisfiedBy);
    }

}
