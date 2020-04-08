// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.metrics;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * ApiMetric describes the api description of a requested metric.
 *
 * In the case of non-parameterized metric syntaxes, this may only include a name which is expected to be in
 * the metric dictionary.  In the case of parameterized or Protocol metrics, it may include one or more key
 * value pairs passed in at request time. (Or annotated on via mapping functions in the code).
 */
public class ApiMetric {

    private final String rawName;
    private final String baseApiMetricId;
    private final Map<String, String> parameters;

    /**
     * Constructor.
     *
     * @param rawName  The name of this metric as it appears in the request
     * @param baseApiMetricId  The name of the base metric from the metric dictionary.
     * @param parameters  The key value pairs of used to modify the metric, if any.
     */
    public ApiMetric(
            String rawName,
            String baseApiMetricId,
            Map<String, String> parameters
    ) {
        this.rawName = rawName;
        this.baseApiMetricId = baseApiMetricId;
        this.parameters = new LinkedHashMap<>(parameters);
    }

    /**
     * Getter.
     *
     * @return raw name from the request.
     */
    public String getRawName() {
        return rawName;
    }

    /**
     * Getter.
     *
     * @return parsed name for the base metric.
     */
    public String getBaseApiMetricId() {
        return baseApiMetricId;
    }

    /**
     * Getter.
     *
     * @return parsed parameters.
     */
    public Map<String, String> getParameters() {
        return parameters;
    }

    /**
     * Convenience wither to add values.
     *
     * @param key  The key of the parameter to add.
     * @param value  The value of the parameter to add.
     *
     * @return A modified copy with an additional parameter
     */
    public ApiMetric withParameter(String key, String value) {
        Map<String, String> newParams = new LinkedHashMap<>(parameters);
        newParams.put(key, value);
        return new ApiMetric(rawName, baseApiMetricId, newParams);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) { return true; }
        if (!(o instanceof ApiMetric)) { return false; }
        final ApiMetric apiMetric = (ApiMetric) o;
        return Objects.equals(rawName, apiMetric.rawName) &&
                Objects.equals(baseApiMetricId, apiMetric.baseApiMetricId) &&
                Objects.equals(parameters, apiMetric.parameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rawName, baseApiMetricId, parameters);
    }

    @Override
    public String toString() {
        return "ApiMetric{" +
                "rawName='" + rawName + '\'' +
                ", baseApiMetricId='" + baseApiMetricId + '\'' +
                ", parameters=" + parameters +
                '}';
    }
}
