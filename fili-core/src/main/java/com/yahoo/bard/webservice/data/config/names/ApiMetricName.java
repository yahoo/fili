// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.names;

import com.yahoo.bard.webservice.data.time.TimeGrain;
import com.yahoo.bard.webservice.druid.model.query.AllGranularity;
import com.yahoo.bard.webservice.druid.model.query.Granularity;

import java.util.Objects;

/**
 * Interface to mark metric names.
 * <p>
 * Metric names are used as the keys in the metric dictionary and are also used in the query api to select logical
 * metrics.
 */
public interface ApiMetricName extends FieldName {

    /**
     * Determine if this API Metric Name is valid for the given time grain.
     * <p>
     * An example of this is a DailyAverage metric than doesn't make sense at the Day grain, but does at the Week.
     *
     * @param grain  TimeGrain to determine validity for
     *
     * @return True if the ApiMetricName is valid for the time grain
     */
    boolean isValidFor(TimeGrain grain);

    /**
     * Determine if this API Metric Name is valid for the given time grain.
     * <p>
     * An example of this is a DailyAverage metric than doesn't make sense to query by the HOUR grain, but does at WEEK.
     *
     * @param granularity  TimeGrain to determine validity for
     *
     * @return True if the ApiMetricName is valid for the time grain
     */
    default boolean isValidFor(Granularity granularity) {
        if (granularity instanceof TimeGrain) {
            return isValidFor(((TimeGrain) granularity));
        } else {
            return granularity instanceof AllGranularity;
        }
    }

    /**
     * The API Name is the external, end-user-facing name for the metric. This is the name of this metric in API
     * requests.
     *
     * @return User facing name for this metric
     */
    String getApiName();

    /**
     * Wrap a string in an anonymous instance of ApiMetricName.
     *
     * @param name the name being wrapped
     *
     * @return an anonymous subclass instance of ApiMetricName
     */
    static ApiMetricName of(String name) {
        return new ApiMetricName() {
            @Override
            public boolean isValidFor(TimeGrain grain) {
                return true;
            }

            @Override
            public String getApiName() {
                return name;
            }

            @Override
            public String asName() {
                return name;
            }

            @Override
            public boolean equals(Object o) {
                if (o == null || ! (o instanceof ApiMetricName)) {
                    return false;
                }
                return Objects.equals(name, ((ApiMetricName) o).asName()) &&
                    Objects.equals(name, ((ApiMetricName) o).getApiName());
            }

            @Override
            public int hashCode() {
                return name.hashCode();
            }
        };
    }
}
