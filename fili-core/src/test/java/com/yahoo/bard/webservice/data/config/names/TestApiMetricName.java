// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.names;

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY;
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.HOUR;
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.MONTH;
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.WEEK;

import com.yahoo.bard.webservice.data.time.TimeGrain;
import com.yahoo.bard.webservice.util.EnumUtils;
import com.yahoo.bard.webservice.util.Utils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Hold the list of API metric names.
 */
public enum TestApiMetricName implements ApiMetricName {
    A_HEIGHT,
    A_WIDTH,
    A_DEPTH,
    A_AREA,
    A_VOLUME,
    A_USERS,
    A_OTHER_USERS,
    A_DAY_AVG_USERS(WEEK, MONTH),
    A_DAY_AVG_OTHER_USERS(WEEK, MONTH),
    A_ROW_NUM,
    A_LIMBS(HOUR),
    A_DAY_AVG_LIMBS,
    //Stubby test metrics for non-numeric metrics: strings, booleans, null, and arbitrary JSON nodes.
    A_STRING_METRIC,
    A_BOOLEAN_METRIC,
    A_JSON_NODE_METRIC,
    A_NULL_METRIC,
    A_SCOPED_WIDTH;

    private final String apiName;
    private final List<TimeGrain> supportedGrains;

    /**
     * Constructor.
     * <p>
     * Defaults to Day as the supported grain and no special name.
     */
    TestApiMetricName() {
        this((String) null, DAY);
    }

    /**
     * Constructor.
     * <p>
     * Defaults to no special name.
     *
     * @param supportedGrains  Set of TimeGrains that this metric supports
     */
    TestApiMetricName(TimeGrain... supportedGrains) {
        this((String) null, supportedGrains);
    }

    /**
     * Constructor.
     *
     * @param apiName  ApiName for the metric
     * @param supportedGrains  Set of TimeGrains that this metric supports
     */
    TestApiMetricName(String apiName, TimeGrain... supportedGrains) {
        // to camelCase
        this.apiName = (apiName == null ? EnumUtils.camelCase(this.name().substring(2)) : apiName);
        this.supportedGrains = Arrays.asList(supportedGrains);
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ENGLISH);
    }

    @Override
    public String getApiName() {
        return apiName;
    }

    @Override
    public String asName() {
        return getApiName();
    }

    @Override
    public boolean isValidFor(TimeGrain grain) {
        // As long as the satisfying grains of this metric satisfy the requested grain
        return supportedGrains.stream().anyMatch(grain::satisfiedBy);
    }

    /**
     * Get the Metric Names by the logical table they should be present in.
     *
     * @param logicalTable  Logical table for which to get the MetricNames
     *
     * @return the metric names for that logical table
     */
    public static Set<ApiMetricName> getByLogicalTable(TestLogicalTableName logicalTable) {
        switch (logicalTable) {
            case PETS:
                return Utils.asLinkedHashSet(A_ROW_NUM, A_LIMBS, A_DAY_AVG_LIMBS);
            case SHAPES:
                return Utils.asLinkedHashSet(
                        A_HEIGHT,
                        A_WIDTH,
                        A_DEPTH,
                        A_AREA,
                        A_VOLUME,
                        A_USERS,
                        A_OTHER_USERS,
                        A_DAY_AVG_USERS,
                        A_DAY_AVG_OTHER_USERS,
                        A_ROW_NUM,
                        A_STRING_METRIC,
                        A_BOOLEAN_METRIC,
                        A_NULL_METRIC,
                        A_JSON_NODE_METRIC,
                        A_SCOPED_WIDTH
                );
            case MONTHLY:
                return Utils.asLinkedHashSet(A_LIMBS, A_DAY_AVG_LIMBS);
            case HOURLY:
                return Utils.asLinkedHashSet(A_LIMBS);
            case HOURLY_MONTHLY:
                return Utils.asLinkedHashSet(A_LIMBS, A_DAY_AVG_LIMBS);
        }
        return Collections.<ApiMetricName>emptySet();
    }
}
